//! The engine that runs statically compiled DBSP circuits.

use std::collections::HashSet;
use std::io;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam::sync::Parker;
use dbsp::circuit::metrics::{
    COMPACTION_STALL_TIME_NANOSECONDS, DBSP_OPERATOR_COMMIT_LATENCY_MICROSECONDS, DBSP_STEP,
    DBSP_STEP_LATENCY_MICROSECONDS, DUPLICATE_EXCHANGE_MESSAGES_RECEIVED,
    EXCHANGE_MESSAGES_RECEIVED, FILES_CREATED, FILES_DELETED, FILES_SYNCED, TOTAL_LATE_RECORDS,
};
use dbsp::circuit::{CircuitConfig, Mode};
use dbsp::profile::{DbspProfile, GraphProfile, ProfileJsonWriter, WorkerProfile};
use dbsp::{DBSPHandle, Runtime, WeakRuntime};
use feldera_adapterlib::bootstrap::{BootstrapInfo, ConcurrentRestoreOutcome};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_ir::LirCircuit;
use feldera_storage::StorageBackend;
use feldera_storage::error::StorageError;
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::config::PipelineConfig;
use feldera_types::memory_pressure::MemoryPressure;
use feldera_types::transaction::CommitProgressSummary;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::ControllerError;
use crate::bootstrap::{self, InputOperators};
use crate::catalog::OUTPUT_MAPPING;
use crate::storage::{StaticStorage, publish_with_state};
use dbsp::Error as DbspError;
use dbsp_connectors::engine::{
    Checkpoint, CheckpointCommitter, CheckpointPublisher, CircuitDriver, CircuitFactory,
    CircuitParams, Engine, EngineCircuit, EngineGraphProfile, EngineMetrics, EngineRuntime,
    JsonProfileChunks, OpenStorageFn, PipelineStorage, ProgramLoader, RuntimeGauges,
};

/// Builds the circuit of a pipeline that the SQL compiler compiled, from dbsp's configuration,
/// and returns dbsp's handle to it with its catalog.
pub type StaticCircuitFactory = Box<
    dyn FnOnce(CircuitConfig) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
>;

/// The engine of a pipeline that the SQL compiler compiled to Rust.
pub struct StaticEngine {
    /// The pipeline's circuit, until the server takes its factory.
    circuit: Mutex<Option<StaticCircuitFactory>>,
}

impl StaticEngine {
    /// Returns the engine of the pipeline whose circuit `circuit` builds.
    ///
    /// # Arguments
    ///
    /// * `circuit` - builds the circuit from dbsp's configuration; the generated `main`
    ///   passes it to `server_main`.
    pub fn new(circuit: StaticCircuitFactory) -> Self {
        Self {
            circuit: Mutex::new(Some(circuit)),
        }
    }
}

/// The static engine takes no command-line arguments of its own.
#[derive(clap::Args, Clone, Debug, Default)]
pub struct StaticEngineArgs {}

impl Engine for StaticEngine {
    type Args = StaticEngineArgs;

    fn circuit_factory(
        &self,
        _config: &PipelineConfig,
        _args: &Self::Args,
    ) -> Result<CircuitFactory, ControllerError> {
        let circuit = self.circuit.lock().unwrap().take().ok_or_else(|| {
            ControllerError::not_supported(
                "the static engine builds the circuit of a pipeline only once",
            )
        })?;
        Ok(static_circuit_factory(circuit))
    }

    fn open_storage(
        &self,
        config: &PipelineConfig,
    ) -> Result<Option<Arc<dyn PipelineStorage>>, ControllerError> {
        static_open_storage()(config)
    }

    /// The circuit is compiled into the binary, so the engine cannot load another program.
    fn program_loader(&self, _args: &Self::Args) -> Option<Box<dyn ProgramLoader>> {
        None
    }
}

/// Returns the circuit factory that builds the circuit of `circuit` with dbsp.
///
/// The factory configures dbsp from the controller's parameters, as a persistent circuit that
/// takes no input during a commit, and wraps dbsp's handle and runtime in the engine traits.
///
/// # Arguments
///
/// * `circuit` - builds the circuit from dbsp's configuration.
///
/// # Returns
///
/// The factory, which records the output assignment of a multihost pipeline where the
/// catalog of the circuit reads it.
pub fn static_circuit_factory(circuit: StaticCircuitFactory) -> CircuitFactory {
    Box::new(move |params: CircuitParams| {
        let CircuitParams {
            layout,
            output_assignment,
            storage,
            init_checkpoint,
            defer_restore,
            max_rss_bytes,
            reserved_memory_bytes,
            pin_cpus,
            dev_tweaks,
        } = params;
        let storage = storage.as_deref().map(|storage| {
            storage
                .as_any()
                .downcast_ref::<StaticStorage>()
                .expect("the static engine runs only on the storage that it opens")
        });
        *OUTPUT_MAPPING.lock().unwrap() = output_assignment;
        let config = CircuitConfig::from(layout)
            .with_max_rss_bytes(max_rss_bytes)
            .with_reserved_memory_bytes(reserved_memory_bytes)
            .with_pin_cpus(pin_cpus)
            .with_storage(
                storage.map(|storage| storage.circuit_config(init_checkpoint, defer_restore)),
            )
            .with_mode(Mode::Persistent)
            .with_dev_tweaks(dev_tweaks)
            .with_allow_input_during_commit(false);
        let (handle, catalog) = circuit(config)?;
        let inputs = InputOperators::new(catalog.as_ref());
        let runtime = Arc::new(StaticRuntime::new(handle.runtime()));
        Ok(EngineCircuit {
            driver: Box::new(StaticCircuitDriver {
                handle,
                inputs,
                backend: storage.map(|storage| storage.backend().clone()),
            }),
            runtime,
            catalog,
        })
    })
}

/// Returns the function that opens the static engine's storage, which the engine's
/// [`Engine::open_storage`] calls.
pub fn static_open_storage() -> OpenStorageFn {
    Arc::new(|config| {
        Ok(StaticStorage::open(config)?.map(|storage| storage as Arc<dyn PipelineStorage>))
    })
}

/// dbsp's handle to a circuit, with the input operators that its bootstrap info names.
struct StaticCircuitDriver {
    handle: DBSPHandle,
    inputs: InputOperators,

    /// The storage backend that the circuit's checkpoints go to, if the pipeline has storage.
    backend: Option<Arc<dyn StorageBackend>>,
}

impl CircuitDriver for StaticCircuitDriver {
    fn transaction(&mut self) -> Result<(), ControllerError> {
        self.handle
            .transaction()
            .map_err(ControllerError::dbsp_error)
    }

    fn start_transaction(&mut self) -> Result<(), ControllerError> {
        self.handle
            .start_transaction()
            .map_err(ControllerError::dbsp_error)
    }

    fn start_commit_transaction(&mut self) -> Result<(), ControllerError> {
        self.handle
            .start_commit_transaction()
            .map_err(ControllerError::dbsp_error)
    }

    /// A dbsp step blocks until it ends, so this ignores `max_wait`.
    fn step(&mut self, _max_wait: Duration) -> Result<bool, ControllerError> {
        self.handle.step().map_err(ControllerError::dbsp_error)
    }

    fn commit_progress(&mut self) -> Result<CommitProgressSummary, ControllerError> {
        self.handle
            .commit_progress()
            .map(|progress| progress.summary())
            .map_err(ControllerError::dbsp_error)
    }

    fn runtime_elapsed(&self) -> Duration {
        self.handle.runtime_elapsed()
    }

    fn prepare_checkpoint(
        &mut self,
        steps: u64,
        processed_records: u64,
    ) -> Result<Box<dyn CheckpointCommitter>, ControllerError> {
        let committer = self
            .handle
            .checkpoint()
            .with_steps(steps)
            .with_processed_records(processed_records)
            .prepare()
            .map_err(ControllerError::dbsp_error)?;
        // dbsp prepares a checkpoint only with storage, so this does not fail.
        let backend = self.backend.clone().ok_or_else(|| {
            ControllerError::dbsp_error(DbspError::Storage(StorageError::StorageDisabled))
        })?;
        Ok(Box::new(StaticCheckpointCommitter {
            committer,
            backend,
            runtime: self.handle.runtime().clone(),
        }))
    }

    fn gc_checkpoint(&mut self, keep: HashSet<Uuid>) -> Result<(), ControllerError> {
        self.handle
            .gc_checkpoint(keep)
            .map(|_| ())
            .map_err(ControllerError::dbsp_error)
    }

    fn bootstrap_in_progress(&self) -> bool {
        self.handle.bootstrap_in_progress()
    }

    fn bootstrap_info(&self) -> Option<BootstrapInfo> {
        bootstrap::bootstrap_info(&self.handle, &self.inputs)
    }

    fn start_concurrent_bootstrap(
        &mut self,
        base: Uuid,
    ) -> Result<ConcurrentRestoreOutcome, ControllerError> {
        let outcome = self
            .handle
            .start_concurrent_bootstrap(base.to_string().into())
            .map_err(ControllerError::dbsp_error)?;
        Ok(bootstrap::concurrent_restore_outcome(outcome, &self.inputs))
    }

    fn step_bootstrap_circuit(&mut self) -> Result<bool, ControllerError> {
        self.handle
            .step_bootstrap_circuit()
            .map_err(ControllerError::dbsp_error)
    }

    fn sync_concurrent_bootstrap(&mut self) -> Result<(), ControllerError> {
        self.handle
            .sync_concurrent_bootstrap()
            .map_err(ControllerError::dbsp_error)
    }

    fn complete_concurrent_bootstrap(&mut self) -> Result<(), ControllerError> {
        self.handle
            .complete_concurrent_bootstrap()
            .map_err(ControllerError::dbsp_error)
    }

    fn destroy_bootstrap_circuit(&mut self) -> Result<(), ControllerError> {
        self.handle
            .destroy_bootstrap_circuit()
            .map_err(ControllerError::dbsp_error)
    }

    fn bootstrap_commit_progress(&mut self) -> Result<CommitProgressSummary, ControllerError> {
        self.handle
            .bootstrap_commit_progress()
            .map(|progress| progress.summary())
            .map_err(ControllerError::dbsp_error)
    }

    fn lir(&mut self) -> Result<LirCircuit, ControllerError> {
        self.handle.lir().map_err(ControllerError::dbsp_error)
    }

    fn graph_profile(&mut self) -> Result<Box<dyn EngineGraphProfile>, ControllerError> {
        let profile = self
            .handle
            .graph_profile()
            .map_err(ControllerError::dbsp_error)?;
        Ok(Box::new(StaticGraphProfile(profile)))
    }

    fn json_profile(&mut self) -> Result<JsonProfileChunks, ControllerError> {
        let profile = self
            .handle
            .retrieve_profile()
            .map_err(ControllerError::dbsp_error)?;
        Ok(Box::new(JsonProfileWriterChunks::new(profile)))
    }

    fn enable_cpu_profiler(&mut self) -> Result<(), ControllerError> {
        self.handle
            .enable_cpu_profiler()
            .map_err(ControllerError::dbsp_error)
    }

    fn set_auto_rebalance(&mut self, enable: bool) -> Result<(), ControllerError> {
        self.handle
            .set_auto_rebalance(enable)
            .map_err(ControllerError::dbsp_error)
    }

    fn rebalance(&mut self) -> Result<(), ControllerError> {
        self.handle.rebalance().map_err(ControllerError::dbsp_error)
    }

    fn start_compaction(&mut self) -> Result<(), ControllerError> {
        self.handle
            .start_compaction()
            .map_err(ControllerError::dbsp_error)
    }

    fn kill(self: Box<Self>) -> Result<(), ControllerError> {
        self.handle
            .kill()
            .map_err(|_| ControllerError::dbsp_panic())
    }
}

/// A checkpoint that dbsp prepared, in the storage backend `backend`.
struct StaticCheckpointCommitter {
    committer: dbsp::circuit::CheckpointCommitter,
    backend: Arc<dyn StorageBackend>,
    // Transfer the storage lease to the publisher, even if a worker error
    // kills the circuit while the checkpoint runs on another thread.
    runtime: Runtime,
}

impl CheckpointCommitter for StaticCheckpointCommitter {
    fn commit(self: Box<Self>) -> Result<Box<dyn CheckpointPublisher>, ControllerError> {
        let publisher = self
            .committer
            .commit()
            .map_err(ControllerError::dbsp_error)?;
        Ok(Box::new(StaticCheckpointPublisher {
            publisher,
            backend: self.backend,
            runtime: self.runtime,
        }))
    }
}

/// A checkpoint that dbsp committed, in the storage backend `backend`.
struct StaticCheckpointPublisher {
    publisher: dbsp::circuit::CheckpointPublisher,
    backend: Arc<dyn StorageBackend>,
    // Backends do not own the directory lock. Keep its runtime alive through
    // the root state write, which follows the inner publisher's disposal.
    runtime: Runtime,
}

impl CheckpointPublisher for StaticCheckpointPublisher {
    fn metadata(&self) -> &CheckpointMetadata {
        self.publisher.metadata()
    }

    fn publish(self: Box<Self>, state: &Checkpoint) -> Result<CheckpointMetadata, ControllerError> {
        let Self {
            publisher,
            backend,
            runtime,
        } = *self;
        let uuid = publisher.metadata().uuid;
        let result = publish_with_state(&*backend, state, uuid, || {
            publisher.publish().map_err(ControllerError::dbsp_error)
        });
        drop(runtime);
        result
    }
}

/// dbsp's runtime, held weakly, so that it goes away when its threads end.
struct StaticRuntime {
    runtime: WeakRuntime,
    shutdown: CancellationToken,
}

impl StaticRuntime {
    fn new(runtime: &Runtime) -> Self {
        Self {
            runtime: runtime.downgrade(),
            shutdown: runtime.cancellation_token(),
        }
    }
}

impl EngineRuntime for StaticRuntime {
    fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// The thread runs inside the runtime, so that it can use the runtime's storage.
    fn spawn_aux_thread(&self, name: &str, parker: Parker, f: Box<dyn FnOnce(Parker) + Send>) {
        self.runtime
            .upgrade()
            .expect("attempt to add an output connector after the runtime has terminated")
            .spawn_aux_thread(name, parker, f);
    }

    fn memory_pressure(&self) -> Option<(MemoryPressure, u64)> {
        self.runtime.upgrade().map(|runtime| {
            (
                runtime.current_memory_pressure(),
                runtime.memory_pressure_epoch(),
            )
        })
    }

    fn set_max_rss_bytes(&self, max_rss: Option<u64>) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.set_max_rss_bytes(max_rss);
        }
    }

    fn metrics(&self) -> EngineMetrics {
        EngineMetrics {
            late_records: TOTAL_LATE_RECORDS.load(Ordering::Relaxed),
            compaction_stall_nanoseconds: COMPACTION_STALL_TIME_NANOSECONDS.load(Ordering::Relaxed),
            files_created: FILES_CREATED.load(Ordering::Relaxed),
            files_deleted: FILES_DELETED.load(Ordering::Relaxed),
            files_synced: FILES_SYNCED.load(Ordering::Relaxed),
            steps: DBSP_STEP.load(Ordering::Relaxed),
            step_latency_microseconds: DBSP_STEP_LATENCY_MICROSECONDS.lock().unwrap().snapshot(),
            operator_checkpoint_latency_microseconds: DBSP_OPERATOR_COMMIT_LATENCY_MICROSECONDS
                .snapshot(),
            exchange_messages_received: EXCHANGE_MESSAGES_RECEIVED.load(Ordering::Relaxed) as u64,
            duplicate_exchange_messages_received: DUPLICATE_EXCHANGE_MESSAGES_RECEIVED
                .load(Ordering::Relaxed) as u64,
            runtime: self.runtime.upgrade().map(|runtime| {
                let (cache_used_bytes, cache_limit_bytes) = runtime.cache_occupancy();
                RuntimeGauges {
                    cache_used_bytes,
                    cache_limit_bytes,
                    fbuf_slabs: runtime.fbuf_slabs_stats(),
                    max_rss_bytes: runtime.max_rss_bytes(),
                    memory_pressure: runtime.current_memory_pressure(),
                    memory_pressure_epoch: runtime.memory_pressure_epoch(),
                }
            }),
        }
    }
}

/// dbsp's profile of the circuit graph.
struct StaticGraphProfile(GraphProfile);

impl EngineGraphProfile for StaticGraphProfile {
    fn dump(&self, dir: &Path) -> io::Result<PathBuf> {
        self.0.dump(dir)
    }

    fn write_zip(&self, out: &mut dyn io::Write) -> io::Result<()> {
        self.0.write_zip(out).map(|_| ()).map_err(io::Error::other)
    }
}

/// The JSON form of dbsp's profile, one worker at a time.
///
/// The first chunk holds the start of the document and the first worker, each further chunk
/// one more worker, and the last chunk the end of the document with the graph. With no
/// workers, the one chunk holds the whole document. An error ends the chunks.
struct JsonProfileWriterChunks {
    state: JsonProfileState,
}

#[derive(Default)]
enum JsonProfileState {
    /// The document has not started.
    Start(DbspProfile),

    /// The document has started, and `workers` are still to come.
    Workers {
        writer: ProfileJsonWriter<Vec<u8>>,
        workers: std::vec::IntoIter<WorkerProfile>,
        graph: Option<dbsp::monitor::visual_graph::Graph>,
    },

    /// The document has ended, or failed.
    #[default]
    Done,
}

impl JsonProfileWriterChunks {
    fn new(profile: DbspProfile) -> Self {
        Self {
            state: JsonProfileState::Start(profile),
        }
    }
}

impl Iterator for JsonProfileWriterChunks {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match take(&mut self.state) {
            JsonProfileState::Start(profile) => {
                let DbspProfile {
                    metrics,
                    worker_profiles,
                    graph,
                } = profile;
                match ProfileJsonWriter::begin(Vec::new(), Some(metrics)) {
                    Ok(writer) => {
                        self.state = JsonProfileState::Workers {
                            writer,
                            workers: worker_profiles.into_iter(),
                            graph,
                        };
                        self.next()
                    }
                    Err(error) => Some(Err(error)),
                }
            }
            JsonProfileState::Workers {
                mut writer,
                mut workers,
                graph,
            } => match workers.next() {
                Some(worker) => Some(writer.worker(&worker).map(|()| {
                    let chunk = take(writer.writer_mut());
                    self.state = JsonProfileState::Workers {
                        writer,
                        workers,
                        graph,
                    };
                    chunk
                })),
                None => Some(writer.finish(&graph)),
            },
            JsonProfileState::Done => None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::sync::atomic::AtomicI64;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dbsp::Runtime;
    use dbsp::circuit::{CircuitConfig, Mode};
    use dbsp::profile::{DbspProfile, WorkerProfile};
    use dbsp::storage::dirlock::LockedDirectory;
    use dbsp::utils::Tup2;
    use dbsp_connectors::engine::{Checkpoint, CircuitDriver, Engine, PipelineStorage};
    use feldera_storage::error::StorageError;
    use feldera_storage::{DirEntry, FileReader, FileWriter, StorageBackend, StoragePath};
    use feldera_types::config::PipelineConfig;
    use feldera_types::constants::STATE_FILE;
    use serde_json::json;
    use tempfile::TempDir;

    use super::{JsonProfileWriterChunks, StaticCircuitDriver, StaticEngine, StaticEngineArgs};
    use crate::storage::StaticStorage;
    use crate::test::init_test_logger;

    /// Observes the real directory lock at both state writes. In particular,
    /// the root write happens after the inner DBSP publisher was consumed.
    /// This wrapper holds only the backend, never a Runtime or lock lease.
    struct ObservedStorage {
        inner: Arc<dyn StorageBackend>,
        writes: Mutex<Vec<(StoragePath, bool)>>,
    }

    impl StorageBackend for ObservedStorage {
        fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
            let locked = LockedDirectory::new(self.inner.file_system_path().unwrap()).is_err();
            self.writes.lock().unwrap().push((name.clone(), locked));
            self.inner.create_named(name)
        }

        fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
            self.inner.open(name)
        }

        fn file_system_path(&self) -> Option<&Path> {
            self.inner.file_system_path()
        }

        fn list(
            &self,
            parent: &StoragePath,
            cb: &mut dyn FnMut(DirEntry),
        ) -> Result<(), StorageError> {
            self.inner.list(parent, cb)
        }

        fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
            self.inner.delete(name)
        }

        fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError> {
            self.inner.delete_recursive(name)
        }

        fn fsync_dir(&self, dir: &StoragePath) -> Result<(), StorageError> {
            self.inner.fsync_dir(dir)
        }

        fn usage(&self) -> Arc<AtomicI64> {
            self.inner.usage()
        }
    }

    fn checkpoint_storage_lease(worker_error: bool, publish: bool) {
        init_test_logger();
        let dir = TempDir::new().unwrap();
        let config: PipelineConfig = serde_json::from_value(json!({
            "name": "checkpoint-storage-lease",
            "workers": 1,
            "storage_config": { "path": dir.path() },
            "storage": true,
            "inputs": {},
        }))
        .unwrap();
        let storage = StaticStorage::open(&config).unwrap().unwrap();
        let backend = Arc::new(ObservedStorage {
            inner: storage.backend().clone(),
            writes: Mutex::new(Vec::new()),
        });
        let circuit_config = CircuitConfig::with_workers(1)
            .with_mode(Mode::Persistent)
            .with_allow_input_during_commit(false)
            .with_storage(Some(storage.circuit_config(None, false)));
        let (handle, input) = Runtime::init_circuit(circuit_config, |circuit| {
            let (stream, input) = circuit.add_input_zset::<i64>();
            stream.set_persistent_id(Some("input"));
            stream.integrate_trace();
            Ok(input)
        })
        .unwrap();
        let shutdown = handle.runtime().cancellation_token();
        let mut driver = StaticCircuitDriver {
            handle,
            inputs: Default::default(),
            backend: Some(backend.clone()),
        };
        input.append(&mut vec![Tup2(11, 1), Tup2(23, 1)]);
        driver.transaction().unwrap();
        let publisher = driver.prepare_checkpoint(1, 2).unwrap().commit().unwrap();
        if worker_error {
            // A real worker command error synchronously kills the runtime.
            // Do not keep a Runtime clone in the test: that would hide the bug.
            let error = driver.step(Duration::ZERO).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("step called outside of a transaction"),
                "{error}"
            );
            assert!(shutdown.is_cancelled());
        } else {
            driver.graph_profile().unwrap();
            assert!(!shutdown.is_cancelled());
        }
        let released_before_publication = LockedDirectory::new(dir.path()).is_ok();
        if publish {
            let state = Checkpoint {
                circuit: Some(publisher.metadata().clone()),
                step: 1,
                config,
                processed_records: 2,
                transaction_number: 1,
                initial_start_time: chrono::DateTime::UNIX_EPOCH,
                input_metadata: Default::default(),
                input_statistics: Default::default(),
                output_statistics: Default::default(),
                input_distribution: Default::default(),
            };
            let metadata = publisher.publish(&state).unwrap();
            let root: Checkpoint = storage.backend().read_json(&STATE_FILE.into()).unwrap();
            assert_eq!(root.circuit.unwrap().uuid, metadata.uuid);
            assert_eq!((root.step, root.processed_records), (1, 2));
        } else {
            drop(publisher);
            assert!(storage.latest_checkpoint().unwrap().is_none());
        }
        Box::new(driver).kill().unwrap();
        assert!(
            LockedDirectory::new(dir.path()).is_ok(),
            "storage lease leaked after completion"
        );
        let writes = backend.writes.lock().unwrap();
        eprintln!(
            "worker_error={worker_error}, publish={publish}, storage_released_before_publication={released_before_publication}, state_write_locks={writes:?}"
        );
        assert!(
            !released_before_publication,
            "pending publisher lost the storage lease"
        );
        if publish {
            assert!(
                writes
                    .iter()
                    .any(|(path, locked)| path.as_ref() == STATE_FILE && *locked),
                "root state write was not protected by the storage lease"
            );
            assert!(
                writes.iter().all(|(_, locked)| *locked),
                "state written after releasing storage"
            );
        }
    }

    #[test]
    fn checkpoint_storage_lease_survives_worker_error_until_publication() {
        checkpoint_storage_lease(true, true);
    }

    #[test]
    fn checkpoint_storage_lease_survives_worker_error_until_cancellation() {
        checkpoint_storage_lease(true, false);
    }

    #[test]
    fn checkpoint_storage_lease_normal_publication() {
        checkpoint_storage_lease(false, true);
    }

    /// The circuit is compiled into the binary, so the engine has no program loader, and
    /// its server refuses `--enable-program-load`.
    #[test]
    fn the_static_engine_cannot_load_programs() {
        let engine = StaticEngine::new(Box::new(|_| {
            unreachable!("the test never builds the circuit")
        }));
        assert!(
            engine
                .program_loader(&StaticEngineArgs::default())
                .is_none()
        );
    }

    /// The chunks concatenate to exactly `DbspProfile::as_json`, one chunk per worker plus the
    /// closing one.
    #[test]
    fn json_profile_chunks_concatenate_to_as_json() {
        for workers in [0, 1, 3] {
            let profile = DbspProfile::new(vec![WorkerProfile::default(); workers], None);
            let expected = profile.as_json();
            let chunks: Vec<Vec<u8>> = JsonProfileWriterChunks::new(profile)
                .collect::<Result<_, _>>()
                .unwrap();
            let expected_chunks = if workers == 0 { 1 } else { workers + 1 };
            assert_eq!(chunks.len(), expected_chunks);
            assert_eq!(chunks.concat(), expected.as_bytes());
        }
    }
}
