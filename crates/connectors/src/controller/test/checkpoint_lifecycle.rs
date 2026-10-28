//! Checkpoint shutdown with the real engine, File transport and storage.

use super::*;
use crate::engine::{
    Checkpoint, CheckpointCommitter, CheckpointPublisher, CircuitDriver, EngineGraphProfile,
    JsonProfileChunks,
};
use feldera_adapterlib::bootstrap::{BootstrapInfo, ConcurrentRestoreOutcome};
use feldera_ir::LirCircuit;
use feldera_types::transaction::CommitProgressSummary;
use std::sync::Weak;

const TIMEOUT: Duration = Duration::from_secs(20);
type PublishGate = (mpsc::Sender<()>, mpsc::Receiver<()>);

/// Observe the lifetime of the engine's actual committer/publisher, without
/// depending on thread names or substituting its storage implementation.
struct ObservedDriver {
    inner: Box<dyn CircuitDriver>,
    prepared: mpsc::Sender<Weak<()>>,
    committed: mpsc::Sender<()>,
    publish_gate: Option<PublishGate>,
}

impl CircuitDriver for ObservedDriver {
    fn transaction(&mut self) -> Result<(), ControllerError> {
        self.inner.transaction()
    }
    fn start_transaction(&mut self) -> Result<(), ControllerError> {
        self.inner.start_transaction()
    }
    fn start_commit_transaction(&mut self) -> Result<(), ControllerError> {
        self.inner.start_commit_transaction()
    }
    fn step(&mut self, max_wait: Duration) -> Result<bool, ControllerError> {
        self.inner.step(max_wait)
    }
    fn commit_progress(&mut self) -> Result<CommitProgressSummary, ControllerError> {
        self.inner.commit_progress()
    }
    fn runtime_elapsed(&self) -> Duration {
        self.inner.runtime_elapsed()
    }
    fn prepare_checkpoint(
        &mut self,
        steps: u64,
        processed_records: u64,
    ) -> Result<Box<dyn CheckpointCommitter>, ControllerError> {
        let inner = self.inner.prepare_checkpoint(steps, processed_records)?;
        let owner = Arc::new(());
        self.prepared.send(Arc::downgrade(&owner)).unwrap();
        Ok(Box::new(ObservedCommitter {
            inner,
            owner,
            committed: self.committed.clone(),
            publish_gate: self.publish_gate.take(),
        }))
    }
    fn gc_checkpoint(&mut self, keep: HashSet<Uuid>) -> Result<(), ControllerError> {
        self.inner.gc_checkpoint(keep)
    }
    fn bootstrap_in_progress(&self) -> bool {
        self.inner.bootstrap_in_progress()
    }
    fn bootstrap_info(&self) -> Option<BootstrapInfo> {
        self.inner.bootstrap_info()
    }
    fn start_concurrent_bootstrap(
        &mut self,
        base: Uuid,
    ) -> Result<ConcurrentRestoreOutcome, ControllerError> {
        self.inner.start_concurrent_bootstrap(base)
    }
    fn step_bootstrap_circuit(&mut self) -> Result<bool, ControllerError> {
        self.inner.step_bootstrap_circuit()
    }
    fn sync_concurrent_bootstrap(&mut self) -> Result<(), ControllerError> {
        self.inner.sync_concurrent_bootstrap()
    }
    fn complete_concurrent_bootstrap(&mut self) -> Result<(), ControllerError> {
        self.inner.complete_concurrent_bootstrap()
    }
    fn destroy_bootstrap_circuit(&mut self) -> Result<(), ControllerError> {
        self.inner.destroy_bootstrap_circuit()
    }
    fn bootstrap_commit_progress(&mut self) -> Result<CommitProgressSummary, ControllerError> {
        self.inner.bootstrap_commit_progress()
    }
    fn lir(&mut self) -> Result<LirCircuit, ControllerError> {
        self.inner.lir()
    }
    fn graph_profile(&mut self) -> Result<Box<dyn EngineGraphProfile>, ControllerError> {
        self.inner.graph_profile()
    }
    fn json_profile(&mut self) -> Result<JsonProfileChunks, ControllerError> {
        self.inner.json_profile()
    }
    fn enable_cpu_profiler(&mut self) -> Result<(), ControllerError> {
        self.inner.enable_cpu_profiler()
    }
    fn set_auto_rebalance(&mut self, enable: bool) -> Result<(), ControllerError> {
        self.inner.set_auto_rebalance(enable)
    }
    fn rebalance(&mut self) -> Result<(), ControllerError> {
        self.inner.rebalance()
    }
    fn start_compaction(&mut self) -> Result<(), ControllerError> {
        self.inner.start_compaction()
    }
    fn kill(self: Box<Self>) -> Result<(), ControllerError> {
        self.inner.kill()
    }
}

struct ObservedCommitter {
    inner: Box<dyn CheckpointCommitter>,
    owner: Arc<()>,
    committed: mpsc::Sender<()>,
    publish_gate: Option<PublishGate>,
}

impl CheckpointCommitter for ObservedCommitter {
    fn commit(self: Box<Self>) -> Result<Box<dyn CheckpointPublisher>, ControllerError> {
        let inner = self.inner.commit()?;
        self.committed.send(()).unwrap();
        Ok(Box::new(ObservedPublisher {
            inner,
            owner: self.owner,
            publish_gate: self.publish_gate,
        }))
    }
}

struct ObservedPublisher {
    inner: Box<dyn CheckpointPublisher>,
    owner: Arc<()>,
    publish_gate: Option<PublishGate>,
}

impl CheckpointPublisher for ObservedPublisher {
    fn metadata(&self) -> &CheckpointMetadata {
        self.inner.metadata()
    }

    fn publish(self: Box<Self>, state: &Checkpoint) -> Result<CheckpointMetadata, ControllerError> {
        if let Some((entered, gate)) = self.publish_gate {
            entered.send(()).unwrap();
            gate.recv_timeout(TIMEOUT).unwrap();
        }
        let result = self.inner.publish(state);
        drop(self.owner);
        result
    }
}

pub(super) fn file_config(root: &Path, fault_tolerant: bool) -> PipelineConfig {
    create_dir(root.join("storage")).unwrap();
    File::create_new(root.join("input.csv")).unwrap();
    let mut config = json!({
        "name": "checkpoint-lifecycle", "workers": 1,
        "storage_config": { "path": root.join("storage") }, "storage": true,
        "clock_resolution_usecs": null, "max_buffering_delay_usecs": 0,
        "program_ir": {
            "mir": {
                "0": { "operation": "source", "table": "test_input1", "persistent_id": "input" },
                "1": { "operation": "sink", "view": "test_output1", "persistent_id": "output" }
            },
            "program_schema": { "inputs": [], "outputs": [] }
        },
        "inputs": { "source": {
            "stream": "test_input1",
            "transport": { "name": "file_input", "config": {
                "path": root.join("input.csv"), "follow": true
            }},
            "format": { "name": "csv" }
        }}
    });
    if fault_tolerant {
        config["fault_tolerance"] = json!({ "checkpoint_interval": null });
    }
    serde_json::from_value(config).unwrap()
}

pub(super) fn append_ids(path: &Path, ids: &[usize]) {
    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(File::options().append(true).open(path).unwrap());
    for &id in ids {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();
}

pub(super) fn weights(controller: &Controller) -> Vec<(i64, i64)> {
    query(
        controller,
        "select id, count(*) from test_output1 group by id order by id",
    )
    .unwrap()
    .iter()
    .flat_map(|batch| {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (ids.value(i), counts.value(i)))
            .collect::<Vec<_>>()
    })
    .collect()
}

pub(super) fn new_controller(
    engine: &dyn PipelineTestEngine,
    config: &PipelineConfig,
) -> Controller {
    engine
        .controller_with_options(
            &identity(&[Some("output")]),
            config,
            ControllerOptions {
                keep_program_diff: true,
                ..ControllerOptions::default()
            },
            Box::new(|error, _| panic!("unexpected controller error: {error}")),
        )
        .unwrap()
}

fn observed_controller(
    engine: &dyn PipelineTestEngine,
    config: &PipelineConfig,
    publish_gate: Option<PublishGate>,
) -> (Controller, mpsc::Receiver<Weak<()>>, mpsc::Receiver<()>) {
    let factory = engine
        .circuit_factory(&identity(&[Some("output")]), None)
        .unwrap();
    let (prepared, owners) = mpsc::channel();
    let (committed, commits) = mpsc::channel();
    let factory = Box::new(move |params| {
        let mut circuit = factory(params)?;
        circuit.driver = Box::new(ObservedDriver {
            inner: circuit.driver,
            prepared,
            committed,
            publish_gate,
        });
        Ok(circuit)
    });
    let controller = Controller::with_test_config_keep_program_diff(
        factory,
        &engine.open_storage(),
        config,
        Box::new(|error, _| panic!("unexpected controller error: {error}")),
    )
    .unwrap();
    (controller, owners, commits)
}

fn start_three(controller: &Controller, input: &Path) {
    append_ids(input, &[7, 23, 59]);
    controller.start();
    wait(
        || {
            controller
                .status()
                .global_metrics
                .num_total_processed_records()
                == 3
        },
        20_000,
    )
    .unwrap();
    assert_eq!(weights(controller), [(7, 1), (23, 1), (59, 1)]);
}

/// A pending checkpoint may be cancelled by shutdown, but it must never
/// replace a valid checkpoint with circuit data lacking its input cursors.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance, Profiles))]
pub fn pending_checkpoint_during_stop_preserves_file_rows(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    let config = file_config(temp.path(), true);
    let input = temp.path().join("input.csv");
    let (controller, owners, commits) = observed_controller(engine, &config, None);
    start_three(&controller, &input);
    controller.checkpoint().unwrap();
    assert!(owners.recv_timeout(TIMEOUT).unwrap().upgrade().is_none());
    commits.recv_timeout(TIMEOUT).unwrap();

    let (entered, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    controller.start_graph_profile(Box::new(move |_| {
        entered.send(()).unwrap();
        gate.recv_timeout(TIMEOUT).unwrap();
    }));
    entry.recv_timeout(TIMEOUT).unwrap();
    let (sent, received) = mpsc::channel();
    controller.start_checkpoint(Box::new(move |result| {
        let _ = sent.send(result);
    }));
    controller.initiate_stop();
    release.send(()).unwrap();
    controller.stop().unwrap();
    received.recv_timeout(TIMEOUT).unwrap().ok();
    // Clean up a detached baseline publisher before inspecting durable state.
    if let Ok(owner) = owners.try_recv() {
        wait(|| owner.upgrade().is_none(), 20_000).unwrap();
    }
    let checkpoint = open_storage(engine, &config)
        .latest_checkpoint()
        .unwrap()
        .unwrap();
    info!(
        "after stop: inputs={:?}, cursors={:?}",
        checkpoint.config.inputs.keys().collect::<Vec<_>>(),
        checkpoint.input_metadata
    );
    let restored = new_controller(engine, &config);
    restored.start();
    append_ids(&input, &[101]);
    wait(|| weights(&restored).contains(&(101, 1)), 20_000).unwrap();
    let actual = weights(&restored);
    restored.stop().unwrap();
    info!("recovered File weights: {actual:?}");
    assert_eq!(actual, [(7, 1), (23, 1), (59, 1), (101, 1)]);
    assert!(checkpoint.config.inputs.contains_key("source"));
    assert!(checkpoint.input_metadata.0.contains_key("source"));
}

/// Output buffering is real: no connector fault or fake storage is needed
/// to leave a publisher waiting on work that shutdown will never transmit.
#[engine_test(requires(Storage, Checkpoints))]
pub fn stop_joins_checkpoint_waiting_for_file_output(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    let mut config = file_config(temp.path(), false);
    let output = temp.path().join("output.csv");
    config.outputs.insert(
        "sink".into(),
        serde_json::from_value(json!({
            "stream": "test_output1",
            "transport": { "name": "file_output", "config": { "path": output } },
            "format": { "name": "csv" },
            "enable_output_buffer": true, "max_output_buffer_size_records": 1000
        }))
        .unwrap(),
    );
    let (controller, owners, commits) = observed_controller(engine, &config, None);
    start_three(&controller, &temp.path().join("input.csv"));
    wait(
        || output_endpoint_metrics(&controller, "sink").buffered_records == 3,
        20_000,
    )
    .unwrap();
    assert!(std::fs::read(&output).unwrap().is_empty());
    let (sent, received) = mpsc::channel();
    controller.start_checkpoint(Box::new(move |result| {
        let _ = sent.send(result);
    }));
    let owner = owners.recv_timeout(TIMEOUT).unwrap();
    commits.recv_timeout(TIMEOUT).unwrap();
    let sink = controller.inner.output_endpoint_id_by_name("sink").unwrap();
    controller.clone().stop().unwrap();
    let released_at_stop = owner.upgrade().is_none();
    let before_cleanup = published_checkpoints(engine, &config);
    // Release the baseline's detached publisher before the assertion fails.
    controller.disconnect_output(&sink);
    wait(|| owner.upgrade().is_none(), 20_000).unwrap();
    let after_cleanup = published_checkpoints(engine, &config);
    received.recv_timeout(TIMEOUT).unwrap().ok();
    info!(
        "publisher released at stop={released_at_stop}; checkpoints before/after cleanup={before_cleanup:?}/{after_cleanup:?}"
    );
    assert!(
        released_at_stop,
        "stop returned while its publisher still owned checkpoint state"
    );
    assert!(
        after_cleanup.is_empty(),
        "a cancelled output wait published a checkpoint"
    );
}

#[derive(Clone, Copy, Debug)]
enum PanickingCallback {
    None,
    Checkpoint,
    Error,
}

fn publication_during_stop(engine: &dyn PipelineTestEngine, callback: PanickingCallback) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    let config = file_config(temp.path(), false);
    let (entered, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let (controller, owners, _commits) =
        observed_controller(engine, &config, Some((entered, gate)));
    start_three(&controller, &temp.path().join("input.csv"));
    let (sent, received) = mpsc::channel();
    controller.start_checkpoint(Box::new(move |result| {
        let _ = sent.send(result);
        if matches!(callback, PanickingCallback::Checkpoint) {
            panic!("injected checkpoint cancellation callback panic");
        }
    }));
    entry.recv_timeout(TIMEOUT).unwrap();
    let owner = owners.recv_timeout(TIMEOUT).unwrap();
    if matches!(callback, PanickingCallback::Error) {
        // The fixture's error callback panics too, after run() catches this
        // callback panic. Before the fix that bypasses finish() entirely.
        let (entered, entry) = mpsc::channel();
        controller.start_graph_profile(Box::new(move |profile| {
            profile.unwrap();
            entered.send(()).unwrap();
            panic!("injected profile callback panic during publication");
        }));
        entry.recv_timeout(TIMEOUT).unwrap();
    }
    let (done, stopped) = mpsc::channel();
    let stop = thread::spawn(move || {
        let _ = done.send(controller.stop());
    });
    let early = stopped.recv_timeout(Duration::from_millis(300)).ok();
    let returned_early = early.is_some();
    release.send(()).unwrap();
    let result = early.unwrap_or_else(|| stopped.recv_timeout(TIMEOUT).unwrap());
    stop.join().unwrap();
    wait(|| owner.upgrade().is_none(), 20_000).unwrap();
    if !matches!(callback, PanickingCallback::Error) {
        received.recv_timeout(TIMEOUT).unwrap().ok();
    }
    let checkpoint = open_storage(engine, &config)
        .latest_checkpoint()
        .unwrap()
        .unwrap();
    let restored = new_controller(engine, &config);
    let actual = weights(&restored);
    restored.stop().unwrap();
    info!(
        "panicking callback={callback:?}; stop returned during publication={returned_early}; recovered weights={actual:?}"
    );
    assert_eq!(checkpoint.processed_records, 3);
    assert_eq!(actual, [(7, 1), (23, 1), (59, 1)]);
    assert_eq!(
        result.is_err(),
        !matches!(callback, PanickingCallback::None)
    );
    assert!(
        !returned_early,
        "stop returned while checkpoint publication was blocked"
    );
}

/// Once a publisher has entered storage I/O it cannot be cancelled. Blocking
/// stop must wait for that operation before a new incarnation opens storage.
#[engine_test(requires(Storage, Checkpoints))]
pub fn stop_waits_for_checkpoint_publication(engine: &dyn PipelineTestEngine) {
    publication_during_stop(engine, PanickingCallback::None);
}

#[engine_test(requires(Storage, Checkpoints))]
pub fn panicking_checkpoint_callback_does_not_detach_publication(engine: &dyn PipelineTestEngine) {
    publication_during_stop(engine, PanickingCallback::Checkpoint);
}

#[engine_test(requires(Storage, Checkpoints, Profiles))]
pub fn panicking_error_callback_does_not_detach_publication(engine: &dyn PipelineTestEngine) {
    publication_during_stop(engine, PanickingCallback::Error);
}
