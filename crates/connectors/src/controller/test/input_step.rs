//! Accepted input must survive removal or an application failure, using the
//! real File reader, parser, native input handle, checkpoint and step journal.

use super::checkpoint_lifecycle::{append_ids, file_config, new_controller, weights};
use super::*;
use feldera_adapterlib::{
    ConnectorMetadata, PipelineState, catalog::StagedBuffers, format::ParseError,
};

const TIMEOUT: Duration = Duration::from_secs(20);
const ROWS: [(i64, i64); 4] = [(11, 1), (23, 1), (37, 1), (51, 1)];
const INJECTED_FAILURE: &str = "injected failure after applying two of four rows";

enum Intervention {
    Pass,
    AfterFlush {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    },
    // Native flush normally returns Ok. This deliberately injects an error
    // at the fallible StagedBuffers boundary, after two real rows were applied.
    PartialFailure,
}

struct ObservedFile {
    inner: Box<dyn TransportInputEndpoint>,
    intervention: Arc<Intervention>,
}

impl InputEndpoint for ObservedFile {
    fn fault_tolerance(&self) -> Option<FtModel> {
        self.inner.fault_tolerance()
    }
}

impl TransportInputEndpoint for ObservedFile {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        self.inner.open(
            consumer,
            Box::new(ObservedParser {
                inner: parser,
                intervention: self.intervention.clone(),
            }),
            schema,
            resume_info,
        )
    }
}

struct ObservedParser {
    inner: Box<dyn Parser>,
    intervention: Arc<Intervention>,
}

impl Parser for ObservedParser {
    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        self.inner.parse(data, metadata)
    }

    fn stage(&self, mut buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        let records: usize = buffers.iter().map(|buffer| buffer.len().records).sum();
        if records == 0 {
            return self.inner.stage(buffers);
        }
        assert_eq!(records, 4, "the intervention must cover the four-row batch");
        if matches!(*self.intervention, Intervention::PartialFailure) {
            let prefix = buffers.take_some(2).unwrap();
            assert_eq!(prefix.len().records, 2);
            buffers = vec![prefix];
        }
        Box::new(ObservedStage {
            inner: self.inner.stage(buffers),
            intervention: self.intervention.clone(),
        })
    }

    fn splitter(&self) -> Box<dyn Splitter> {
        self.inner.splitter()
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self {
            inner: self.inner.fork(),
            intervention: self.intervention.clone(),
        })
    }
}

struct ObservedStage {
    inner: Box<dyn StagedBuffers>,
    intervention: Arc<Intervention>,
}

impl StagedBuffers for ObservedStage {
    fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.flush()?;
        match &*self.intervention {
            Intervention::Pass => Ok(()),
            Intervention::AfterFlush { entered, release } => {
                entered.send(()).unwrap();
                release.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
                // File reports extended normally after the gate, then
                // processes Disconnect. Its completion contract is intact.
                Ok(())
            }
            Intervention::PartialFailure => Err(anyhow!(INJECTED_FAILURE)),
        }
    }
}

fn connect_file(
    controller: &Controller,
    config: InputEndpointConfig,
    root: &Path,
    intervention: Arc<Intervention>,
) -> u64 {
    let inner = input_transport_config_to_endpoint(
        &config.connector_config.transport,
        "source",
        root,
        None,
    )
    .unwrap()
    .unwrap();
    controller
        .add_input_endpoint(
            "source",
            config,
            Box::new(ObservedFile {
                inner,
                intervention,
            }),
            None,
        )
        .unwrap()
}

fn removal(engine: &dyn PipelineTestEngine, in_flight: bool) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    let mut config = file_config(temp.path(), true);
    config.global.min_batch_size_records = 1_000_000;
    config.global.max_buffering_delay = Some(Duration::from_secs(1_000).into());
    let input = config.inputs.remove("source").unwrap();
    let controller = new_controller(engine, &config);
    let (entered, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let endpoint = connect_file(
        &controller,
        input,
        temp.path(),
        Arc::new(Intervention::AfterFlush {
            entered,
            release: Mutex::new(gate),
        }),
    );
    let reader = controller.get_input_endpoint("source").unwrap();
    controller.start();
    controller.request_step();
    let anchor = controller.checkpoint().unwrap();
    assert_eq!(anchor.input_metadata.0["source"]["offsets"]["end"], 0);
    assert!(weights(&controller).is_empty());

    append_ids(&temp.path().join("input.csv"), &[11, 23, 37, 51]);
    wait(
        || controller.status().num_buffered_input_records() == 4,
        20_000,
    )
    .unwrap();
    let before = controller.status().global_metrics.total_completed_steps();
    controller.request_step();
    entry.recv_timeout(TIMEOUT).unwrap();
    if in_flight {
        // Permit either synchronous or asynchronous retirement: always
        // release the flush gate, even if removal waits for completion.
        let (started, start) = mpsc::channel();
        let (removed, removal) = mpsc::channel();
        let removing = thread::spawn({
            let controller = controller.clone();
            move || {
                started.send(()).unwrap();
                controller.disconnect_input(&endpoint);
                removed.send(()).unwrap();
            }
        });
        start.recv_timeout(TIMEOUT).unwrap();
        let early = removal.recv_timeout(Duration::from_millis(300)).is_ok();
        release.send(()).unwrap();
        if !early {
            removal.recv_timeout(TIMEOUT).unwrap();
        }
        removing.join().unwrap();
    } else {
        release.send(()).unwrap();
        wait(
            || controller.status().global_metrics.total_completed_steps() > before,
            20_000,
        )
        .unwrap();
        controller.disconnect_input(&endpoint);
    }
    wait(|| reader.is_closed(), 20_000).unwrap();
    wait(
        || controller.status().global_metrics.total_completed_steps() > before,
        20_000,
    )
    .unwrap();
    // Journal retirement without a new checkpoint that could mask lost input.
    let before = controller.status().global_metrics.total_completed_steps();
    controller.request_step();
    wait(
        || controller.status().global_metrics.total_completed_steps() > before,
        20_000,
    )
    .unwrap();
    let live = weights(&controller);
    controller.stop().unwrap();
    assert_eq!(live, ROWS);

    let records = {
        let storage = (engine.open_storage())(&config).unwrap().unwrap();
        let journal = storage
            .open_journal(crate::engine::JournalMode::Open(anchor.step))
            .unwrap();
        journal
            .list_steps(anchor.step)
            .unwrap()
            .into_iter()
            .map(|step| journal.read(step).unwrap().unwrap())
            .collect::<Vec<_>>()
    };
    for record in &records {
        info!("removal in_flight={in_flight}, durable journal: {record:?}");
    }
    // Restore the checkpoint's source, then let the actual journal retire it.
    // No source is re-added, and no config change discards the journal.
    let restored = new_controller(engine, &anchor.config);
    wait(|| !restored.is_replaying(), 20_000).unwrap();
    let recovered = weights(&restored);
    restored.stop().unwrap();
    info!("removal in_flight={in_flight}, live={live:?}, recovered={recovered:?}");
    assert_eq!(recovered, ROWS);
    assert!(
        records
            .iter()
            .any(|record| record.remove_inputs.contains("source"))
    );
    assert_eq!(
        records
            .iter()
            .filter_map(|record| record.input_logs.get("source"))
            .map(|log| log.checksums.num_records)
            .sum::<u64>(),
        4
    );
}

#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn removal_after_completed_step_preserves_file_rows(engine: &dyn PipelineTestEngine) {
    removal(engine, false);
}

#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn removal_during_input_step_preserves_file_rows(engine: &dyn PipelineTestEngine) {
    removal(engine, true);
}

fn apply_failure(engine: &dyn PipelineTestEngine, inject: bool) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("input.csv");
    let mut config = file_config(temp.path(), true);
    config.global.min_batch_size_records = 1_000_000;
    config.global.max_buffering_delay = Some(Duration::from_secs(1_000).into());
    let input = config.inputs.remove("source").unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let controller = engine
        .controller_with_options(
            &identity(&[Some("output")]),
            &config,
            ControllerOptions {
                keep_program_diff: true,
                ..Default::default()
            },
            Box::new({
                let errors = errors.clone();
                move |error, _| errors.lock().unwrap().push(error.to_string())
            }),
        )
        .unwrap();
    connect_file(
        &controller,
        input,
        temp.path(),
        Arc::new(if inject {
            Intervention::PartialFailure
        } else {
            Intervention::Pass
        }),
    );
    let reader = controller.get_input_endpoint("source").unwrap();
    controller.start();
    controller.request_step();
    let anchor = controller.checkpoint().unwrap();
    assert_eq!(anchor.input_metadata.0["source"]["offsets"]["end"], 0);
    assert!(weights(&controller).is_empty());

    append_ids(&path, &[11, 23, 37, 51]);
    wait(
        || controller.status().num_buffered_input_records() == 4,
        20_000,
    )
    .unwrap();
    controller.request_step();
    wait(
        || {
            controller.status().num_total_processed_records() == 4
                || errors
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|error| error.contains(INJECTED_FAILURE))
        },
        20_000,
    )
    .unwrap();
    let checkpoint = TOKIO
        .block_on(async { tokio::time::timeout(TIMEOUT, controller.async_checkpoint()).await })
        .expect("checkpoint must complete or reject, not hang");
    let live = (controller.state() != PipelineState::Terminated).then(|| weights(&controller));
    info!(
        "input apply FAULT_INJECTION={inject}, checkpoint={:?}, live={live:?}, errors={:?}",
        checkpoint.as_ref().map(|c| &c.input_metadata),
        errors.lock().unwrap()
    );
    if checkpoint.is_err() {
        // Cleanup must not depend on the embedding application calling stop.
        wait(|| reader.is_closed(), 20_000).unwrap();
    }
    controller.stop().unwrap();
    if inject {
        assert!(
            errors
                .lock()
                .unwrap()
                .iter()
                .any(|error| error.contains(INJECTED_FAILURE))
        );
    } else {
        assert!(errors.lock().unwrap().is_empty());
        assert!(checkpoint.is_ok());
    }

    let saved = {
        let storage = (engine.open_storage())(&config).unwrap().unwrap();
        let saved = storage.latest_checkpoint().unwrap().unwrap();
        let journal = storage
            .open_journal(crate::engine::JournalMode::Open(anchor.step))
            .unwrap();
        for step in journal.list_steps(anchor.step).unwrap() {
            let record = journal.read(step).unwrap().unwrap();
            info!("input apply FAULT_INJECTION={inject}, durable journal: {record:?}");
            if checkpoint.is_err() {
                assert!(!record.remove_inputs.contains("source"));
                assert!(
                    record
                        .input_logs
                        .get("source")
                        .is_none_or(|log| log.checksums.num_records == 0),
                    "a rejected input batch must not advance the replay journal"
                );
            }
        }
        saved
    };
    info!(
        "input apply FAULT_INJECTION={inject}, saved rows={}, cursor={:?}",
        saved.processed_records, saved.input_metadata
    );
    match &checkpoint {
        Ok(checkpoint) => {
            assert_eq!(
                saved.circuit.as_ref().unwrap().uuid,
                checkpoint.circuit.as_ref().unwrap().uuid
            );
            assert_eq!(
                saved.input_metadata.0["source"]["offsets"]["end"],
                std::fs::metadata(&path).unwrap().len()
            );
        }
        Err(_) => {
            assert_eq!(
                saved.circuit.as_ref().unwrap().uuid,
                anchor.circuit.as_ref().unwrap().uuid,
                "safe abort must preserve the clean anchor"
            );
            assert_eq!(saved.input_metadata.0, anchor.input_metadata.0);
            assert_eq!(saved.processed_records, 0);
            assert!(saved.config.inputs.contains_key("source"));
        }
    }

    // Recovery uses the original File source, without the fault injector.
    let restored = new_controller(engine, &anchor.config);
    wait(|| !restored.is_replaying(), 20_000).unwrap();
    if checkpoint.is_err() {
        assert!(weights(&restored).is_empty());
        restored.start();
        wait(
            || restored.status().num_buffered_input_records() == 4,
            20_000,
        )
        .unwrap();
        restored.request_step();
        wait(
            || restored.status().num_total_processed_records() == 4,
            20_000,
        )
        .unwrap();
    }
    let recovered = weights(&restored);
    restored.stop().unwrap();
    info!("input apply FAULT_INJECTION={inject}, recovered={recovered:?}");
    assert_eq!(recovered, ROWS);
    if checkpoint.is_ok() {
        assert_eq!(live, Some(ROWS.to_vec()));
    }
}

#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn partial_input_apply_error_preserves_recoverable_state(engine: &dyn PipelineTestEngine) {
    apply_failure(engine, true);
}

#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn successful_input_apply_preserves_recoverable_state(engine: &dyn PipelineTestEngine) {
    apply_failure(engine, false);
}

const SYNCHRONOUS_FAILURE: &str = "injected synchronous native input flush error";

/// The native buffer still applies its rows. Only the fallible result is
/// fault-injected, so the real synchronous InputQueue invokes check_flush.
struct FailingBuffer(Box<dyn InputBuffer>);

impl InputBuffer for FailingBuffer {
    fn flush(&mut self) -> anyhow::Result<()> {
        self.0.flush()?;
        Err(anyhow!(SYNCHRONOUS_FAILURE))
    }

    fn len(&self) -> BufferSize {
        self.0.len()
    }

    fn hash(&self, hasher: &mut dyn std::hash::Hasher) {
        self.0.hash(hasher);
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.0
            .take_some(n)
            .map(|buffer| Box::new(Self(buffer)) as Box<dyn InputBuffer>)
    }
}

/// Unlike File's worker, InputQueue can invoke a flush error callback on the
/// circuit thread itself. initiate_stop is documented as callback-safe.
#[engine_test]
pub fn synchronous_flush_error_callback_can_initiate_stop(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let callback_controller: Arc<Mutex<Option<Controller>>> = Arc::new(Mutex::new(None));
    let (entered, entry) = mpsc::channel();
    let (returned, result) = mpsc::channel();
    let config = serde_json::from_value(json!({
        "name": "synchronous-input-error", "workers": 1, "clock_resolution_usecs": null
    }))
    .unwrap();
    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new({
                let callback_controller = callback_controller.clone();
                move |error, _| {
                    assert!(error.to_string().contains(SYNCHRONOUS_FAILURE), "{error}");
                    let stopping = callback_controller
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .clone();
                    entered.send(()).unwrap();
                    stopping.initiate_stop();
                    returned.send(()).unwrap();
                }
            }),
        )
        .unwrap();
    *callback_controller.lock().unwrap() = Some(controller.clone());
    let endpoint = TransactionalInputEndpoint::new();
    controller
        .add_input_endpoint(
            "synchronous",
            serde_json::from_value(json!({
                "stream": "test_input1",
                "transport": { "name": "file_input", "config": { "path": "unused" } },
                "format": { "name": "csv" }
            }))
            .unwrap(),
            Box::new(endpoint.clone()),
            None,
        )
        .unwrap();
    controller.start();
    {
        let mut details = endpoint.details.lock().unwrap();
        let details = details.as_mut().unwrap();
        let (buffer, errors) = details.parser.parse(b"11,true,,eleven\n", None);
        assert!(errors.is_empty());
        details.queue.push(
            (Some(Box::new(FailingBuffer(buffer.unwrap()))), errors),
            Utc::now(),
        );
    }
    controller.request_step();
    entry.recv_timeout(TIMEOUT).unwrap();
    let callback_returned = result.recv_timeout(TIMEOUT).is_ok();
    callback_controller.lock().unwrap().take();
    info!("synchronous native flush callback returned={callback_returned}");
    assert!(
        callback_returned,
        "initiate_stop deadlocked inside the input error callback"
    );
    controller.stop().unwrap();
}
