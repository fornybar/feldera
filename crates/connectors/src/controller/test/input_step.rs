//! Accepted input must survive removal or an application failure, using the
//! real File reader, parser, native input handle, checkpoint and step journal.

use super::checkpoint_lifecycle::{append_ids, file_config, new_controller, weights};
use super::*;
use feldera_adapterlib::{ConnectorMetadata, catalog::StagedBuffers, format::ParseError};

const TIMEOUT: Duration = Duration::from_secs(20);
const ROWS: [(i64, i64); 4] = [(11, 1), (23, 1), (37, 1), (51, 1)];

struct AfterFlush {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

struct ObservedFile {
    inner: Box<dyn TransportInputEndpoint>,
    gate: Arc<AfterFlush>,
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
                gate: self.gate.clone(),
            }),
            schema,
            resume_info,
        )
    }
}

struct ObservedParser {
    inner: Box<dyn Parser>,
    gate: Arc<AfterFlush>,
}

impl Parser for ObservedParser {
    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        self.inner.parse(data, metadata)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        let records: usize = buffers.iter().map(|buffer| buffer.len().records).sum();
        if records == 0 {
            return self.inner.stage(buffers);
        }
        assert_eq!(records, 4, "the gate must cover the entire four-row batch");
        Box::new(ObservedStage {
            inner: self.inner.stage(buffers),
            gate: self.gate.clone(),
        })
    }

    fn splitter(&self) -> Box<dyn Splitter> {
        self.inner.splitter()
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self {
            inner: self.inner.fork(),
            gate: self.gate.clone(),
        })
    }
}

struct ObservedStage {
    inner: Box<dyn StagedBuffers>,
    gate: Arc<AfterFlush>,
}

impl StagedBuffers for ObservedStage {
    fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.flush()?;
        self.gate.entered.send(()).unwrap();
        self.gate
            .release
            .lock()
            .unwrap()
            .recv_timeout(TIMEOUT)
            .unwrap();
        // The real File worker reports extended after this returns, then
        // processes Disconnect. The documented completion contract is intact.
        Ok(())
    }
}

fn connect_file(
    controller: &Controller,
    config: InputEndpointConfig,
    root: &Path,
    gate: Arc<AfterFlush>,
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
            Box::new(ObservedFile { inner, gate }),
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
        Arc::new(AfterFlush {
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
