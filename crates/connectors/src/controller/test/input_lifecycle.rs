//! Input admission and shutdown through the real File transport.

use super::*;
use crate::transport::file::FileInputEndpoint;
use std::panic::{AssertUnwindSafe, catch_unwind};

const TIMEOUT: Duration = Duration::from_secs(20);

fn controller(engine: &dyn PipelineTestEngine) -> Controller {
    let config = serde_json::from_value(json!({
        "name": "input-lifecycle",
        "workers": 1,
        "clock_resolution_usecs": null,
        "inputs": {},
    }))
    .unwrap();
    engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|error, _| panic!("unexpected controller error: {error}")),
        )
        .unwrap()
}

fn file_config(path: &Path) -> InputEndpointConfig {
    serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": {
            "name": "file_input",
            "config": { "path": path, "follow": true },
        },
        "format": { "name": "csv" },
    }))
    .unwrap()
}

/// Closing admission must not prevent ordinary pre-stop connections.
#[engine_test]
pub fn input_admission_after_stop_is_rejected(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let file = NamedTempFile::new().unwrap();
    let controller = controller(engine);
    let config = file_config(file.path());
    controller.connect_input("before", &config, None).unwrap();
    let reader = controller.get_input_endpoint("before").unwrap();
    controller.clone().stop().unwrap();
    wait(|| reader.is_closed(), 20_000).unwrap();

    let result = controller.connect_input("after", &config, None);
    // Clean up a wrongly admitted reader too, so the failing baseline does
    // not leave an input thread behind.
    if let Ok(id) = &result {
        let reader = controller.get_input_endpoint("after").unwrap();
        controller.disconnect_input(id);
        wait(|| reader.is_closed(), 20_000).unwrap();
    }
    assert!(
        matches!(result, Err(ControllerError::ControllerExit)),
        "a stopped controller must reject input admission: {result:?}"
    );
}

struct GatedFileOpen {
    inner: FileInputEndpoint,
    entered: mpsc::Sender<()>,
    gate: Mutex<mpsc::Receiver<()>>,
}

impl InputEndpoint for GatedFileOpen {
    fn fault_tolerance(&self) -> Option<FtModel> {
        self.inner.fault_tolerance()
    }
}

impl TransportInputEndpoint for GatedFileOpen {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        self.entered.send(()).unwrap();
        self.gate.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
        self.inner.open(consumer, parser, schema, resume)
    }
}

/// Admission begins before stop, but the real File reader is returned after
/// stop's disconnect sweep. It must be rejected without panicking, and stop
/// must account for it before returning.
#[engine_test]
pub fn stop_fences_an_admitted_input_open(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let file = NamedTempFile::new().unwrap();
    let controller = controller(engine);
    let (entered, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let endpoint = GatedFileOpen {
        inner: FileInputEndpoint::new(
            serde_json::from_value(json!({ "path": file.path(), "follow": true })).unwrap(),
        ),
        entered,
        gate: Mutex::new(gate),
    };
    let config = file_config(file.path());
    let opening = controller.clone();
    let open = thread::spawn(move || {
        catch_unwind(AssertUnwindSafe(|| {
            opening.add_input_endpoint("late", config, Box::new(endpoint), None)
        }))
    });
    entry.recv_timeout(TIMEOUT).unwrap();
    controller.initiate_stop();
    let (done, stopped) = mpsc::channel();
    let stop = thread::spawn(move || done.send(controller.stop()).unwrap());
    let early_result = stopped.recv_timeout(Duration::from_millis(300)).ok();
    let returned_early = early_result.is_some();
    release.send(()).unwrap();
    let open_result = open.join().unwrap();
    early_result
        .unwrap_or_else(|| stopped.recv_timeout(TIMEOUT).unwrap())
        .unwrap();
    stop.join().unwrap();

    assert!(open_result.is_ok(), "late reader installation panicked");
    assert!(
        matches!(open_result.unwrap(), Err(ControllerError::ControllerExit)),
        "late reader installation was not rejected"
    );
    assert!(!returned_early, "stop returned before the admitted open");
}

/// File has no asynchronous cleanup hook. Add one around its real reader to
/// exercise the InputReader completion contract independently of a transport.
struct CleanupFile {
    inner: FileInputEndpoint,
    completion: Mutex<Option<futures::future::BoxFuture<'static, ()>>>,
    disconnect: Option<Arc<DisconnectGate>>,
}

impl InputEndpoint for CleanupFile {
    fn fault_tolerance(&self) -> Option<FtModel> {
        self.inner.fault_tolerance()
    }
}

impl TransportInputEndpoint for CleanupFile {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        let reader: Arc<dyn InputReader> =
            Arc::from(self.inner.open(consumer, parser, schema, resume)?);
        let observed = reader.clone();
        let completion = self.completion.lock().unwrap().take().map(|completion| {
            Box::pin(async move {
                while !observed.is_closed() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                completion.await;
            }) as futures::future::BoxFuture<'static, ()>
        });
        Ok(Box::new(CleanupReader {
            reader,
            completion: Mutex::new(completion),
            disconnect: self.disconnect.clone(),
        }))
    }
}

struct DisconnectGate {
    entered: mpsc::Sender<()>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

struct CleanupReader {
    reader: Arc<dyn InputReader>,
    completion: Mutex<Option<futures::future::BoxFuture<'static, ()>>>,
    disconnect: Option<Arc<DisconnectGate>>,
}

impl InputReader for CleanupReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        if matches!(command, InputReaderCommand::Disconnect)
            && let Some(gate) = &self.disconnect
            && let Some(release) = gate.release.lock().unwrap().take()
        {
            gate.entered.send(()).unwrap();
            release.recv_timeout(TIMEOUT).unwrap();
        }
        self.reader.request(command);
    }

    fn is_closed(&self) -> bool {
        self.reader.is_closed()
    }

    fn take_completion(&self) -> Option<futures::future::BoxFuture<'static, ()>> {
        self.completion.lock().unwrap().take()
    }
}

#[engine_test]
pub fn concurrent_stop_waits_for_input_cleanup(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let file = NamedTempFile::new().unwrap();
    let controller = controller(engine);
    let (entered, entry) = mpsc::channel();
    let (release, gate) = oneshot::channel();
    controller
        .add_input_endpoint(
            "cleanup",
            file_config(file.path()),
            Box::new(CleanupFile {
                inner: FileInputEndpoint::new(
                    serde_json::from_value(json!({ "path": file.path(), "follow": true })).unwrap(),
                ),
                completion: Mutex::new(Some(Box::pin(async move {
                    entered.send(()).unwrap();
                    gate.await.unwrap();
                }))),
                disconnect: None,
            }),
            None,
        )
        .unwrap();
    let (first_done, first_result) = mpsc::channel();
    let first_controller = controller.clone();
    let first = thread::spawn(move || first_done.send(first_controller.stop()).unwrap());
    entry.recv_timeout(TIMEOUT).unwrap();
    assert!(first_result.try_recv().is_err());

    let (second_done, second_result) = mpsc::channel();
    let second = thread::spawn(move || second_done.send(controller.stop()).unwrap());
    let early = second_result.recv_timeout(Duration::from_millis(300)).ok();
    let returned_early = early.is_some();
    release.send(()).unwrap();
    first_result.recv_timeout(TIMEOUT).unwrap().unwrap();
    early
        .unwrap_or_else(|| second_result.recv_timeout(TIMEOUT).unwrap())
        .unwrap();
    first.join().unwrap();
    second.join().unwrap();
    assert!(!returned_early, "second stop returned before input cleanup");
}

fn disconnect_dispatch(engine: &dyn PipelineTestEngine, first_blocks: bool) {
    init_test_logger();
    let file = NamedTempFile::new().unwrap();
    let controller = controller(engine);
    let (entered, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    controller
        .add_input_endpoint(
            "dispatch",
            file_config(file.path()),
            Box::new(CleanupFile {
                inner: FileInputEndpoint::new(
                    serde_json::from_value(json!({ "path": file.path(), "follow": true })).unwrap(),
                ),
                // No completion hook: this checks ownership of Disconnect itself.
                completion: Mutex::new(None),
                disconnect: Some(Arc::new(DisconnectGate {
                    entered,
                    release: Mutex::new(Some(gate)),
                })),
            }),
            None,
        )
        .unwrap();
    let reader = controller.get_input_endpoint("dispatch").unwrap();
    let first_controller = controller.clone();
    let first = thread::spawn(move || {
        if first_blocks {
            first_controller.stop().unwrap();
        } else {
            first_controller.initiate_stop();
        }
    });
    entry.recv_timeout(TIMEOUT).unwrap();
    assert!(!reader.is_closed());
    let (done, stopped) = mpsc::channel();
    let second = thread::spawn(move || done.send(controller.stop()).unwrap());
    let early = stopped.recv_timeout(Duration::from_millis(300)).ok();
    let returned_early = early.is_some();
    let closed_early = reader.is_closed();
    release.send(()).unwrap();
    early
        .unwrap_or_else(|| stopped.recv_timeout(TIMEOUT).unwrap())
        .unwrap();
    first.join().unwrap();
    second.join().unwrap();
    wait(|| reader.is_closed(), 20_000).unwrap();
    info!(
        "disconnect first_blocks={first_blocks}, stop_returned_early={returned_early}, reader_closed_early={closed_early}"
    );
    assert!(
        !returned_early,
        "stop returned before Disconnect was dispatched"
    );
}

#[engine_test]
pub fn concurrent_stop_waits_for_disconnect_dispatch(engine: &dyn PipelineTestEngine) {
    disconnect_dispatch(engine, true);
}

#[engine_test]
pub fn stop_waits_for_initiated_disconnect_dispatch(engine: &dyn PipelineTestEngine) {
    disconnect_dispatch(engine, false);
}

/// Even if reporting a callback panic triggers another panic in the error
/// callback, blocking stop must still disconnect the readers.
#[engine_test(requires(Profiles))]
pub fn stop_disconnects_inputs_after_callback_panic(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let file = NamedTempFile::new().unwrap();
    let controller = controller(engine);
    controller
        .connect_input("source", &file_config(file.path()), None)
        .unwrap();
    let reader = controller.get_input_endpoint("source").unwrap();
    let (entered, entry) = mpsc::channel();
    controller.start_graph_profile(Box::new(move |profile| {
        profile.unwrap();
        entered.send(()).unwrap();
        panic!("injected profile callback panic");
    }));
    entry.recv_timeout(TIMEOUT).unwrap();
    let stopped = controller.stop();
    let closed = wait(|| reader.is_closed(), 20_000);
    assert!(stopped.is_err(), "the callback panic was not reported");
    closed.expect("stop left the File reader running after a callback panic");
}
