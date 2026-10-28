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
