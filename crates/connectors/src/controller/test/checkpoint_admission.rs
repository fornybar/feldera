//! Eligibility must describe the input configuration actually checkpointed.

use super::checkpoint_lifecycle::{file_config, new_controller, weights};
use super::*;
use feldera_types::suspend::{PermanentSuspendError, SuspendError};

fn postgres_admission(engine: &dyn PipelineTestEngine, during_checkpoint: bool) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    // Storage without FT allows non-checkpointable connectors to be connected.
    // A checkpoint containing such a connector must still be rejected.
    let mut config = file_config(temp.path(), false);
    config.inputs.clear();
    let controller = new_controller(engine, &config);
    assert!(controller.can_checkpoint().is_ok());
    let input: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": { "name": "postgres_input", "config": {
            "uri": crate::test::Service::Postgres.address(),
            "query": "SELECT * FROM (VALUES (7::bigint, false, NULL::bigint, ''::text), (23, false, NULL, ''), (59, false, NULL, '')) AS t(id, b, i, s)"
        }}
    })).unwrap();
    if !during_checkpoint {
        controller.connect_input("postgres", &input, None).unwrap();
    }

    // Hold the public watch borrow while the controller marks the checkpoint
    // in progress. This exposes admission between the old eligibility check
    // and its separate input snapshot, without adding a production hook.
    let watcher = controller.checkpoint_watcher();
    let gate = during_checkpoint.then(|| watcher.borrow());
    let (sent, received) = mpsc::channel();
    controller.start_checkpoint(Box::new(move |result| {
        let _ = sent.send(result);
    }));
    if during_checkpoint {
        wait(|| controller.checkpoint_started().is_some(), 20_000).unwrap();
        controller.connect_input("postgres", &input, None).unwrap();
    }
    assert!(
        matches!(controller.can_checkpoint(), Err(SuspendError::Permanent(reasons))
        if reasons.contains(&PermanentSuspendError::UnsupportedInputEndpoint("postgres".into())))
    );
    drop(gate);
    let result = received.recv_timeout(Duration::from_secs(20)).unwrap();
    let valid = match result.as_ref().map_err(Arc::as_ref) {
        Ok(checkpoint) => {
            info!(
                "admission during checkpoint={during_checkpoint}, captured inputs={:?}",
                checkpoint.config.inputs.keys().collect::<Vec<_>>()
            );
            // Admission after an atomic snapshot belongs to a later checkpoint.
            during_checkpoint && !checkpoint.config.inputs.contains_key("postgres")
        }
        Err(ControllerError::SuspendError(SuspendError::Permanent(reasons))) => reasons.contains(
            &PermanentSuspendError::UnsupportedInputEndpoint("postgres".into()),
        ),
        Err(error) => panic!("unexpected checkpoint error: {error}"),
    };

    // Verify the actual PostgreSQL connector runs its query, not just a fake
    // endpoint claiming that it cannot checkpoint.
    let reader = controller.get_input_endpoint("postgres").unwrap();
    controller.start();
    wait(|| controller.pipeline_complete(), 20_000).unwrap();
    let actual = weights(&controller);
    controller.stop().unwrap();
    wait(|| reader.is_closed(), 20_000).unwrap();
    info!("PostgreSQL query rows: {actual:?}");
    assert_eq!(actual, [(7, 1), (23, 1), (59, 1)]);
    assert!(
        valid,
        "checkpoint included an input its eligibility check did not examine"
    );
}

#[engine_test(requires(Storage, Checkpoints), services(Postgres))]
pub fn checkpoint_checks_the_inputs_it_captures(engine: &dyn PipelineTestEngine) {
    postgres_admission(engine, true);
}

#[engine_test(requires(Storage, Checkpoints), services(Postgres))]
pub fn unsupported_input_before_checkpoint_is_rejected(engine: &dyn PipelineTestEngine) {
    postgres_admission(engine, false);
}
