//! A checkpoint must record the cursor of this input incarnation, even idle.

use super::checkpoint_lifecycle::{append_ids, file_config, new_controller, weights};
use super::*;
use feldera_types::config::TransportConfig;

#[engine_test(requires(Storage, Checkpoints, FaultTolerance, Profiles))]
pub fn readded_input_gets_its_own_initial_cursor(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let temp = TempDir::new().unwrap();
    let config = file_config(temp.path(), true);
    append_ids(&temp.path().join("input.csv"), &[7, 23, 59]);
    let controller = new_controller(engine, &config);
    controller.start();
    wait(
        || weights(&controller) == [(7, 1), (23, 1), (59, 1)],
        20_000,
    )
    .unwrap();
    controller.pause();
    controller.checkpoint().unwrap();

    let replacement = temp.path().join("replacement.csv");
    File::create(&replacement).unwrap();
    // The first three rows occupy the same number of bytes as the old file,
    // but have different IDs. An inherited cursor silently skips those rows.
    append_ids(&replacement, &[5, 13, 43, 101, 211]);
    let mut input = config.inputs["source"].clone();
    input.connector_config.transport = TransportConfig::FileInput(
        serde_json::from_value(json!({ "path": replacement, "follow": true })).unwrap(),
    );
    let (entered, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    controller.start_graph_profile(Box::new(move |_| {
        entered.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(20)).unwrap();
    }));
    entry.recv_timeout(Duration::from_secs(20)).unwrap();
    let id = controller
        .inner
        .input_endpoint_id_by_name("source")
        .unwrap();
    controller.disconnect_input(&id);
    controller.connect_input("source", &input, None).unwrap();
    let (sent, received) = mpsc::channel();
    controller.start_checkpoint(Box::new(move |result| {
        let _ = sent.send(result);
    }));
    release.send(()).unwrap();
    let checkpoint = received
        .recv_timeout(Duration::from_secs(20))
        .unwrap()
        .unwrap();
    info!("replacement File cursor: {:?}", checkpoint.input_metadata);
    controller.stop().unwrap();

    let restored = new_controller(engine, &checkpoint.config);
    restored.start();
    wait(|| weights(&restored).contains(&(211, 1)), 20_000).unwrap();
    let actual = weights(&restored);
    restored.stop().unwrap();
    info!("recovered replacement File rows: {actual:?}");
    assert_eq!(
        actual,
        [
            (5, 1),
            (7, 1),
            (13, 1),
            (23, 1),
            (43, 1),
            (59, 1),
            (101, 1),
            (211, 1)
        ]
    );
    assert_eq!(checkpoint.input_metadata.0["source"]["offsets"]["end"], 0);
}

#[cfg(feature = "with-kafka")]
mod kafka {
    use super::*;
    use crate::test::kafka::{KafkaResources, TestProducer};
    use feldera_types::transport::kafka::default_redpanda_server;
    use rdkafka::{
        ClientConfig,
        consumer::{BaseConsumer, Consumer},
        producer::Producer,
    };

    fn idle_latest(engine: &dyn PipelineTestEngine, restart: bool, request_step: bool) {
        init_test_logger();
        let topic = format!("idle-latest-{}", Uuid::now_v7());
        let _topics = KafkaResources::create_topics(&[(&topic, 1)]);
        let producer = TestProducer::new();
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", default_redpanda_server())
            .set("group.id", format!("probe-{topic}"))
            .create()
            .unwrap();
        let publish = |ids: &[usize], end: i64| {
            let rows = ids
                .iter()
                .map(|id| format!("{id},false,,\n").into_bytes())
                .collect::<Vec<_>>();
            producer.send_to_topic(&rows, &topic);
            producer.producer.flush(Duration::from_secs(10)).unwrap();
            assert_eq!(
                probe
                    .fetch_watermarks(&topic, 0, Duration::from_secs(5))
                    .unwrap(),
                (0, end)
            );
        };
        publish(&[7, 29], 2);
        let temp = TempDir::new().unwrap();
        let mut config = file_config(temp.path(), true);
        config.inputs.clear();
        let controller = new_controller(engine, &config);
        let input = serde_json::from_value(json!({
            "stream": "test_input1", "transport": { "name": "kafka_input", "config": {
                "bootstrap.servers": default_redpanda_server(), "topic": topic, "start_from": "latest"
            }}, "format": { "name": "csv" }
        })).unwrap();
        // Paused: open pins the broker's tail but no data can be extended.
        controller.connect_input("hot", &input, None).unwrap();
        if request_step {
            let before = controller.status().global_metrics.total_completed_steps();
            controller.request_step();
            wait(
                || controller.status().global_metrics.total_completed_steps() > before,
                20_000,
            )
            .unwrap();
        }
        let (live, checkpoint) = if restart {
            let checkpoint = controller.checkpoint().unwrap();
            assert_eq!(checkpoint.processed_records, 0);
            info!(
                "idle Latest, requested step={request_step}, pinned tail=2, checkpoint={:?}",
                checkpoint.input_metadata
            );
            controller.stop().unwrap();
            (None, Some(checkpoint))
        } else {
            (Some(controller), None)
        };
        publish(&[103, 211], 4);
        let controller = match checkpoint {
            Some(checkpoint) => new_controller(engine, &checkpoint.config),
            None => live.unwrap(),
        };
        controller.start();
        publish(&[997], 5);
        // The marker is a positive completion witness in this one partition:
        // missing older rows cannot be explained by a slow fetch.
        wait(|| weights(&controller).contains(&(997, 1)), 20_000).unwrap();
        let actual = weights(&controller);
        let reader = controller.get_input_endpoint("hot").unwrap();
        controller.stop().unwrap();
        wait(|| reader.is_closed(), 20_000).unwrap();
        info!("idle Latest restart={restart}, requested step={request_step}, recovered={actual:?}");
        assert_eq!(actual, [(103, 1), (211, 1), (997, 1)]);
    }

    #[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
    pub fn idle_latest_checkpoint_preserves_open_time_cursor(engine: &dyn PipelineTestEngine) {
        idle_latest(engine, true, false);
    }

    #[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
    pub fn explicit_empty_step_preserves_latest_cursor(engine: &dyn PipelineTestEngine) {
        idle_latest(engine, true, true);
    }

    #[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
    pub fn uninterrupted_idle_latest_preserves_cursor(engine: &dyn PipelineTestEngine) {
        idle_latest(engine, false, false);
    }
}
