//! Tests of replay by stream sequence (Direct Get and the stream leader), of
//! the stream identity check, and of the positions the connector records.

use crate::{
    test::engine_test,
    transport::nats::input::test::{
        jetstream_proxy::ProxyFaults,
        mock_tests::{
            CONSUMER_COUNT_TIMEOUT, NatsMockAction, basic_nats_config, fast_retry_config,
            run_nats_mock_test,
        },
    },
};
use feldera_adapterlib::test::ConnectorTestEngine;
use serde_json::{Value as JsonValue, json};
use std::time::Duration;

const FALLBACKS: &str = "input_connector_nats_replay_leader_fallbacks_total";
const CONSUMERS_CREATED: &str = "input_connector_nats_consumers_created_total";

/// [fast_retry_config] with `consumer_config` fields replaced by those of
/// `consumer`.
fn config_with_consumer(nats_url: &str, consumer: JsonValue) -> JsonValue {
    let mut config = fast_retry_config(nats_url, json!({}));
    let target = &mut config["transport"]["config"]["consumer_config"];
    for (key, value) in consumer.as_object().unwrap() {
        target[key] = value.clone();
    }
    config
}

/// A replay reads the step with batched Direct Get, across several batches,
/// and creates no consumer.
#[engine_test(services(Nats))]
pub fn replay_reads_with_direct_get(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            // More than one batch of 512.
            Publish(1100),
            CreatePipeline,
            ReplayStep(json!({"sequence_numbers": {"start": 2, "end": 1101}, "messages": 1099})),
            WaitForReplayedRecords(1099),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 2,
                count: 1099,
            },
            AssertRecordCount(1099),
            AssertCustomMetric {
                name: FALLBACKS,
                value: 0.0,
            },
            AssertCustomMetric {
                name: CONSUMERS_CREATED,
                value: 0.0,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// A stream without `allow_direct` is replayed from the stream leader.
#[engine_test(services(Nats))]
pub fn replay_without_allow_direct_reads_from_leader(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStreamWith {
                subjects: &["sub"],
                allow_direct: false,
            },
            Publish(5),
            CreatePipeline,
            ReplayStep(json!({"sequence_numbers": {"start": 2, "end": 5}, "messages": 3})),
            WaitForReplayedRecords(3),
            VerifySequences(&[2, 3, 4]),
            AssertCustomMetric {
                name: FALLBACKS,
                value: 0.0,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// With several filter subjects, a replay returns exactly the step's
/// messages of those subjects, in stream order, and none of other subjects.
#[engine_test(services(Nats))]
pub fn replay_merges_filter_subjects_in_stream_order(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    for allow_direct in [true, false] {
        run_nats_mock_test(
            engine,
            |nats_url| config_with_consumer(nats_url, json!({"filter_subjects": ["sub", "sub2"]})),
            &[
                StartNats,
                CreateStreamWith {
                    subjects: &["sub", "sub2", "other"],
                    allow_direct,
                },
                PublishTo {
                    subject: "sub",
                    count: 2,
                },
                PublishTo {
                    subject: "other",
                    count: 1,
                },
                PublishTo {
                    subject: "sub2",
                    count: 2,
                },
                PublishTo {
                    subject: "sub",
                    count: 1,
                },
                PublishTo {
                    subject: "other",
                    count: 1,
                },
                CreatePipeline,
                ReplayStep(json!({"sequence_numbers": {"start": 1, "end": 7}, "messages": 5})),
                WaitForReplayedRecords(5),
                VerifySequences(&[1, 2, 4, 5, 6]),
                Disconnect,
            ],
        )
        .unwrap();
    }
}

/// A message deleted from the middle of a step fails the replay, and none of
/// the step reaches the circuit: feeding part of it and then failing (or
/// retrying) would ingest records twice or not at all.
#[engine_test(services(Nats))]
pub fn replay_of_step_missing_a_middle_message_is_fatal(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            DeleteMessage(3),
            CreatePipeline,
            ReplayStep(json!({"sequence_numbers": {"start": 1, "end": 6}, "messages": 5})),
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(10),
                needle: "ingested 5 messages, sequences 1 to 5, but the stream now has 4 messages",
            },
            AssertRecordCount(0),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// A step whose last message was deleted fails the replay at once. Replay
/// through a consumer used to wait forever for it.
#[engine_test(services(Nats))]
pub fn replay_of_step_missing_its_last_message_is_fatal(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    for allow_direct in [true, false] {
        run_nats_mock_test(
            engine,
            basic_nats_config,
            &[
                StartNats,
                CreateStreamWith {
                    subjects: &["sub"],
                    allow_direct,
                },
                Publish(5),
                DeleteMessage(4),
                CreatePipeline,
                // Metadata of earlier versions, without a message count.
                Replay { start: 2, end: 5 },
                ExpectFatalErrorContains {
                    timeout: Duration::from_secs(10),
                    needle: "but the stream now has 2 messages, sequences 2 to 3",
                },
                AssertRecordCount(0),
                DisconnectAllowNonFatal,
            ],
        )
        .unwrap();
    }
}

/// A Direct Get answer that lost a message is read again from the stream
/// leader, intact.
#[engine_test(services(Nats))]
pub fn replay_falls_back_to_leader_when_direct_get_loses_a_message(
    engine: &dyn ConnectorTestEngine,
) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(10),
            StartProxy(ProxyFaults {
                drop_direct_get_reply: Some(4),
                ..Default::default()
            }),
            CreatePipeline,
            ReplayStep(json!({"sequence_numbers": {"start": 1, "end": 11}, "messages": 10})),
            WaitForReplayedRecords(10),
            VerifyRecords {
                output_index: 0,
                count: 10,
            },
            AssertRecordCount(10),
            AssertCustomMetric {
                name: FALLBACKS,
                value: 1.0,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// A `LastPerSubject` step replays exactly the snapshot it read, not every
/// message of its range.
#[engine_test(services(Nats))]
pub fn last_per_subject_reads_and_replays_the_snapshot(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    let config = |nats_url: &str| {
        config_with_consumer(
            nats_url,
            json!({"deliver_policy": "LastPerSubject", "filter_subjects": ["sub.*"]}),
        )
    };
    let setup = [
        StartNats,
        CreateStreamWith {
            subjects: &["sub.*"],
            allow_direct: true,
        },
        // Sequences 1 (a), 2 (b), 3 (c), 4 (b): the snapshot is 1, 3, 4.
        PublishTo {
            subject: "sub.a",
            count: 1,
        },
        PublishTo {
            subject: "sub.b",
            count: 1,
        },
        PublishTo {
            subject: "sub.c",
            count: 1,
        },
        PublishTo {
            subject: "sub.b",
            count: 1,
        },
        CreatePipeline,
    ];

    let mut live = setup.to_vec();
    live.extend([
        Extend,
        WaitForRecords(3),
        // Newer messages follow the snapshot.
        PublishTo {
            subject: "sub.a",
            count: 1,
        },
        WaitForRecords(4),
        QueueFor(Duration::from_millis(500)),
        VerifySequences(&[1, 3, 4, 5]),
        Disconnect,
    ]);
    run_nats_mock_test(engine, config, &live).unwrap();

    let mut replay = setup.to_vec();
    replay.extend([
        ReplayStep(json!({
            "sequence_numbers": {"start": 1, "end": 5},
            "messages": 3,
            "sequences": [1, 3, 4],
        })),
        WaitForReplayedRecords(3),
        VerifySequences(&[1, 3, 4]),
        Disconnect,
    ]);
    run_nats_mock_test(engine, config, &replay).unwrap();
}

/// A stream deleted and recreated under the same name numbers its messages
/// from 1 again. The connector must notice, rather than read the new
/// stream's messages as if they were the old ones.
#[engine_test(services(Nats))]
pub fn recreated_stream_is_fatal(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({})),
        &[
            StartNats,
            CreateStream,
            Publish(2),
            CreatePipeline,
            Extend,
            WaitForRecords(2),
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            DeleteStream,
            CreateStream,
            // Enough messages that the position alone looks valid.
            Publish(5),
            Extend,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(10),
                needle: "is not the stream this pipeline read before",
            },
            AssertRecordCount(2),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();

    // The same, for a journaled step of the earlier stream.
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(3),
            CreatePipeline,
            ReplayStep(json!({
                "sequence_numbers": {"start": 1, "end": 4},
                "messages": 3,
                "stream_created": "1",
            })),
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(10),
                needle: "is not the stream this pipeline read before",
            },
            AssertRecordCount(0),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// `New` reads what is published after the connector first started, also
/// across a pause: earlier versions evaluated `New` again for every new
/// consumer and skipped what was published while paused.
#[engine_test(services(Nats))]
pub fn deliver_new_keeps_its_start_across_pause(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| config_with_consumer(nats_url, json!({"deliver_policy": "New"})),
        &[
            StartNats,
            CreateStream,
            Publish(2),
            CreatePipeline,
            Extend,
            WaitForConsumerCount {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            QueueFor(Duration::from_millis(500)),
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Publish(3),
            Extend,
            WaitForRecords(3),
            QueueFor(Duration::from_millis(500)),
            VerifySequences(&[3, 4, 5]),
            Disconnect,
        ],
    )
    .unwrap();
}

/// A message lost between the server and the connector is noticed from the
/// gap in delivery numbers, and read again from a new consumer.
#[engine_test(services(Nats))]
pub fn lost_delivery_is_read_again(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({})),
        &[
            StartNats,
            CreateStream,
            Publish(5),
            StartProxy(ProxyFaults {
                drop_delivery: Some(3),
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            WaitForRecordsNoFatal(5),
            QueueFor(Duration::from_millis(500)),
            AssertRecordCount(5),
            VerifyRecords {
                output_index: 0,
                count: 5,
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// A connector that reads one subject of a busy stream keeps its position
/// moving while it waits for its own messages, so that retention removing
/// other subjects' messages does not look like lost data.
#[engine_test(services(Nats))]
pub fn filtered_position_moves_past_other_subjects(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({})),
        &[
            StartNats,
            CreateStreamWith {
                subjects: &["sub", "other"],
                allow_direct: true,
            },
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            PublishTo {
                subject: "other",
                count: 5,
            },
            // Long enough for an inactivity check (1s).
            QueueFor(Duration::from_secs(3)),
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            // Messages 1 to 6 are gone; the connector had read up to 1.
            PurgeStream,
            Extend,
            Publish(1),
            WaitForRecords(2),
            AssertNoFatalError,
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay feeds the records of messages that hold several records, or none,
/// as reading them the first time did.
#[engine_test(services(Nats))]
pub fn replay_feeds_every_record_of_a_message(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    for replay in [false, true] {
        let mut actions = vec![
            StartNats,
            CreateStream,
            PublishRaw {
                subject: "sub",
                payloads: &[
                    r#"[{"s": "a", "b": true, "i": 1}, {"s": "b", "b": false, "i": 2}]"#,
                    "[]",
                    r#"[{"s": "c", "b": true, "i": 3}]"#,
                ],
            },
            CreatePipeline,
        ];
        if replay {
            actions.extend([
                ReplayStep(json!({"sequence_numbers": {"start": 1, "end": 4}, "messages": 3})),
                WaitForReplayedRecordsNoFatal(3),
            ]);
        } else {
            actions.extend([Extend, WaitForRecordsNoFatal(3)]);
        }
        actions.extend([
            QueueFor(Duration::from_millis(300)),
            AssertRecordCount(3),
            DisconnectAllowNonFatal,
        ]);
        run_nats_mock_test(
            engine,
            |nats_url| {
                let mut config = basic_nats_config(nats_url);
                config["format"]["config"]["array"] = json!(true);
                config
            },
            &actions,
        )
        .unwrap();
    }
}
