use super::NatsTestRecord;
use super::controller_framework::*;
use super::mock_framework::*;
use super::util;
use crate::test::mock_input_pipeline;
use anyhow::Result as AnyResult;
use feldera_types::program_schema::Relation;
use std::time::Duration;

#[test]
fn test_nats_basic_input_consumption() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(2),
            CreatePipeline,
            Extend,
            WaitForRecords(2),
            VerifyRecords {
                output_index: 0,
                count: 2,
            },
            AssertRecordCount(2),
            Disconnect,
        ],
    )
}
/// Replay all published records from a fresh pipeline and verify correctness.
#[test]
fn test_nats_replay_basic() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            CreatePipeline,
            Extend,
            WaitForRecords(5),
            VerifyRecords {
                output_index: 0,
                count: 5,
            },
            AssertRecordCount(5),
            Disconnect,
            // Replay all 5 records: sequences [1, 6).
            CreatePipeline,
            Replay { start: 1, end: 6 },
            WaitForReplayedRecords(5),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(5),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 1,
                count: 5,
            },
            Disconnect,
        ],
    )
}

/// Replay a subset of published records (partial range).
#[test]
fn test_nats_replay_partial_range() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(10),
            // First pipeline: consume all records.
            CreatePipeline,
            Extend,
            WaitForRecords(10),
            AssertRecordCount(10),
            Disconnect,
            // Second pipeline: replay sequences [3, 8), i.e. sequences 3, 4, 5, 6, 7.
            CreatePipeline,
            Replay { start: 3, end: 8 },
            WaitForReplayedRecords(5),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(5),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 3,
                count: 5,
            },
            Disconnect,
        ],
    )
}

/// Replay records then extend to consume new records published after the replay range.
#[test]
fn test_nats_replay_then_extend() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            CreatePipeline,
            Extend,
            WaitForRecords(5),
            AssertRecordCount(5),
            Disconnect,
            Publish(10),
            CreatePipeline,
            Replay { start: 1, end: 6 },
            WaitForReplayedRecords(5),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(5),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 1,
                count: 5,
            },
            // Now extend to consume the 5 new records (sequences [6, 11)).
            Extend,
            WaitForRecords(15),
            AssertRecordCount(15),
            VerifyOutputSlice {
                output_index: 5,
                nats_seq: 6,
                count: 10,
            },
            Disconnect,
        ],
    )
}

/// Replay with an empty range should succeed immediately without errors.
#[test]
fn test_nats_replay_empty_range() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(3),
            CreatePipeline,
            // Empty range: start == end (e.g., nothing to replay).
            Replay { start: 1, end: 1 },
            // Empty replay produces no records; give the command time to be processed.
            Sleep(Duration::from_millis(100)),
            // Verify the empty replay produced exactly zero records.
            AssertRecordCount(0),
            // Extend to consume records normally.
            Extend,
            WaitForRecords(3),
            VerifyRecords {
                output_index: 0,
                count: 3,
            },
            AssertRecordCount(3),
            Disconnect,
        ],
    )
}


/// Multiple sequential replays before extending.
#[test]
fn test_nats_replay_multiple_ranges() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(10),
            CreatePipeline,
            Extend,
            WaitForRecords(10),
            AssertRecordCount(10),
            Disconnect,
            CreatePipeline,
            Replay { start: 1, end: 4 },
            WaitForReplayedRecords(3),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(3),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 1,
                count: 3,
            },
            Replay { start: 4, end: 8 },
            WaitForReplayedRecords(7),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(7),
            VerifyOutputSlice {
                output_index: 3,
                nats_seq: 4,
                count: 4,
            },
            Extend,
            WaitForRecords(10),
            AssertRecordCount(10),
            VerifyOutputSlice {
                output_index: 7,
                nats_seq: 8,
                count: 3,
            },
            Disconnect,
        ],
    )
}

// ---------------------------------------------------------------------------
// Disconnect verification
//
// Verifies that after calling `Disconnect`, records published to the stream
// are NOT delivered to the pipeline.
// ---------------------------------------------------------------------------

#[test]
fn test_nats_disconnect_stops_delivery() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            // Publish and consume 3 records.
            Publish(3),
            CreatePipeline,
            Extend,
            WaitForRecords(3),
            VerifyRecords {
                output_index: 0,
                count: 3,
            },
            // Disconnect the endpoint.
            Disconnect,
            // Publish 5 more records (indices 3..8) while disconnected.
            Publish(5),
            // Give the endpoint time to (incorrectly) receive them.
            Sleep(Duration::from_millis(500)),
            // Assert that no new records arrived — still exactly 3.
            AssertRecordCount(3),
        ],
    )
}

#[test]
fn test_nats_ft_simple() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 5,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_with_checkpoints() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_without_checkpoints() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: false,
        },
    ]);
}

#[test]
fn test_nats_ft_alternating() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_initially_zero_without_checkpoint() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 0,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_initially_zero_with_checkpoint() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_empty_step_checkpoint() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 5,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
    ]);
}

/// Tests rapid restart+replay with a named consumer.
///
/// This reproduces a bug where the previous ordered consumer hadn't expired yet,
/// causing "consumer already exists" errors. The fix generates unique consumer names
/// by appending a UUID suffix when a name is explicitly configured.
#[test]
fn test_nats_ft_with_named_consumer() -> AnyResult<()> {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new()?.with_consumer_name("my_named_consumer"),
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
        ],
    )
}

/// Helper to assert that a connection error contains expected context.
fn assert_nats_connect_error(
    result: AnyResult<(
        Box<dyn crate::InputReader>,
        crate::test::MockInputConsumer,
        crate::test::MockInputParser,
        crate::test::MockDeZSet<NatsTestRecord, NatsTestRecord>,
    )>,
    expected_url: &str,
    expected_cause: &str,
) {
    match result {
        Ok(_) => panic!("Expected connection to fail"),
        Err(err) => {
            let err_msg = format!("{err:#}"); // Full error chain
            assert!(
                err_msg.contains(expected_url),
                "Error message should contain server URL, got: {err_msg}"
            );
            assert!(
                err_msg.contains("Failed to connect"),
                "Error message should indicate connection failure, got: {err_msg}"
            );
            assert!(
                err_msg.contains(expected_cause),
                "Error message should contain cause '{expected_cause}', got: {err_msg}"
            );
        }
    }
}

/// Test that connecting to a non-existent server (connection refused) produces
/// a clear error message with the server URL included.
#[test]
fn test_nats_connection_refused_error() {
    let nonexistent_url = "nats://127.0.0.1:59999";

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nonexistent_url}
        stream_name: my_stream
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    );

    assert_nats_connect_error(result, nonexistent_url, "Connection refused");
}

/// Test that connecting to a valid server but requesting a non-existent stream
/// produces a clear error message with the stream name.
#[test]
fn test_nats_stream_not_found_error() {
    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address().unwrap();

    // Wait for NATS to be ready
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        util::wait_for_nats_ready(&nats_url, Duration::from_secs(5))
            .await
            .unwrap();
    });

    let nonexistent_stream = "this_stream_does_not_exist";

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {nonexistent_stream}
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    );

    match result {
        Ok(_) => panic!("Expected stream lookup to fail"),
        Err(err) => {
            let err_msg = format!("{err:#}"); // Full error chain
            // The error message should contain the stream name for easy debugging
            assert!(
                err_msg.contains(nonexistent_stream),
                "Error message should contain stream name, got: {err_msg}"
            );
            assert!(
                err_msg.contains("Failed to get stream"),
                "Error message should indicate stream lookup failure, got: {err_msg}"
            );
        }
    }
}

/// Test that connection timeout option is respected.
#[test]
fn test_nats_connection_timeout() {
    // Use a non-routable IP address that will cause a connection timeout
    // 10.255.255.1 is a reserved address that should not respond
    let non_routable_url = "nats://10.255.255.1:4222";
    let timeout_secs = 1;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {non_routable_url}
            connection_timeout_secs: {timeout_secs}
        stream_name: some_stream
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let start = std::time::Instant::now();

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    );

    let elapsed = start.elapsed();

    // Should fail within a reasonable time relative to the timeout
    // Allow some slack for test execution overhead
    let max_expected = Duration::from_secs(timeout_secs + 3);
    assert!(
        elapsed < max_expected,
        "Connection should timeout within ~{timeout_secs}s, took {:?}",
        elapsed
    );

    assert_nats_connect_error(result, non_routable_url, "timed out");
}
