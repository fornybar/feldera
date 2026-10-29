use super::super::{ConnectorError, StreamState, validate_resume_position};
use crate::transport::input_transport_config_to_endpoint;
use crate::transport::nats::input::{replay::validate_replay_range, test::util};
use feldera_types::config::InputEndpointConfig;
use feldera_types::secret_resolver::default_secrets_directory;
use serde_json::json;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration Validation
// ---------------------------------------------------------------------------

/// Builds a NATS input config with `key: value` inserted at the transport
/// config level, and runs it through configuration validation.
fn validate_config_with(key: &str, value: serde_json::Value) -> Result<(), String> {
    validate_config_at(&["transport", "config", key], value)
}

/// Like [validate_config_with], with `value` at any `path` of the endpoint
/// configuration.
fn validate_config_at(path: &[&str], value: serde_json::Value) -> Result<(), String> {
    let mut config = json!({
        "stream": "test_input",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://127.0.0.1:4222",
                },
                "stream_name": "some_stream",
                "consumer_config": {
                    "deliver_policy": "All",
                },
            },
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
            },
        },
    });
    let (last, parents) = path.split_last().unwrap();
    let mut target = &mut config;
    for key in parents {
        target = &mut target[*key];
    }
    target[*last] = value;

    let config: InputEndpointConfig = serde_json::from_value(config).unwrap();
    input_transport_config_to_endpoint(
        &config.connector_config.transport,
        "",
        default_secrets_directory(),
        None,
    )
    .map(|_| ())
    .map_err(|err| format!("{err:#}"))
}

/// An inactivity timeout below one second is rejected early by configuration
/// validation, whether written as `inactivity_timeout` or as the bare number
/// of seconds its `inactivity_timeout_secs` spelling took.
#[test]
fn test_nats_inactivity_timeout_below_one_second_rejected() {
    // `500ms` is the case a zero check would miss: the setting now holds a
    // `Duration`, so "below one second" is a wider set than "zero".
    for (key, value) in [
        ("inactivity_timeout", json!("0s")),
        ("inactivity_timeout", json!("500ms")),
        ("inactivity_timeout_secs", json!(0)),
    ] {
        let err_msg = validate_config_with(key, value.clone())
            .expect_err(&format!("Expected `{key}: {value}` to be rejected"));
        // Both spellings reach one setting, so the error names that setting
        // rather than the key the user happened to type.
        assert!(
            err_msg.contains("`inactivity_timeout`"),
            "Error message should name `inactivity_timeout`, got: {err_msg}"
        );
    }
}

/// A retry interval below one second is rejected early by configuration
/// validation, whether written as `retry_interval` or as the bare number of
/// seconds its `retry_interval_secs` spelling took.
#[test]
fn test_nats_retry_interval_below_one_second_rejected() {
    // `500ms` is the case a zero check would miss: the setting now holds a
    // `Duration`, so "below one second" is a wider set than "zero".
    for (key, value) in [
        ("retry_interval", json!("0s")),
        ("retry_interval", json!("500ms")),
        ("retry_interval_secs", json!(0)),
    ] {
        let err_msg = validate_config_with(key, value.clone())
            .expect_err(&format!("Expected `{key}: {value}` to be rejected"));
        // Both spellings reach one setting, so the error names that setting
        // rather than the key the user happened to type.
        assert!(
            err_msg.contains("`retry_interval`"),
            "Error message should name `retry_interval`, got: {err_msg}"
        );
    }
}

/// Configuration mistakes that no server can fix fail when the connector
/// opens, instead of turning into retries without end.
#[test]
fn test_nats_permanent_config_errors_rejected_at_open() {
    for (path, value, needle) in [
        (
            &["transport", "config", "consumer_config", "rate_limit"][..],
            json!(1000),
            "`rate_limit` is 1000",
        ),
        (
            &["transport", "config", "stream_name"][..],
            json!("orders.eu"),
            "contains '.'",
        ),
        (
            &["transport", "config", "stream_name"][..],
            json!(""),
            "`stream_name` is empty",
        ),
        (
            &["transport", "config", "consumer_config", "filter_subjects"][..],
            json!(["orders.>.eu"]),
            "uses `>` before the last token",
        ),
        (
            &["transport", "config", "consumer_config", "filter_subjects"][..],
            json!(["orders..eu"]),
            "has an empty token",
        ),
        (
            &["transport", "config", "consumer_config", "filter_subjects"][..],
            json!(["orders.e*"]),
            "uses a wildcard inside a token",
        ),
        (
            &["transport", "config", "consumer_config", "deliver_policy"][..],
            json!("LastPerSubject"),
            "`LastPerSubject` needs `filter_subjects`",
        ),
        (
            &["transport", "config", "connection_config", "auth"][..],
            json!({"token": "t", "user_and_password": {"user": "u", "password": "p"}}),
            "multiple NATS authentication methods",
        ),
        (
            &["transport", "config", "connection_config", "auth"][..],
            json!({"jwt": "eyJ0eXAiOiJKV1QifQ"}),
            "requires `nkey`",
        ),
        (
            &["transport", "config", "connection_config", "auth"][..],
            json!({"nkey": "SUNOTASEED"}),
            "invalid NATS nkey seed",
        ),
    ] {
        let error = validate_config_at(path, value.clone())
            .expect_err(&format!("{path:?} = {value} must be rejected"));
        assert!(error.contains(needle), "{path:?} = {value}: {error}");
    }

    // Valid subjects and names pass.
    validate_config_at(
        &["transport", "config", "consumer_config", "filter_subjects"],
        json!(["orders.*.eu", "orders.us.>", "x"]),
    )
    .unwrap();
    validate_config_at(
        &["transport", "config", "stream_name"],
        json!("ORDERS-v2_x"),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Validation of stream positions
// ---------------------------------------------------------------------------

fn stream(messages: u64, first_sequence: u64, last_sequence: u64) -> StreamState {
    StreamState {
        messages,
        first_sequence,
        last_sequence,
    }
}

fn is_fatal(result: Result<(), ConnectorError>) -> bool {
    matches!(result, Err(ConnectorError::Fatal(_)))
}

/// A fresh start (resume cursor 0) needs no validation.
#[test]
fn test_nats_validate_resume_fresh_start_is_ok() {
    assert!(validate_resume_position(&stream(0, 0, 0), "s", false, 0).is_ok());
    assert!(validate_resume_position(&stream(3, 5, 7), "s", false, 0).is_ok());
}

#[test]
fn test_nats_validate_resume_within_and_at_tail_is_ok() {
    // Messages 1..=3: resume at 1, 3, or the next message 4.
    for cursor in [1, 3, 4] {
        assert!(validate_resume_position(&stream(3, 1, 3), "s", false, cursor).is_ok());
    }
    // Interior deletes leave first and last in place.
    assert!(validate_resume_position(&stream(2, 1, 3), "s", false, 2).is_ok());
}

/// A stream whose already-read messages all expired, or were purged, is
/// empty but continues at `last + 1`. Resuming there loses nothing.
#[test]
fn test_nats_validate_resume_after_consumed_history_removed_is_ok() {
    // Purged after 5 messages: first = 6, last = 5.
    assert!(validate_resume_position(&stream(0, 6, 5), "s", false, 6).is_ok());
    // A new stream that has never had a message.
    assert!(validate_resume_position(&stream(0, 0, 0), "s", false, 1).is_ok());
}

#[test]
fn test_nats_validate_resume_before_head_is_fatal() {
    // Messages 1..=5 purged, 6..=8 published: 2..=5 were never read.
    assert!(is_fatal(validate_resume_position(
        &stream(3, 6, 8),
        "s",
        false,
        2
    )));
    // Messages 3..=5 purged before the connector read them.
    assert!(is_fatal(validate_resume_position(
        &stream(0, 6, 5),
        "s",
        false,
        3
    )));
}

#[test]
fn test_nats_validate_resume_with_gap_from_tail_is_fatal() {
    // Messages 1..=3, resume from 5: the stream lost messages already read.
    assert!(is_fatal(validate_resume_position(
        &stream(3, 1, 3),
        "s",
        false,
        5
    )));
    // The same against an empty, recreated stream.
    assert!(is_fatal(validate_resume_position(
        &stream(0, 0, 0),
        "s",
        false,
        6
    )));
}

#[test]
fn test_nats_validate_replay_range_exceeds_tail_is_fatal() {
    assert!(is_fatal(validate_replay_range(
        &stream(3, 1, 3),
        "s",
        &(1..100)
    )));
}

#[test]
fn test_nats_validate_replay_range_precedes_head_is_fatal() {
    // Messages 4..=7: 1..=3 are gone.
    assert!(is_fatal(validate_replay_range(
        &stream(4, 4, 7),
        "s",
        &(1..8)
    )));
}

#[test]
fn test_nats_validate_replay_empty_stream_is_fatal() {
    assert!(is_fatal(validate_replay_range(
        &stream(0, 6, 5),
        "s",
        &(1..6)
    )));
}

#[test]
fn test_nats_validate_replay_empty_range_is_ok() {
    assert!(validate_replay_range(&stream(0, 0, 0), "s", &(5..5)).is_ok());
}

#[test]
fn test_nats_validate_replay_range_is_ok() {
    assert!(validate_replay_range(&stream(6, 1, 6), "s", &(3..7)).is_ok());
    assert!(validate_replay_range(&stream(6, 1, 6), "s", &(1..2)).is_ok());
}

/// Failing to reach the server is worth retrying; it says nothing about the
/// stream.
#[test]
fn test_nats_stream_info_server_down_is_retryable() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (guard, url) = util::start_nats_and_get_address().unwrap();
    let js = rt.block_on(async {
        util::create_stream(&url, "s", "sub").await.unwrap();
        let client = util::wait_for_nats_ready(&url, Duration::from_secs(5))
            .await
            .unwrap();
        let mut js = async_nats::jetstream::new(client);
        js.set_timeout(Duration::from_secs(1));
        js
    });
    assert!(
        rt.block_on(super::super::fetch_stream_info(&js, "s"))
            .is_ok()
    );
    drop(guard);
    std::thread::sleep(Duration::from_millis(200));
    assert!(matches!(
        rt.block_on(super::super::fetch_stream_info(&js, "s")),
        Err(ConnectorError::Retryable(_))
    ));
}
