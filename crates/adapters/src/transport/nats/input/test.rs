use crate::test::{
    DEFAULT_TIMEOUT_MS, TestStruct, init_test_logger, mock_input_pipeline, test_circuit, wait,
};
use crate::{Controller, PipelineConfig};
use anyhow::Result as AnyResult;
use async_nats::{self, jetstream};
use csv::ReaderBuilder as CsvReaderBuilder;
use feldera_macros::IsNone;
use feldera_sqllib::{SqlString, Timestamp, Variant};
use feldera_types::deserialize_table_record;
use feldera_types::deserialize_without_context;
use feldera_types::program_schema::Relation;
use serde::{Deserialize, Serialize};
use serde_json;
use size_of::SizeOf;
use std::{fs::create_dir, thread::sleep, time::Duration};
use tempfile::TempDir;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct NatsTestRecord {
    s: String,
    b: bool,
    i: i64,
}

impl NatsTestRecord {
    fn new(s: String, b: bool, i: i64) -> Self {
        Self { s, b, i }
    }
}

deserialize_without_context!(NatsTestRecord);

#[test]
fn test_nats_basic_input_consumption() -> AnyResult<()> {
    let stream_name = "str";
    let subject_name = "sub";

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address()?;

    let test_data = [
        NatsTestRecord::new("foo".to_string(), true, 10),
        NatsTestRecord::new("bar".to_string(), false, -10),
    ];

    // Create and populate NATS stream before initializing the input connector.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await?;

        for val in test_data.iter() {
            let ack = jetstream
                .publish(subject_name, serde_json::to_string(val)?.into())
                .await?;
            ack.await?;
        }

        Ok::<(), anyhow::Error>(())
    })?;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {stream_name}
        consumer_config:
            deliver_policy: All
            subjects: [{subject_name}]
format:
    name: json
    config:
        update_format: raw
"#
    );

    println!("Config:\n{}", config_str);

    let (endpoint, consumer, _parser, zset) =
        mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

    sleep(Duration::from_millis(10));

    // No outputs should be produced at this point.
    assert!(!consumer.state().eoi);

    // Unpause the endpoint, wait for the data to appear at the output.
    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == test_data.len()
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();
    for (i, upd) in zset.state().flushed.iter().enumerate() {
        assert_eq!(upd.unwrap_insert(), &test_data[i]);
    }

    endpoint.disconnect();

    Ok(())
}

#[derive(Clone)]
struct NatsFtTestRound {
    n_records: usize,
    do_checkpoint: bool,
}

impl NatsFtTestRound {
    fn with_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: true,
        }
    }

    fn without_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: false,
        }
    }
}

fn test_nats_ft(rounds: &[NatsFtTestRound]) {
    init_test_logger();

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address().unwrap();

    let stream_name = "str";
    let subject_name = "sub";

    // Setup NATS stream
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5))
            .await
            .unwrap();
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();
    });

    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let output_path = tempdir_path.join("output.csv");

    let config_str = format!(
        r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
fault_tolerance: {{}}
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: nats_input
            config:
                connection_config:
                    server_url: {nats_url}
                stream_name: {stream_name}
                consumer_config:
                    deliver_policy: All
                    subjects: [{subject_name}]
        format:
            name: json
            config:
                update_format: raw
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
"#
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let mut total_records = 0usize;
    let mut checkpointed_records = 0usize;

    for (
        round,
        NatsFtTestRound {
            n_records,
            do_checkpoint,
        },
    ) in rounds.iter().cloned().enumerate()
    {
        println!(
            "--- round {round}: add {n_records} records, {} ---",
            if do_checkpoint {
                "and checkpoint"
            } else {
                "no checkpoint"
            }
        );

        println!(
            "Writing records {total_records}..{}",
            total_records + n_records
        );
        if n_records > 0 {
            let nats_url = &nats_url;
            rt.block_on(async move {
                let client = util::wait_for_nats_ready(nats_url, Duration::from_secs(5))
                    .await
                    .unwrap();
                let jetstream = jetstream::new(client);

                for id in total_records..total_records + n_records {
                    let test_struct = TestStruct {
                        id: id as u32,
                        b: id % 2 == 0,
                        i: Some(id as i64),
                        s: format!("msg{}", id),
                    };
                    let json_data = serde_json::to_string(&test_struct).unwrap();
                    println!("Publishing: {}", json_data);
                    let ack = jetstream
                        .publish(subject_name, json_data.into())
                        .await
                        .unwrap();
                    let ack_result = ack.await.unwrap();
                    println!(
                        "Published message {} with sequence: {}",
                        id, ack_result.sequence
                    );
                }
                println!("Successfully published {} records to NATS", n_records);
            });
            total_records += n_records;
        }

        println!("start pipeline");
        let controller = Controller::with_test_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            &config,
            Box::new(|e, _tag| {
                println!("Controller error: {e}");
                panic!("Controller error: {e}");
            }),
        )
        .unwrap();

        controller.start();

        // Wait for the records that are not in the checkpoint to be
        // processed or replayed.
        println!(
            "wait for {} records {checkpointed_records}..{total_records}",
            total_records - checkpointed_records
        );
        let mut last_n = 0;
        let result = wait(
            || {
                let n = controller
                    .status()
                    .output_status()
                    .get(&0)
                    .unwrap()
                    .transmitted_records() as usize;

                if n > last_n {
                    println!("received {n} records of {total_records}");
                    last_n = n;
                }
                n >= total_records
            },
            10_000,
        );

        if let Err(()) = result {
            println!(
                "Controller status:\n{}",
                serde_json::to_string_pretty(controller.status()).unwrap()
            );
            panic!("Failed to receive expected records within timeout");
        }

        // No more records should arrive, but give the controller some time
        // to send some more in case there's a bug.
        sleep(Duration::from_millis(100));

        // Then verify that the number is as expected.
        assert_eq!(
            controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records(),
            total_records as u64
        );

        if do_checkpoint {
            println!("checkpoint");
            controller.checkpoint().unwrap();
        }

        println!("stop controller");
        controller.stop().unwrap();

        let mut actual = CsvReaderBuilder::new()
            .has_headers(false)
            .from_path(&output_path)
            .unwrap()
            .deserialize::<(TestStruct, i32)>()
            .map(|res| {
                let (val, weight) = res.unwrap();
                assert_eq!(weight, 1);
                val
            })
            .collect::<Vec<_>>();
        actual.sort_by_key(|item| item.id);

        assert_eq!(actual.len(), total_records - checkpointed_records);
        for (record, expect_record) in
            actual
                .into_iter()
                .zip((checkpointed_records..).map(|id| TestStruct {
                    id: id as u32,
                    b: id % 2 == 0,
                    i: Some(id as i64),
                    s: format!("msg{}", id),
                }))
        {
            assert_eq!(record, expect_record);
        }

        if do_checkpoint {
            checkpointed_records = total_records;
        }
        println!();
    }
}

#[test]
fn test_nats_ft_simple() {
    test_nats_ft(&[NatsFtTestRound::with_checkpoint(5)]);
}

#[test]
fn test_nats_ft_with_checkpoints() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(10),
        NatsFtTestRound::with_checkpoint(15),
        NatsFtTestRound::with_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_without_checkpoints() {
    test_nats_ft(&[
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::without_checkpoint(15),
        NatsFtTestRound::without_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_alternating() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(10),
        NatsFtTestRound::without_checkpoint(15),
        NatsFtTestRound::with_checkpoint(20),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::with_checkpoint(15),
    ]);
}

#[test]
fn test_nats_ft_initially_zero_without_checkpoint() {
    test_nats_ft(&[
        NatsFtTestRound::without_checkpoint(0),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::without_checkpoint(0),
        NatsFtTestRound::with_checkpoint(15),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::with_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_initially_zero_with_checkpoint() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::without_checkpoint(0),
        NatsFtTestRound::with_checkpoint(15),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::with_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_empty_step_checkpoint() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(5),
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::with_checkpoint(10),
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::with_checkpoint(10),
    ]);
}

/// Test struct with NATS metadata fields.
/// Used to test passing of record metadata from NATS connector to deserializer.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct NatsTestStructMetadata {
    pub i: i32,
    pub nats_subject: SqlString,
    pub nats_stream: SqlString,
    pub nats_stream_sequence: i64,
    pub nats_consumer_sequence: i64,
    pub nats_delivered: i64,
    pub nats_pending: i64,
    pub nats_published: Timestamp,
}

deserialize_table_record!(NatsTestStructMetadata["NatsTestStructMetadata", Variant, 8] {
    (i, "i", false, i32, |_| None),
    (nats_subject, "nats_subject", false, SqlString, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| SqlString::try_from(metadata.index_string("nats_subject")).ok())),
    (nats_stream, "nats_stream", false, SqlString, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| SqlString::try_from(metadata.index_string("nats_stream")).ok())),
    (nats_stream_sequence, "nats_stream_sequence", false, i64, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| i64::try_from(metadata.index_string("nats_stream_sequence")).ok())),
    (nats_consumer_sequence, "nats_consumer_sequence", false, i64, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| i64::try_from(metadata.index_string("nats_consumer_sequence")).ok())),
    (nats_delivered, "nats_delivered", false, i64, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| i64::try_from(metadata.index_string("nats_delivered")).ok())),
    (nats_pending, "nats_pending", false, i64, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| i64::try_from(metadata.index_string("nats_pending")).ok())),
    (nats_published, "nats_published", false, Timestamp, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().and_then(|metadata| Timestamp::try_from(metadata.index_string("nats_published")).ok()))
});

impl NatsTestStructMetadata {
    pub fn new(
        i: i32,
        nats_subject: SqlString,
        nats_stream: SqlString,
        nats_stream_sequence: i64,
        nats_consumer_sequence: i64,
        nats_delivered: i64,
        nats_pending: i64,
        nats_published: Timestamp,
    ) -> Self {
        Self {
            i,
            nats_subject,
            nats_stream,
            nats_stream_sequence,
            nats_consumer_sequence,
            nats_delivered,
            nats_pending,
            nats_published,
        }
    }
}

/// Test struct with NATS headers metadata.
/// Used to test headers metadata separately since headers require special handling.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct NatsTestStructWithHeaders {
    pub i: i32,
    pub nats_headers: Variant,
}

deserialize_table_record!(NatsTestStructWithHeaders["NatsTestStructWithHeaders", Variant, 2] {
    (i, "i", false, i32, |_| None),
    (nats_headers, "nats_headers", false, Variant, |__feldera_metadata: &Option<Variant>| __feldera_metadata.as_ref().map(|metadata| metadata.index_string("nats_headers")))
});

impl NatsTestStructWithHeaders {
    pub fn new(i: i32, nats_headers: Variant) -> Self {
        Self { i, nats_headers }
    }
}

#[test]
fn test_nats_metadata_json() -> AnyResult<()> {
    init_test_logger();

    let stream_name = "metadata_str";
    let subject_name = "metadata_sub";

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address()?;

    // Create NATS stream and publish test messages.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await?;

        // Publish two test messages
        for i in 0..2 {
            let ack = jetstream
                .publish(subject_name, format!("{{\"i\": {i}}}").into())
                .await?;
            ack.await?;
        }

        Ok::<(), anyhow::Error>(())
    })?;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {stream_name}
        consumer_config:
            deliver_policy: All
        include_subject: true
        include_stream: true
        include_stream_sequence: true
        include_consumer_sequence: true
        include_delivered: true
        include_pending: true
        include_published: true
format:
    name: json
    config:
        update_format: raw
"#
    );

    println!("Config:\n{}", config_str);

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<NatsTestStructMetadata, NatsTestStructMetadata>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

    sleep(Duration::from_millis(10));

    // Unpause the endpoint, wait for data.
    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == 2
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let received: Vec<_> = zset
        .state()
        .flushed
        .iter()
        .map(|upd| upd.unwrap_insert().clone())
        .collect();

    // Verify metadata fields
    assert_eq!(received.len(), 2);

    // First message
    assert_eq!(received[0].i, 0);
    assert_eq!(received[0].nats_subject, SqlString::from(subject_name));
    assert_eq!(received[0].nats_stream, SqlString::from(stream_name));
    assert_eq!(received[0].nats_stream_sequence, 1); // First message is sequence 1
    assert_eq!(received[0].nats_consumer_sequence, 1);
    assert_eq!(received[0].nats_delivered, 1); // First delivery attempt
    assert_eq!(received[0].nats_pending, 1); // One message pending after this one
    // nats_published should be non-zero (we don't check exact value since it's dynamic)
    assert!(received[0].nats_published.milliseconds() > 0);

    // Second message
    assert_eq!(received[1].i, 1);
    assert_eq!(received[1].nats_subject, SqlString::from(subject_name));
    assert_eq!(received[1].nats_stream, SqlString::from(stream_name));
    assert_eq!(received[1].nats_stream_sequence, 2); // Second message is sequence 2
    assert_eq!(received[1].nats_consumer_sequence, 2);
    assert_eq!(received[1].nats_delivered, 1);
    assert_eq!(received[1].nats_pending, 0); // No more messages pending
    assert!(received[1].nats_published.milliseconds() > 0);

    endpoint.disconnect();

    Ok(())
}

#[test]
fn test_nats_metadata_headers() -> AnyResult<()> {
    init_test_logger();

    let stream_name = "headers_str";
    let subject_name = "headers_sub";

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address()?;

    // Create NATS stream and publish test messages with headers.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await?;

        // Publish message with headers
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("test-header", "test-value");
        headers.insert("another-header", "another-value");

        let ack = jetstream
            .publish_with_headers(subject_name, headers, "{\"i\": 42}".into())
            .await?;
        ack.await?;

        // Publish message without headers
        let ack = jetstream
            .publish(subject_name, "{\"i\": 43}".into())
            .await?;
        ack.await?;

        Ok::<(), anyhow::Error>(())
    })?;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {stream_name}
        consumer_config:
            deliver_policy: All
        include_headers: true
format:
    name: json
    config:
        update_format: raw
"#
    );

    println!("Config:\n{}", config_str);

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<NatsTestStructWithHeaders, NatsTestStructWithHeaders>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

    sleep(Duration::from_millis(10));

    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == 2
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let received: Vec<_> = zset
        .state()
        .flushed
        .iter()
        .map(|upd| upd.unwrap_insert().clone())
        .collect();

    assert_eq!(received.len(), 2);

    // First message should have headers
    assert_eq!(received[0].i, 42);
    match &received[0].nats_headers {
        Variant::Map(headers) => {
            // Check that our headers are present (values are ByteArray wrapped in Variant::Binary)
            let test_header_key = Variant::String(SqlString::from("test-header"));
            let another_header_key = Variant::String(SqlString::from("another-header"));
            assert!(headers.contains_key(&test_header_key));
            assert!(headers.contains_key(&another_header_key));
        }
        _ => panic!("Expected nats_headers to be a Map variant"),
    }

    // Second message should have empty headers map
    assert_eq!(received[1].i, 43);
    match &received[1].nats_headers {
        Variant::Map(headers) => {
            assert!(headers.is_empty());
        }
        _ => panic!("Expected nats_headers to be a Map variant"),
    }

    endpoint.disconnect();

    Ok(())
}

#[test]
fn test_nats_metadata_not_requested() -> AnyResult<()> {
    // Test that when no metadata flags are set, the connector still works normally.
    let stream_name = "no_meta_str";
    let subject_name = "no_meta_sub";

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address()?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await?;

        let ack = jetstream
            .publish(
                subject_name,
                "{\"s\": \"test\", \"b\": true, \"i\": 100}".into(),
            )
            .await?;
        ack.await?;

        Ok::<(), anyhow::Error>(())
    })?;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {stream_name}
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

    sleep(Duration::from_millis(10));

    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == 1
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let state = zset.state();
    let received = state.flushed[0].unwrap_insert();
    assert_eq!(received.s, "test");
    assert!(received.b);
    assert_eq!(received.i, 100);

    endpoint.disconnect();

    Ok(())
}

mod util {
    use crate::test::wait;
    use anyhow::{Result as AnyResult, anyhow};
    use async_nats::Client;
    use serde::Deserialize;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    pub struct ProcessKillGuard {
        process: Child,
    }

    impl ProcessKillGuard {
        fn new(process: Child) -> Self {
            Self { process }
        }
    }

    impl Drop for ProcessKillGuard {
        fn drop(&mut self) {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }

    pub async fn wait_for_nats_ready(addr: &str, timeout: Duration) -> anyhow::Result<Client> {
        let deadline = Instant::now() + timeout;
        loop {
            match async_nats::connect(addr).await {
                Ok(client) => return Ok(client),
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(e) => return Err(anyhow::anyhow!("Timeout waiting for NATS: {e}")),
            }
        }
    }

    pub fn start_nats_and_get_address() -> AnyResult<(ProcessKillGuard, String)> {
        let nats_ip_addr = "127.0.0.1";
        const RANDOM_PORT: &str = "-1";

        let temp_dir = env::temp_dir();
        let port_file_dir = temp_dir.join("nats_ports");

        fs::create_dir_all(&port_file_dir)?;

        let child = Command::new("nats-server")
            .arg("-a")
            .arg(nats_ip_addr)
            .arg("-p")
            .arg(RANDOM_PORT)
            .arg("--ports_file_dir")
            .arg(port_file_dir.to_str().unwrap())
            .arg("--jetstream")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let pid = child.id();
        let port_file_path = port_file_dir.join(format!("nats-server_{}.ports", pid));

        let child = ProcessKillGuard::new(child);

        if wait(|| port_file_path.exists(), 10_000).is_err() {
            return Err(anyhow!("Port file was not created within timeout period"));
        }

        fn get_address_from_ports_file(file_path: &Path) -> AnyResult<String> {
            #[derive(Deserialize)]
            struct PortsData {
                nats: Vec<String>,
            }

            let port_content = fs::read_to_string(file_path)?;
            let ports_data: PortsData = serde_json::from_str(&port_content)
                .map_err(|_| anyhow!("Could not parse ports file"))?;

            ports_data
                .nats
                .into_iter()
                .next()
                .ok_or(anyhow!("No NATS addresses found in port file"))
        }

        let nats_addr = get_address_from_ports_file(&port_file_path)?;

        Ok((child, nats_addr))
    }
}
