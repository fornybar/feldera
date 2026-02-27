//! NATS JetStream input adapter with exactly-once fault tolerance.
//!
//! This adapter reads from a NATS JetStream using an **ordered pull consumer**,
//! which provides strict message ordering with automatic recreation on failures.
//! Combined with feldera message tracking, we achieve exactly-once semantics.
//!
//! # Ordered Pull Consumer
//!
//! We use `jetstream::consumer::pull::OrderedConfig` which provides:
//! - **Strict ordering**: Messages delivered in exact stream order
//! - **No acknowledgments**: Uses `AckPolicy::None` (tracked via sequences instead)
//! - **Automatic recreation**: On gap detection, heartbeat loss, or deletion
//! - **Ephemeral & single-replica**: Always in-memory, no durability overhead
//!
//! The ordered consumer automatically detects sequence gaps and recreates itself,
//! resuming from the last processed position. This complements our exactly-once
//! logic: we track sequences externally for checkpointing while the ordered
//! consumer ensures no gaps in the message stream.
//!
//! # Authentication
//!
//! Currently only credentials-based authentication (`.creds` files or inline strings)
//! is implemented. Additional authentication methods are defined in the configuration
//! schema but not yet implemented:
//! - TODO: JWT authentication
//! - TODO: NKey authentication
//! - TODO: Token authentication
//! - TODO: Username/password authentication
//!
//! See `config_utils::translate_connect_options` for implementation details.

mod config_utils;
#[cfg(test)]
mod test;

use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand},
};
use anyhow::{Context, Error as AnyError, Result as AnyResult, anyhow};
use async_nats::{
    self,
    jetstream::{self, consumer as nats_consumer},
};

use chrono::Utc;
use config_utils::{translate_connect_options, translate_consumer_options};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::nats::{self as cfg, NatsInputConfig},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cmp;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span};
use xxhash_rust::xxh3::Xxh3Default;

type NatsConsumerConfig = nats_consumer::pull::OrderedConfig;
type NatsConsumer = nats_consumer::Consumer<NatsConsumerConfig>;

/// Checkpoint/resume metadata
///
/// The sequence_numbers is a range `[start, end)` where:
/// - `start` = first message sequence in batch
/// - `end - 1` = last message in batch
/// - `end` = next message to consume (exclusive)
///
/// - `[0, 0)`: No messages processed and no checkpoint yet, start from beginning
/// - `[6, 6)`: (empty) All messages up to #6 processed, resume from #6
/// - `[6, 10)`: Batch contained messages #6-9, resume from #10
#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    sequence_numbers: std::ops::Range<u64>,
}

impl Metadata {
    fn from_resume_info(resume_info: Option<JsonValue>) -> Result<Self, AnyError> {
        // If None JsonValue create Metadata value 0..0, meaning "start from beginning"
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(Self {
                sequence_numbers: 0..0,
            }))
    }
}

pub struct NatsInputEndpoint {
    config: Arc<NatsInputConfig>,
}

impl NatsInputEndpoint {
    pub fn new(config: NatsInputConfig) -> Result<Self, AnyError> {
        if config.inactivity_timeout_secs == 0 {
            return Err(anyhow!(
                "Invalid NATS input configuration: inactivity_timeout_secs must be at least 1 second"
            ));
        }
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for NatsInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for NatsInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let resume_info = Metadata::from_resume_info(resume_info)?;
        info!("Resume info: {:?}", resume_info);

        Ok(Box::new(NatsReader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
            &schema.name.name(),
        )?))
    }
}

struct NatsReader {
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl NatsReader {
    fn new(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        table_name: &str,
    ) -> AnyResult<Self> {
        let span = info_span!(
            "nats_input",
            table = %table_name,
            server_url = %config.connection_config.server_url,
            stream_name = %config.stream_name,
            // Note: this is consumer_name from config, not the created name with unique suffix.
            consumer_name = config.consumer_config.name.as_deref().unwrap_or(""),
            consumer_description = config.consumer_config.description.as_deref().unwrap_or(""),
            filter_subjects = ?config.consumer_config.filter_subjects,
        );
        let (command_sender, command_receiver) = unbounded_channel();

        // Connect to NATS and verify stream exists (early validation).
        // This ensures we fail fast with a clear error if the server is
        // unreachable or the stream doesn't exist.
        // The async-nats connection_timeout only bounds the TCP handshake,
        // not the NATS protocol handshake (INFO/CONNECT/PONG). Wrap the
        // entire init in an outer timeout to prevent hanging if TCP connects
        // but the server process is unresponsive.
        let init_deadline = Duration::from_secs(
            config.connection_config.connection_timeout_secs
                + config.connection_config.request_timeout_secs,
        );
        let (nats_connection, jetstream) = TOKIO
            .block_on(
                async {
                    tokio::time::timeout(init_deadline, async {
                        let client = Self::connect_nats(&config.connection_config).await?;
                        let js = jetstream::new(client.clone());
                        Self::verify_stream_exists(&js, &config.stream_name).await?;
                        Ok::<_, AnyError>((client, js))
                    })
                    .await
                    .map_err(|_| {
                        anyhow!("NATS initialization timed out after {init_deadline:?}")
                    })?
                }
                .instrument(span.clone()),
            )
            .map_err(|e| {
                error!(
                    server_url = %config.connection_config.server_url,
                    stream_name = %config.stream_name,
                    connection_timeout_secs = config.connection_config.connection_timeout_secs,
                    request_timeout_secs = config.connection_config.request_timeout_secs,
                    "NATS initialization failed: {e:#}"
                );
                e.context(format!(
                    "NATS initialization failed for stream '{}' at server '{}' \
                (connection_timeout={}s, request_timeout={}s)",
                    config.stream_name,
                    config.connection_config.server_url,
                    config.connection_config.connection_timeout_secs,
                    config.connection_config.request_timeout_secs,
                ))
            })?;

        // The connection is established but we don't need the client reference
        // in the worker - it stays alive as long as the jetstream context exists.
        drop(nats_connection);

        let consumer_clone = consumer.clone();
        TOKIO.spawn(async move {
            Self::worker_task(
                config,
                resume_info,
                jetstream,
                consumer_clone,
                parser,
                command_receiver,
            )
            .instrument(span)
            .await
            .unwrap_or_else(|e| consumer.error(true, e, Some("nats-input")));
        });

        Ok(Self { command_sender })
    }

    async fn connect_nats(
        connection_config: &cfg::ConnectOptions,
    ) -> Result<async_nats::Client, AnyError> {
        let connect_options = translate_connect_options(connection_config).await?;

        let client = connect_options
            .connect(&connection_config.server_url)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to NATS server at {}",
                    connection_config.server_url
                )
            })?;

        Ok(client)
    }

    /// Verifies that the specified stream exists on the JetStream server.
    ///
    /// This provides early validation during initialization.
    /// If the stream doesn't exist, we fail fast with a clear error
    /// instead of timing out later during consumer creation.
    async fn verify_stream_exists(
        jetstream: &jetstream::Context,
        stream_name: &str,
    ) -> Result<(), AnyError> {
        let _ = fetch_stream_state(jetstream, stream_name).await?;
        Ok(())
    }

    async fn verify_server_and_stream_health(
        connection_config: &cfg::ConnectOptions,
        stream_name: &str,
    ) -> Result<(), AnyError> {
        // The async-nats connection_timeout only bounds the TCP handshake,
        // not the NATS protocol handshake (INFO/CONNECT/PONG). Wrap the
        // entire health check in an outer timeout to prevent hanging if
        // TCP connects but the server process is unresponsive.
        let deadline = Duration::from_secs(
            connection_config.connection_timeout_secs + connection_config.request_timeout_secs,
        );
        tokio::time::timeout(deadline, async {
            let client = Self::connect_nats(connection_config)
                .await
                .context("server health check failed")?;
            let js = jetstream::new(client);
            Self::verify_stream_exists(&js, stream_name)
                .await
                .with_context(|| format!("stream '{stream_name}' health check failed"))
        })
        .await
        .map_err(|_| anyhow!("health check timed out after {deadline:?}"))?
    }

    async fn worker_task(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        jetstream: jetstream::Context,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut canceller: Option<Canceller> = None;
        let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));
        let next_sequence = Arc::new(AtomicU64::new(resume_info.sequence_numbers.end));
        let nats_consumer_config = translate_consumer_options(&config.consumer_config);
        let inactivity_timeout = Duration::from_secs(config.inactivity_timeout_secs);

        validate_resume_position(
            &jetstream,
            &config.stream_name,
            next_sequence.load(Ordering::Acquire),
        )
        .await?;

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

        // Handle replay commands
        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Attempt to replay: {:?}", metadata);
            if !metadata.sequence_numbers.is_empty() {
                validate_replay_range(&jetstream, &config.stream_name, &metadata.sequence_numbers)
                    .await?;

                let first_message_sequence = metadata.sequence_numbers.start;

                let nats_consumer = create_nats_consumer(
                    &jetstream,
                    &nats_consumer_config,
                    &config.stream_name,
                    first_message_sequence,
                )
                .await?;

                // Since range is exclusive, last message to reply is (end-1).
                let last_message_sequence = metadata.sequence_numbers.end - 1;
                let (hasher, buffer_size) = consume_nats_messages_until(
                    nats_consumer,
                    last_message_sequence,
                    &config.connection_config,
                    &config.stream_name,
                    inactivity_timeout,
                    consumer.clone(),
                    parser.fork(),
                )
                .await
                .with_context(|| format!("While attempting to replay sequences {first_message_sequence}..{last_message_sequence}"))?;

                consumer.replayed(buffer_size, hasher.finish());

                next_sequence.store(last_message_sequence + 1, Ordering::Release);
            } else {
                consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
            }
        }

        loop {
            let command = command_receiver.recv().await?;
            match command {
                command @ InputReaderCommand::Replay { .. } => {
                    unreachable!("{command:?} must be at the beginning of the command stream")
                }
                InputReaderCommand::Queue { .. } => {
                    let (buffer_size, hasher, batches) = queue.flush_with_aux();
                    let sequence_number_range = match (batches.first(), batches.last()) {
                        (Some((_, first)), Some((_, last))) => *first..*last + 1,
                        _ => {
                            // If no batches were queued, create an empty range [pos, pos).
                            let pos = next_sequence.load(Ordering::Acquire);
                            pos..pos
                        }
                    };
                    info!(
                        "Queued {:?} records ({sequence_number_range:?})",
                        buffer_size
                    );
                    let metadata_json = serde_json::to_value(&Metadata {
                        sequence_numbers: sequence_number_range,
                    })?;
                    let timestamp = batches.last().map(|(ts, _)| *ts).unwrap_or_else(Utc::now);
                    let hash = hasher.map(|h| h.finish()).unwrap_or(0);
                    let resume = Resume::Replay {
                        hash,
                        seek: metadata_json.clone(),
                        replay: rmpv::Value::Nil,
                    };

                    consumer.extended(
                        buffer_size,
                        Some(resume),
                        vec![Watermark::new(timestamp, Some(metadata_json))],
                    );
                }
                InputReaderCommand::Pause => {
                    if let Some(canceller) = canceller.take() {
                        canceller.cancel_and_join().await;
                    }
                }
                InputReaderCommand::Extend => {
                    info!("Extend from {:?}", next_sequence.load(Ordering::Acquire));
                    if canceller.is_none() {
                        let nats_consumer = create_nats_consumer(
                            &jetstream,
                            &nats_consumer_config,
                            &config.stream_name,
                            next_sequence.load(Ordering::Acquire),
                        )
                        .await?;

                        canceller = Some(
                            spawn_nats_reader(
                                nats_consumer,
                                next_sequence.clone(),
                                queue.clone(),
                                config.connection_config.clone(),
                                config.stream_name.clone(),
                                inactivity_timeout,
                                consumer.clone(),
                                parser.fork(),
                            )
                            .await?,
                        );
                    }
                }
                InputReaderCommand::Disconnect => break,
            }
        }
        if let Some(canceller) = canceller.take() {
            canceller.cancel_and_join().await;
        }
        Ok(())
    }
}

async fn create_nats_consumer(
    jetstream: &jetstream::Context,
    consumer_config: &NatsConsumerConfig,
    stream_name: &str,
    message_start_sequence: u64,
) -> AnyResult<NatsConsumer> {
    let mut consumer_config = consumer_config.clone();

    // For 0, use the deliver policy configured by the user.
    // For >0, override with ByStartSequence to resume from a checkpoint position.
    if message_start_sequence > 0 {
        consumer_config.deliver_policy = jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: message_start_sequence,
        };
    }

    // Add a unique suffix to named consumers.
    // If consumer is unnamed, NATS automatically generates a random name.
    //
    // This fixes "consumer already exists" errors that occurred with rapid
    // pipeline restarts/replays before the previous consumer expires (inactive_threshold).
    consumer_config.name = consumer_config
        .name
        .map(|n| format!("{n}_{}", uuid::Uuid::now_v7()));

    jetstream
        .create_consumer_strict_on_stream(consumer_config.clone(), stream_name)
        .await
        .with_context(|| {
            format!(
                "Failed to create consumer on stream '{}' (start_sequence={}, deliver_policy={:?}, filter_subjects={:?})",
                stream_name,
                message_start_sequence,
                consumer_config.deliver_policy,
                consumer_config.filter_subjects,
            )
        })
}

async fn consume_nats_messages_until(
    nats_consumer: NatsConsumer,
    last_message_sequence: u64,
    connection_config: &cfg::ConnectOptions,
    stream_name: &str,
    inactivity_timeout: Duration,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
) -> AnyResult<(Xxh3Default, BufferSize)> {
    let mut nats_messages = nats_consumer.messages().await?;

    let mut hasher = Xxh3Default::new();
    let mut buffer_size = BufferSize::default();
    loop {
        let next_result = tokio::time::timeout(inactivity_timeout, nats_messages.next()).await;
        let Some(result) = (match next_result {
            Ok(result) => result,
            Err(_) => {
                match NatsReader::verify_server_and_stream_health(connection_config, stream_name)
                    .await
                {
                    Ok(()) => continue,
                    Err(error) => {
                        return Err(anyhow!(
                            "NATS replay stalled for {:?} and {error:#}",
                            inactivity_timeout,
                        ));
                    }
                }
            }
        }) else {
            return Err(anyhow!("Unexpected end of NATS stream"));
        };
        match result {
            Ok(message) => {
                let info = match message.info() {
                    Ok(info) => info,
                    Err(error) => {
                        consumer.error(
                            false,
                            anyhow!("Failed to get NATS message info: {error}"),
                            Some("nats-input"),
                        );
                        continue;
                    }
                };
                let data = &message.payload;
                let (buffer, errors) = parser.parse(data, None);
                consumer.parse_errors(errors);
                if let Some(mut buffer) = buffer {
                    buffer.hash(&mut hasher);
                    buffer.flush();
                }
                let amt = BufferSize {
                    records: 1,
                    bytes: data.len(),
                };
                consumer.buffered(amt);
                buffer_size += amt;
                info!("Got message #{}", info.stream_sequence);

                match info.stream_sequence.cmp(&last_message_sequence) {
                    cmp::Ordering::Less => (),     // Still more messages to consume
                    cmp::Ordering::Equal => break, // This was the final message we wanted
                    cmp::Ordering::Greater => {
                        return Err(anyhow!(
                            "Received unexpected message with offset {}; maybe the requested messages have been deleted?",
                            info.stream_sequence
                        ));
                    }
                }
            }
            Err(error) => consumer.error(false, anyhow!("NATS error: {error}"), Some("nats-input")),
        }
    }

    Ok((hasher, buffer_size))
}

/// Spawns a background task that continuously reads from an ordered consumer
/// and queues parsed messages.
///
/// Messages are tagged with their stream sequence number for checkpoint tracking.
/// The ordered consumer ensures no gaps occur; if one is detected, it automatically
/// recreates itself and resumes from the last known position.
async fn spawn_nats_reader(
    nats_consumer: NatsConsumer,
    next_sequence: Arc<AtomicU64>,
    queue: Arc<InputQueue<u64>>,
    connection_config: cfg::ConnectOptions,
    stream_name: String,
    inactivity_timeout: Duration,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
) -> AnyResult<Canceller> {
    let mut nats_messages = nats_consumer.messages().await?;

    let cancel_token = CancellationToken::new();
    let join_handle = tokio::spawn({
        let cancel_token_copy = cancel_token.clone();
        async move {
            loop {
                select! {
                    _ = cancel_token_copy.cancelled() => {
                        break;
                    }
                    result = tokio::time::timeout(inactivity_timeout, nats_messages.next()) => {
                        match result {
                            Ok(result) => {
                                let Some(result) = result else {
                                    consumer.error(true, anyhow!("Unexpected end of NATS stream"), Some("nats-input"));
                                    return;
                                };
                                match result {
                                    Ok(message) => {
                                        let info = match message.info() {
                                            Ok(info) => info,
                                            Err(error) => {
                                                consumer.error(false, anyhow!("Failed to get NATS message info: {error}"), Some("nats-input"));
                                                continue;
                                            }
                                        };
                                        info!("Got message #{}", info.stream_sequence);
                                        // Store the *next* sequence to process for resume tracking.
                                        // This is the checkpoint position if we need to restart.
                                        next_sequence.store(info.stream_sequence + 1, Ordering::Release);
                                        let data = &message.payload;
                                        queue.push_with_aux(parser.parse(data, None), Utc::now(), info.stream_sequence);
                                    }
                                    Err(error) => {
                                        consumer.error(false, anyhow!("NATS error: {error}"), Some("nats-input"));
                                    }
                                }
                            }
                            Err(_) => {
                                match NatsReader::verify_server_and_stream_health(&connection_config, &stream_name).await {
                                    Ok(()) => (),
                                    Err(error) => {
                                        consumer.error(
                                            true,
                                            anyhow!(
                                                "NATS input stalled for {:?} and {error:#}",
                                                inactivity_timeout,
                                            ),
                                            Some("nats-input"),
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(Canceller {
        cancel_token,
        join_handle,
    })
}

struct StreamState {
    messages: u64,
    first_sequence: u64,
    last_sequence: u64,
}

async fn fetch_stream_state(
    jetstream: &jetstream::Context,
    stream_name: &str,
) -> AnyResult<StreamState> {
    let mut stream = jetstream
        .get_stream(stream_name)
        .await
        .with_context(|| format!("Failed to get stream '{stream_name}'"))?;
    let stream_info = stream
        .info()
        .await
        .with_context(|| format!("Failed to fetch stream info for '{stream_name}'"))?;

    Ok(StreamState {
        messages: stream_info.state.messages,
        first_sequence: stream_info.state.first_sequence,
        last_sequence: stream_info.state.last_sequence,
    })
}

async fn validate_replay_range(
    jetstream: &jetstream::Context,
    stream_name: &str,
    requested_range: &std::ops::Range<u64>,
) -> AnyResult<()> {
    if requested_range.is_empty() {
        return Ok(());
    }

    let stream_state = fetch_stream_state(jetstream, stream_name).await?;

    if stream_state.messages == 0 {
        return Err(anyhow!(
            "Replay requested sequences {:?} from stream '{stream_name}', but the stream is empty",
            requested_range
        ));
    }

    let requested_first = requested_range.start;
    let requested_last = requested_range.end - 1;
    let available_first = stream_state.first_sequence;
    let available_last = stream_state.last_sequence;

    if requested_first < available_first || requested_first > available_last {
        return Err(anyhow!(
            "Replay start sequence {requested_first} is outside available stream range [{available_first}, {available_last}] for stream '{stream_name}'"
        ));
    }

    if requested_last > available_last {
        return Err(anyhow!(
            "Replay end sequence {requested_last} exceeds available stream tail {available_last} for stream '{stream_name}'"
        ));
    }

    Ok(())
}

async fn validate_resume_position(
    jetstream: &jetstream::Context,
    stream_name: &str,
    next_sequence: u64,
) -> AnyResult<()> {
    // Fresh starts use `0` and should always be allowed.
    if next_sequence == 0 {
        return Ok(());
    }

    let stream_state = fetch_stream_state(jetstream, stream_name).await?;

    if stream_state.messages == 0 {
        return Err(anyhow!(
            "Resume sequence {next_sequence} is invalid for stream '{stream_name}': stream is empty"
        ));
    }

    let available_first = stream_state.first_sequence;
    let available_last = stream_state.last_sequence;
    let valid_upper = available_last.saturating_add(1);

    if next_sequence < available_first {
        return Err(anyhow!(
            "Resume sequence {next_sequence} is before earliest available sequence {available_first} for stream '{stream_name}'"
        ));
    }

    if next_sequence > valid_upper {
        return Err(anyhow!(
            "Resume sequence {next_sequence} is after valid upper bound {valid_upper} for stream '{stream_name}'"
        ));
    }

    Ok(())
}

/// Used to instruct a task to shut down, and wait for it to end.
struct Canceller {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<()>,
}

impl Canceller {
    async fn cancel_and_join(self) {
        self.cancel_token.cancel();
        let _ = self.join_handle.await;
    }
}

impl InputReader for NatsReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}
