//! NATS JetStream input adapter with exactly-once fault tolerance.
//!
//! # Reading
//!
//! The connector reads with an ephemeral, single-replica, in-memory pull
//! consumer without acknowledgments, and tracks its position in the stream
//! itself: [ReaderState::next_sequence] is the stream sequence of the next
//! message it wants. Every message carries its consumer sequence, which grows
//! by exactly one per delivered message, so the reader notices any message
//! that the server sent but the client never received. On such a gap, on a
//! deleted consumer, and on any other read error, the reader stops and the
//! worker recreates the consumer at the tracked position, with the retry
//! policy of [retry::RetryPolicy].
//!
//! Earlier versions read with the client library's *ordered* consumer, which
//! recreates itself on the same conditions. That hid the recreation from the
//! connector: the library retries without limit and without the connector's
//! backoff or metrics, names the replacement consumers differently (so the
//! connector could not delete them), and recreates a consumer that has not
//! delivered anything yet from the *start of the stream* rather than from the
//! position the connector asked for.
//!
//! # Positions and checkpoints
//!
//! Each step records the range of stream sequences it fed to the circuit (see
//! [Metadata]). A step with no messages records the position where the next
//! one starts, taken from what was flushed, not from what the reader has
//! read: the reader can be ahead of the circuit, and a checkpoint at that
//! position would skip the messages still queued.
//!
//! The first consumer of a fresh start uses the configured deliver policy. The
//! connector then pins the start position that the server chose, so that a
//! later consumer (after a pause, an error, or a restart from a checkpoint
//! taken before any message arrived) continues from there instead of
//! evaluating `New` or `Last` again and skipping what was published in
//! between.
//!
//! `LastPerSubject` delivers a snapshot (the last message of each subject)
//! and then every newer message. The snapshot is not a contiguous range of
//! the stream, so steps that contain snapshot messages list their sequences
//! explicitly, and a consumer that has to be recreated before the snapshot is
//! complete takes a new snapshot rather than reading every message from its
//! position on.
//!
//! Each step also records which stream its sequences refer to: the stream's
//! creation time, and the sequence and storage time of the last message the
//! pipeline ingested. A stream that was deleted and recreated under the same
//! name numbers its messages from 1 again, and a stream restored from a backup
//! older than what the pipeline ingested reuses the sequences of the lost
//! messages for new ones, so the recorded sequences would silently point at
//! different messages. See [verify_stream_identity].
//!
//! # Replay
//!
//! Journaled steps are read back by sequence, without a consumer; see
//! [replay].

pub(crate) mod config_utils;
mod metrics;
mod replay;
mod retry;
#[cfg(any(test, feature = "testing"))]
pub mod test;

use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand},
};
use anyhow::{Error as AnyError, Result as AnyResult, anyhow};
use async_nats::jetstream::{
    self,
    consumer::{self as nats_consumer, pull::MessagesErrorKind},
    context::GetStreamErrorKind,
    stream::{
        self as nats_stream, ConsumerCreateStrictErrorKind, LastRawMessageErrorKind,
        RetentionPolicy,
    },
};
use chrono::Utc;
use config_utils::{
    is_permanent_api_error, redact_server_url, translate_connect_options,
    translate_consumer_options, validate_input_config,
};
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::metrics::ConnectorMetrics;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_storage::tokio::TOKIO;
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::nats::{self as cfg, NatsInputConfig},
};
use futures::StreamExt;
use metrics::{NatsInputMetrics, RetryState};
use replay::{ReplayMessage, ReplaySource};
use retry::RetryPolicy;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, trace, warn};
use xxhash_rust::xxh3::Xxh3Default;

type PullConsumer = nats_consumer::Consumer<nats_consumer::pull::Config>;

/// Connection and identification context for a NATS stream.
#[derive(Clone)]
struct StreamContext {
    connection_config: cfg::ConnectOptions,
    /// The server URL without credentials, for messages.
    server: String,
    stream_name: String,
    filter_subjects: Vec<String>,
    inactivity_timeout: Duration,
    request_timeout: Duration,
    /// Prefix for the names of consumers this connector creates.
    /// See [`consumer_name_prefix`].
    consumer_name_prefix: String,
    metrics: Arc<NatsInputMetrics>,
    /// Whether the connector has checked, since it opened, that replay can
    /// use batched Direct Get; see [replay::warn_once_without_direct_get].
    direct_get_checked: Arc<AtomicBool>,
}

/// Upper bound on how long teardown waits for the server to acknowledge a
/// consumer delete before giving up and leaving it to `inactive_threshold`.
const CONSUMER_DELETE_TIMEOUT: Duration = Duration::from_secs(5);

enum ConnectorError {
    Retryable(AnyError),
    Fatal(AnyError),
}

impl std::fmt::Debug for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(e) => write!(f, "Retryable({e:#})"),
            Self::Fatal(e) => write!(f, "Fatal({e:#})"),
        }
    }
}

impl ConnectorError {
    fn with_context(self, context: impl std::fmt::Display + Send + Sync + 'static) -> Self {
        match self {
            Self::Retryable(error) => Self::Retryable(error.context(context)),
            Self::Fatal(error) => Self::Fatal(error.context(context)),
        }
    }

    fn inner(&self) -> &AnyError {
        match self {
            Self::Retryable(error) | Self::Fatal(error) => error,
        }
    }
}

/// Checkpoint, resume, and replay metadata of one step.
///
/// `sequence_numbers` is a range `[start, end)` of stream sequences:
/// - `start` = sequence of the first message in the step
/// - `end - 1` = sequence of the last message in the step
/// - `end` = where reading continues after the step
///
/// - `[0, 0)`: nothing read yet; start with the configured deliver policy
/// - `[6, 6)`: (empty) everything before #6 is done, resume from #6
/// - `[6, 10)`: the step read messages #6 through #9, resume from #10
///
/// The other fields are absent in metadata written by earlier versions, which
/// therefore still reads back.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Metadata {
    sequence_numbers: Range<u64>,

    /// Number of messages in the step, so that a replay can tell whether
    /// messages in the middle of the range have disappeared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    messages: Option<u64>,

    /// The sequences of the step's messages, when they are not simply every
    /// message in `sequence_numbers` that matches the filter subjects: a
    /// `LastPerSubject` snapshot skips older messages of each subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequences: Option<Vec<u64>>,

    /// Creation time of the stream, in nanoseconds since the Unix epoch,
    /// identifying the stream that the sequence numbers refer to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_created: Option<String>,

    /// A `LastPerSubject` snapshot was not completely read at the end of the
    /// step, so a resumed connector must take a snapshot again.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    last_per_subject_pending: bool,

    /// The last message the pipeline ingested up to the end of the step: the
    /// step's last message, or for a step without messages, the one before.
    /// See [verify_stream_identity].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_ingested: Option<Anchor>,
}

/// A message the connector ingested, identified by its stream sequence and
/// by the time the server stored it, in nanoseconds since the Unix epoch.
///
/// Another stream, or the same stream restored from an older backup and then
/// written to again, has another message at the same sequence, stored at
/// another time. A restore of the same data keeps the message's time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Anchor {
    sequence: u64,
    timestamp: String,
}

impl Anchor {
    fn new(sequence: u64, timestamp: i128) -> Self {
        Self {
            sequence,
            timestamp: timestamp.to_string(),
        }
    }
}

impl Metadata {
    fn from_resume_info(resume_info: Option<JsonValue>) -> Result<Self, AnyError> {
        // `None` means "start from the beginning", as `[0, 0)` does.
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default())
    }
}

/// The auxiliary data queued with each entry of the [InputQueue].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Queued {
    /// The parsed contents of the message with this stream sequence.
    Message {
        sequence: u64,
        /// When the server stored the message; see [Anchor].
        timestamp: i128,
        /// Part of a `LastPerSubject` snapshot.
        snapshot: bool,
    },
    /// The messages before this entry complete a `LastPerSubject` snapshot.
    SnapshotComplete,
    /// Every message before this stream sequence that the connector reads
    /// has been queued before this entry, so a step that ends here can
    /// record this as its end position even if it has no messages. See
    /// [ReaderState::next_sequence].
    Advance(u64),
}

/// State shared by the worker and its reader tasks.
#[derive(Clone)]
struct ReaderState {
    /// Stream sequence of the next message to read.
    next_sequence: Arc<AtomicU64>,
    queue: Arc<InputQueue<Queued>>,
    /// Set when a reader passes a health check, so that its later failure
    /// counts as a new outage.
    healthy: Arc<AtomicBool>,
    /// A reader has read the end of the `LastPerSubject` snapshot, so the next
    /// consumer must continue from [Self::next_sequence] rather than take a
    /// new snapshot, even if the circuit has not received the end yet.
    snapshot_read: Arc<AtomicBool>,
    /// See [Metadata::stream_created]. Set before a reader queues anything,
    /// so that every step with messages records it.
    stream_created: Arc<Mutex<Option<String>>>,
    /// The last message a reader queued, which the next reader's stream must
    /// still have; see [verify_stream_identity].
    last_read: Arc<Mutex<Option<Anchor>>>,
}

/// The worker's view of what reached the circuit.
struct Position {
    /// Where the step after the last flushed one starts.
    flushed_end: u64,
    /// See [Metadata::stream_created].
    stream_created: Option<String>,
    /// See [Metadata::last_ingested].
    last_ingested: Option<Anchor>,
    /// See [Metadata::last_per_subject_pending].
    snapshot_pending: bool,
    /// The configured deliver policy is `LastPerSubject`.
    last_per_subject: bool,
}

impl Position {
    fn new(config: &NatsInputConfig) -> Self {
        let mut position = Self {
            flushed_end: 0,
            stream_created: None,
            last_ingested: None,
            snapshot_pending: false,
            last_per_subject: matches!(
                config.consumer_config.deliver_policy,
                cfg::DeliverPolicy::LastPerSubject
            ),
        };
        position.apply(&Metadata::default());
        position
    }

    /// Takes on the position at the end of the step that `metadata`
    /// describes.
    fn apply(&mut self, metadata: &Metadata) {
        self.flushed_end = metadata.sequence_numbers.end;
        // Nothing read yet means the snapshot is still ahead.
        self.snapshot_pending = metadata.last_per_subject_pending
            || (self.last_per_subject && metadata.sequence_numbers.end == 0);
        if let Some(created) = &metadata.stream_created {
            self.stream_created = Some(created.clone());
        }
        if let Some(anchor) = &metadata.last_ingested {
            self.last_ingested = Some(anchor.clone());
        }
    }
}

/// What the worker is doing.
enum Phase {
    /// Not reading, until the next `Extend`.
    Paused,
    /// Starting a reader.
    Starting(Starting),
    Running(Box<Reader>),
    /// Waiting to try again to start a reader.
    Retrying {
        at: Instant,
    },
    /// Stopped for good: disconnected, or failed fatally.
    Stopped,
}

impl Phase {
    /// Stops whatever reader is running or starting, and returns the work
    /// that deletes its consumer, which the worker must join before it
    /// exits.
    async fn stop(self) -> Option<JoinHandle<()>> {
        match self {
            Phase::Running(reader) => {
                let consumer = reader.stop().await;
                Some(tokio::spawn(async move { consumer.delete().await }))
            }
            Phase::Starting(starting) => {
                // The start cannot be abandoned midway: a consumer create
                // that was sent may still succeed. It notices the
                // cancellation at its next step and cleans up after itself.
                starting.cancel.cancel();
                Some(tokio::spawn(async move {
                    if let Ok(Ok(reader)) = starting.task.await {
                        reader.stop().await.delete().await;
                    }
                }))
            }
            Phase::Paused | Phase::Retrying { .. } | Phase::Stopped => None,
        }
    }
}

pub struct NatsInputEndpoint {
    config: Arc<NatsInputConfig>,
}

impl NatsInputEndpoint {
    pub fn new(config: NatsInputConfig) -> Result<Self, AnyError> {
        if config.inactivity_timeout() < Duration::from_secs(1) {
            return Err(anyhow!(
                "Invalid NATS input configuration: `inactivity_timeout` is {}, but it must be at least 1 second; raise it, for example to `30s`",
                config.inactivity_timeout()
            ));
        }
        if config.retry_interval() < Duration::from_secs(1) {
            return Err(anyhow!(
                "Invalid NATS input configuration: `retry_interval` is {}, but it must be at least 1 second; raise it, for example to `1s`",
                config.retry_interval()
            ));
        }
        if config.retry_max_interval() < config.retry_interval() {
            return Err(anyhow!(
                "Invalid NATS input configuration: `retry_max_interval` is {}, but it must be at least `retry_interval` ({}); raise `retry_max_interval` or lower `retry_interval`",
                config.retry_max_interval(),
                config.retry_interval()
            ));
        }
        if config.retry_max_attempts == Some(0) {
            return Err(anyhow!(
                "Invalid NATS input configuration: retry_max_attempts must be at least 1 when set"
            ));
        }
        validate_input_config(&config)?;
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

        let metrics = Arc::new(NatsInputMetrics::default());
        consumer.set_custom_metrics(Arc::clone(&metrics) as Arc<dyn ConnectorMetrics>);

        Ok(Box::new(NatsReader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
            &schema.name.name(),
            metrics,
        )?))
    }
}

struct NatsReader {
    command_sender: UnboundedSender<InputReaderCommand>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// What the worker reacts to next.
enum Event {
    Command(InputReaderCommand),
    /// The reader task ended, by itself: it only ends without an error when
    /// cancelled.
    ReaderExited(Result<Result<(), ConnectorError>, JoinError>),
    /// The start of a reader finished.
    Started(Result<Result<Box<Reader>, ConnectorError>, JoinError>),
    /// Time for the next attempt to start a reader.
    RetryDue,
}

/// Consecutive failures to read.
struct Retry {
    policy: RetryPolicy,
    /// Consecutive failed attempts, including the first failure.
    failures: u32,
}

impl Retry {
    /// Records a failure and returns the phase the worker continues in.
    fn fail(
        &mut self,
        error: ConnectorError,
        consumer: &dyn InputConsumer,
        metrics: &NatsInputMetrics,
    ) -> Phase {
        match error {
            ConnectorError::Fatal(error) => {
                metrics.set_retry_state(RetryState::Stopped);
                consumer.error(true, error, Some("nats-input"));
                Phase::Stopped
            }
            ConnectorError::Retryable(error) => {
                self.failures += 1;
                if self.policy.exhausted(self.failures) {
                    metrics.set_retry_state(RetryState::Stopped);
                    consumer.error(
                        true,
                        retry_exhausted_error(&self.policy, self.failures, "input", error),
                        Some("nats-input"),
                    );
                    Phase::Stopped
                } else {
                    let delay = self.policy.delay(self.failures);
                    metrics.set_retry_state(RetryState::Retrying);
                    consumer.error(
                        false,
                        anyhow!(
                            "NATS input entered ERROR state (failure #{}), will retry in {delay:?}: {error:#}",
                            self.failures
                        ),
                        Some("nats-input"),
                    );
                    Phase::Retrying {
                        at: deadline_after(delay),
                    }
                }
            }
        }
    }
}

/// `now + delay`, saturating instead of panicking for absurd delays.
fn deadline_after(delay: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(delay)
        .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60))
}

impl NatsReader {
    fn new(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        table_name: &str,
        metrics: Arc<NatsInputMetrics>,
    ) -> AnyResult<Self> {
        let span = info_span!(
            "nats_input",
            table = %table_name,
            server_url = %redact_server_url(&config.connection_config.server_url),
            stream_name = %config.stream_name,
            // Note: this is consumer_name from config, not the created name with unique suffix.
            consumer_name = config.consumer_config.name.as_deref().unwrap_or(""),
            consumer_description = config.consumer_config.description.as_deref().unwrap_or(""),
            pipeline = config.consumer_config.metadata.get("pipeline").map(|s| s.as_str()).unwrap_or(""),
            filter_subjects = ?config.consumer_config.filter_subjects,
        );
        let (command_sender, command_receiver) = unbounded_channel();
        let consumer_name_prefix = consumer_name_prefix(&config.consumer_config, table_name);

        let consumer_clone = consumer.clone();
        let worker = TOKIO.spawn(async move {
            Self::worker_task(
                config,
                resume_info,
                consumer_clone,
                parser,
                InputCommandReceiver::new(command_receiver),
                consumer_name_prefix,
                metrics,
            )
            .instrument(span)
            .await
            .unwrap_or_else(|e| consumer.error(true, e, Some("nats-input")));
        });

        Ok(Self {
            command_sender,
            worker: Mutex::new(Some(worker)),
        })
    }

    async fn worker_task(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        mut command_receiver: InputCommandReceiver<Metadata, ()>,
        consumer_name_prefix: String,
        metrics: Arc<NatsInputMetrics>,
    ) -> Result<(), AnyError> {
        let ctx = StreamContext {
            connection_config: config.connection_config.clone(),
            server: redact_server_url(&config.connection_config.server_url),
            stream_name: config.stream_name.clone(),
            filter_subjects: config.consumer_config.filter_subjects.clone(),
            inactivity_timeout: config.inactivity_timeout().into(),
            request_timeout: config.connection_config.request_timeout().into(),
            consumer_name_prefix,
            metrics: metrics.clone(),
            direct_get_checked: Arc::new(AtomicBool::new(false)),
        };
        let mut retry = Retry {
            policy: RetryPolicy::new(
                config.retry_interval().into(),
                config.retry_max_interval().into(),
                config.retry_max_attempts,
            ),
            failures: 0,
        };
        let nats_consumer_config = translate_consumer_options(&config.consumer_config);
        let mut position = Position::new(&config);
        position.apply(&resume_info);
        let live_state = ReaderState {
            next_sequence: Arc::new(AtomicU64::new(position.flushed_end)),
            queue: Arc::new(InputQueue::new(consumer.clone())),
            healthy: Arc::new(AtomicBool::new(false)),
            snapshot_read: Arc::new(AtomicBool::new(false)),
            stream_created: Arc::new(Mutex::new(None)),
            last_read: Arc::new(Mutex::new(None)),
        };

        match Self::replay_steps(
            &ctx,
            &mut position,
            &retry.policy,
            consumer.as_ref(),
            parser.as_mut(),
            &mut command_receiver,
        )
        .await?
        {
            ReplayOutcome::Completed => {
                live_state
                    .next_sequence
                    .store(position.flushed_end, Ordering::Release);
                *live_state.stream_created.lock().unwrap() = position.stream_created.clone();
                *live_state.last_read.lock().unwrap() = position.last_ingested.clone();
            }
            ReplayOutcome::Stopped => return Ok(()),
        }

        let mut phase = Phase::Paused;
        // Deletes of the consumers of readers that stopped while the worker
        // carried on (on `Pause`, or after a reader failed). The worker joins
        // them before it exits, or a delete already queued at the server
        // could still be in flight after the process that owns it is gone.
        let mut pending_deletes: Vec<JoinHandle<()>> = Vec::new();

        // All command-loop exits must join reader and delete work, including
        // errors and a dropped command sender, not just explicit Disconnect.
        let result = loop {
            // Do not retain completed deletes for the lifetime of a
            // long-lived input.
            pending_deletes.retain(|handle| !handle.is_finished());
            let event = match &mut phase {
                Phase::Running(reader) => select! {
                    result = &mut reader.task => Event::ReaderExited(result),
                    command = command_receiver.recv() => match command {
                        Ok(command) => Event::Command(command),
                        Err(error) => break Err(error.into()),
                    },
                },
                // The start runs on its own task, so that a slow or
                // unreachable server does not hold up `Queue` (and with it
                // the pipeline's steps) or `Pause`.
                Phase::Starting(starting) => select! {
                    result = &mut starting.task => Event::Started(result),
                    command = command_receiver.recv() => match command {
                        Ok(command) => Event::Command(command),
                        Err(error) => break Err(error.into()),
                    },
                },
                Phase::Retrying { at } => select! {
                    _ = tokio::time::sleep_until(*at) => Event::RetryDue,
                    command = command_receiver.recv() => match command {
                        Ok(command) => Event::Command(command),
                        Err(error) => break Err(error.into()),
                    },
                },
                Phase::Paused => match command_receiver.recv().await {
                    Ok(command) => Event::Command(command),
                    Err(error) => break Err(error.into()),
                },
                Phase::Stopped => break Ok(()),
            };

            match event {
                Event::ReaderExited(result) => {
                    let Phase::Running(stopped) = std::mem::replace(&mut phase, Phase::Paused)
                    else {
                        unreachable!("only a running reader exits");
                    };
                    let consumer_handle = stopped.consumer;
                    pending_deletes.push(tokio::spawn(async move {
                        consumer_handle.delete().await;
                    }));
                    let error = match result {
                        Ok(Err(error)) => error,
                        Ok(Ok(())) => ConnectorError::Retryable(anyhow!(
                            "the reader stopped without reporting an error"
                        )),
                        // A panic is a bug that would most likely repeat on
                        // every retry: report it instead of looping.
                        Err(error) => {
                            ConnectorError::Fatal(anyhow!("the reader task failed: {error}"))
                        }
                    };
                    // A reader that started (its consumer create succeeded)
                    // and then failed right away is still a failure to
                    // deliver, not a recovery: keep counting consecutive
                    // failures across these cycles, or a cluster that always
                    // accepts the create but never lets a reader succeed
                    // would retry forever regardless of `retry_max_attempts`.
                    // A reader that passed a health check did recover, so its
                    // failure starts a new count.
                    if live_state.healthy.swap(false, Ordering::AcqRel) {
                        retry.failures = 0;
                    }
                    phase = retry.fail(
                        error.with_context("NATS reader failed"),
                        consumer.as_ref(),
                        &metrics,
                    );
                }
                Event::Started(result) => {
                    let result = result.unwrap_or_else(|error| {
                        Err(ConnectorError::Fatal(anyhow!(
                            "starting the reader failed: {error}"
                        )))
                    });
                    phase = match result {
                        Ok(started) => {
                            if retry.failures > 0 {
                                info!(
                                    "NATS input recovered after {} failures, resuming from {}",
                                    retry.failures,
                                    live_state.next_sequence.load(Ordering::Acquire)
                                );
                            }
                            // `failures` is deliberately not reset here: a
                            // successful create is not yet a recovery if the
                            // new reader immediately fails again. Real
                            // forward progress (a non-empty Queue flush or a
                            // passed health check) resets it instead.
                            metrics.set_retry_state(RetryState::Healthy);
                            Phase::Running(started)
                        }
                        Err(error) => retry.fail(error, consumer.as_ref(), &metrics),
                    };
                }
                Event::RetryDue => {
                    metrics.retry_attempt(retry.failures);
                    phase = Phase::Starting(Self::start(
                        &nats_consumer_config,
                        &ctx,
                        &live_state,
                        &position,
                        parser.fork(),
                    ));
                }
                Event::Command(InputReaderCommand::Replay { .. }) => {
                    break Err(anyhow!(
                        "Replay must be at the beginning of the command stream"
                    ));
                }
                Event::Command(InputReaderCommand::Queue { .. }) => {
                    let (metadata, buffer_size, hash, timestamp) =
                        Self::flush(&live_state, &mut position);
                    if buffer_size.records > 0 {
                        debug!(
                            "Queued {buffer_size:?} records ({:?})",
                            metadata.sequence_numbers
                        );
                        // Real forward progress: messages reached the
                        // circuit, so whatever got the connector here worked.
                        retry.failures = 0;
                    } else {
                        trace!("Queued 0 records ({:?})", metadata.sequence_numbers);
                    }
                    let metadata_json = match serde_json::to_value(&metadata) {
                        Ok(metadata) => metadata,
                        Err(error) => break Err(error.into()),
                    };
                    consumer.extended(
                        buffer_size,
                        Some(Resume::Replay {
                            hash,
                            seek: metadata_json.clone(),
                            replay: rmpv::Value::Nil,
                        }),
                        vec![Watermark::new(timestamp, Some(metadata_json))],
                    );
                }
                Event::Command(InputReaderCommand::Pause) => {
                    pending_deletes
                        .extend(std::mem::replace(&mut phase, Phase::Paused).stop().await);
                    retry.failures = 0;
                    metrics.set_retry_state(RetryState::Healthy);
                }
                Event::Command(InputReaderCommand::Extend) => {
                    info!(
                        "Extend from {}",
                        live_state.next_sequence.load(Ordering::Acquire)
                    );
                    if matches!(phase, Phase::Paused | Phase::Retrying { .. }) {
                        phase = Phase::Starting(Self::start(
                            &nats_consumer_config,
                            &ctx,
                            &live_state,
                            &position,
                            parser.fork(),
                        ));
                    }
                }
                Event::Command(InputReaderCommand::Disconnect) => {
                    pending_deletes
                        .extend(std::mem::replace(&mut phase, Phase::Stopped).stop().await);
                    metrics.set_retry_state(RetryState::Healthy);
                }
            }
        };
        pending_deletes.extend(phase.stop().await);
        // Join deletes detached by an earlier Pause/failure too: the worker
        // must not exit, and so must not close the reader's command channel,
        // while one of its own deletes is still in flight.
        for pending_delete in pending_deletes {
            let _ = pending_delete.await;
        }
        result
    }

    /// Flushes queued records to the circuit and describes the step.
    ///
    /// Returns the step's metadata, the size and hash of what was flushed,
    /// and the step's timestamp.
    fn flush(
        state: &ReaderState,
        position: &mut Position,
    ) -> (Metadata, BufferSize, u64, chrono::DateTime<Utc>) {
        let (buffer_size, hasher, entries) = state.queue.flush_with_aux();
        let mut sequences = Vec::new();
        let mut snapshot = false;
        let mut advance = 0;
        for (_, queued) in &entries {
            match queued {
                Queued::Message {
                    sequence,
                    timestamp,
                    snapshot: in_snapshot,
                } => {
                    sequences.push(*sequence);
                    snapshot |= in_snapshot;
                    advance = advance.max(sequence + 1);
                    position.last_ingested = Some(Anchor::new(*sequence, *timestamp));
                }
                Queued::SnapshotComplete => position.snapshot_pending = false,
                Queued::Advance(sequence) => advance = advance.max(*sequence),
            }
        }
        // Where the next step starts, from what was flushed: the reader may
        // already have queued later messages, or be about to.
        position.flushed_end = position.flushed_end.max(advance);
        let sequence_numbers = match (sequences.first(), sequences.last()) {
            // A replay checks that the step's last message ends the range,
            // so the range ends right after it, even if the position is
            // further on.
            (Some(first), Some(last)) => *first..*last + 1,
            _ => position.flushed_end..position.flushed_end,
        };
        let metadata = Metadata {
            sequence_numbers,
            messages: (!sequences.is_empty()).then_some(sequences.len() as u64),
            sequences: snapshot.then_some(sequences),
            stream_created: state.stream_created.lock().unwrap().clone(),
            last_per_subject_pending: position.snapshot_pending,
            last_ingested: position.last_ingested.clone(),
        };
        let timestamp = entries.last().map(|(ts, _)| *ts).unwrap_or_else(Utc::now);
        let hash = hasher.map(|h| h.finish()).unwrap_or(0);
        (metadata, buffer_size, hash, timestamp)
    }

    /// Replays the journaled steps that the controller requests, which come
    /// before any other command.
    async fn replay_steps(
        ctx: &StreamContext,
        position: &mut Position,
        policy: &RetryPolicy,
        consumer: &dyn InputConsumer,
        // `&mut`, not `&`: the future holds it across awaits, and a shared
        // reference would make it `Send` only for a `Sync` parser.
        parser: &mut dyn Parser,
        command_receiver: &mut InputCommandReceiver<Metadata, ()>,
    ) -> Result<ReplayOutcome, AnyError> {
        // Kept across steps; dropped after an error, so that the next attempt
        // connects afresh.
        let mut source: Option<ReplaySource> = None;
        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Replaying {:?}", metadata);
            if metadata.sequence_numbers.is_empty() {
                consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
                position.apply(&metadata);
                continue;
            }

            let mut failures: u32 = 0;
            loop {
                let attempt = async {
                    let replay_source = match source.take() {
                        Some(replay_source) => replay_source,
                        None => ReplaySource::connect(ctx).await?,
                    };
                    let messages = replay_source
                        .fetch_step(ctx, &metadata, &mut position.stream_created)
                        .await;
                    if !matches!(messages, Err(ConnectorError::Retryable(_))) {
                        source = Some(replay_source);
                    }
                    messages
                };
                let Some(result) = unless_disconnected(command_receiver, attempt).await? else {
                    return Ok(ReplayOutcome::Stopped);
                };
                match result {
                    Ok(messages) => {
                        let (buffer_size, hash) =
                            Self::feed_replayed(messages, consumer, parser.fork());
                        consumer.replayed(buffer_size, hash);
                        position.apply(&metadata);
                        ctx.metrics.set_retry_state(RetryState::Healthy);
                        break;
                    }
                    Err(ConnectorError::Fatal(error)) => {
                        ctx.metrics.set_retry_state(RetryState::Stopped);
                        consumer.error(
                            true,
                            error.context(format!(
                                "While attempting to replay sequences {:?}",
                                metadata.sequence_numbers
                            )),
                            Some("nats-input"),
                        );
                        return Ok(ReplayOutcome::Stopped);
                    }
                    Err(ConnectorError::Retryable(error)) => {
                        failures += 1;
                        if policy.exhausted(failures) {
                            ctx.metrics.set_retry_state(RetryState::Stopped);
                            consumer.error(
                                true,
                                retry_exhausted_error(policy, failures, "replay", error),
                                Some("nats-input"),
                            );
                            return Ok(ReplayOutcome::Stopped);
                        }
                        let delay = policy.delay(failures);
                        ctx.metrics.set_retry_state(RetryState::Retrying);
                        ctx.metrics.retry_attempt(failures);
                        consumer.error(
                            false,
                            anyhow!(
                                "NATS replay entered ERROR state (failure #{failures}), retrying in {delay:?}: {error:#}"
                            ),
                            Some("nats-input"),
                        );
                        // Wait out the backoff, but notice a Disconnect
                        // during it: the wait can reach `retry_max_interval`.
                        // Any other command waits until the step is done: an
                        // acknowledged replay step must run to completion.
                        if unless_disconnected(
                            command_receiver,
                            tokio::time::sleep_until(deadline_after(delay)),
                        )
                        .await?
                        .is_none()
                        {
                            return Ok(ReplayOutcome::Stopped);
                        }
                    }
                }
            }
        }
        Ok(ReplayOutcome::Completed)
    }

    /// Parses replayed messages and feeds them to the circuit, the way
    /// [InputQueue] does for new ones, so that the record count and hash
    /// match the step's journal entry.
    fn feed_replayed(
        messages: Vec<ReplayMessage>,
        consumer: &dyn InputConsumer,
        mut parser: Box<dyn Parser>,
    ) -> (BufferSize, u64) {
        let mut hasher = Xxh3Default::new();
        let mut total = BufferSize::default();
        for message in messages {
            let (buffer, errors) = parser.parse(&message.payload, None);
            consumer.parse_errors(errors);
            if let Some(mut buffer) = buffer {
                let amount = buffer.len();
                consumer.buffered(amount);
                total += amount;
                buffer.hash(&mut hasher);
                consumer.check_flush(buffer.flush());
            }
        }
        (total, hasher.finish())
    }

    /// Starts a reader at the current position, on a task of its own.
    fn start(
        consumer_config: &nats_consumer::pull::Config,
        ctx: &StreamContext,
        state: &ReaderState,
        position: &Position,
        parser: Box<dyn Parser>,
    ) -> Starting {
        let cancel = CancellationToken::new();
        let task = tokio::spawn(
            start_reader(
                consumer_config.clone(),
                ctx.clone(),
                state.clone(),
                position.snapshot_pending && !state.snapshot_read.load(Ordering::Acquire),
                parser,
                cancel.clone(),
            )
            .in_current_span(),
        );
        Starting { cancel, task }
    }
}

/// A reader being started.
struct Starting {
    /// Also cancels the reader, once it runs.
    cancel: CancellationToken,
    task: JoinHandle<Result<Box<Reader>, ConnectorError>>,
}

/// Connects, checks the stream, creates a consumer at the current position
/// (or for a new `LastPerSubject` snapshot, if `take_snapshot`), and starts a
/// reader task on it.
///
/// Cancelling `cancel` stops the start at its next step. A consumer create
/// that was already sent runs to completion, since it may succeed anyway;
/// the consumer is then deleted.
async fn start_reader(
    consumer_config: nats_consumer::pull::Config,
    ctx: StreamContext,
    state: ReaderState,
    take_snapshot: bool,
    parser: Box<dyn Parser>,
    cancel: CancellationToken,
) -> Result<Box<Reader>, ConnectorError> {
    let cancelled = || ConnectorError::Retryable(anyhow!("the start of the reader was cancelled"));
    let mut stream_created = state.stream_created.lock().unwrap().clone();
    let last_read = state.last_read.lock().unwrap().clone();
    let next_sequence = state.next_sequence.load(Ordering::Acquire);
    let prepare = async {
        let jetstream = connect(&ctx).await.map_err(|error| {
            error.with_context(format!(
                "NATS initialization failed for stream '{}' at server '{}' (connection_timeout={}, request_timeout={})",
                ctx.stream_name,
                ctx.server,
                ctx.connection_config.connection_timeout(),
                ctx.connection_config.request_timeout(),
            ))
        })?;
        let info = fetch_stream_info(&jetstream, &ctx.stream_name).await?;
        verify_stream_identity(
            &jetstream,
            &ctx.stream_name,
            &info,
            &mut stream_created,
            last_read.as_ref(),
        )
        .await?;
        replay::warn_once_without_direct_get(&ctx, &jetstream, &info);
        if info.config.retention != RetentionPolicy::Limits {
            warn!(
                "Stream '{}' has {:?} retention: the server may discard a message as soon as the connector has read it, before a checkpoint covers it, and then the pipeline cannot replay it after a failure; exactly-once fault tolerance needs Limits retention",
                ctx.stream_name, info.config.retention
            );
        }
        if !take_snapshot {
            // A new snapshot does not continue from the position.
            validate_resume_position(
                &StreamState::from(&info),
                &ctx.stream_name,
                !ctx.filter_subjects.is_empty(),
                next_sequence,
            )?;
        }
        Ok::<_, ConnectorError>((jetstream, info.state.first_sequence))
    };
    let (jetstream, first_sequence) = select! {
        biased;
        _ = cancel.cancelled() => return Err(cancelled()),
        prepared = prepare => prepared?,
    };

    let deliver_policy = if take_snapshot {
        Some(nats_consumer::DeliverPolicy::LastPerSubject)
    } else if next_sequence > 0 {
        Some(nats_consumer::DeliverPolicy::ByStartSequence {
            start_sequence: next_sequence,
        })
    } else {
        None
    };
    let nats_consumer = create_nats_consumer(
        &jetstream,
        &consumer_config,
        &ctx,
        deliver_policy,
        next_sequence,
    )
    .await?;
    let consumer_info = nats_consumer.cached_info().clone();
    let handle = ConsumerHandle {
        jetstream: jetstream.clone(),
        ctx: ctx.clone(),
        name: consumer_info.name.clone(),
    };
    if cancel.is_cancelled() {
        handle.delete().await;
        return Err(cancelled());
    }

    if next_sequence == 0 {
        // Pin the start position the server chose for the configured policy
        // (see the module documentation). Nothing has been read yet, so
        // nothing is queued that this could skip. The server reports `All`
        // as starting after sequence 0, even when retention limits have
        // removed the messages before `first_sequence`; a later consumer at
        // 1 would then look as if it lost them.
        let start = (consumer_info.delivered.stream_sequence + 1).max(first_sequence);
        state.next_sequence.store(start, Ordering::Release);
        state
            .queue
            .push_with_aux((None, Vec::new()), Utc::now(), Queued::Advance(start));
        debug!(
            "Consumer '{}' starts at stream sequence {start}",
            consumer_info.name
        );
    }
    // The create response counts the snapshot: one message per subject.
    let snapshot_size = take_snapshot.then_some(consumer_info.num_pending);

    let messages = match nats_consumer.messages().await {
        Ok(messages) => messages,
        Err(error) => {
            // The consumer exists on the server but nothing will ever read
            // from it; remove it now rather than leaving it to expire.
            handle.delete().await;
            return Err(ConnectorError::Retryable(anyhow!(
                "Failed to start reading from consumer '{}': {error}",
                consumer_info.name
            )));
        }
    };

    let stream_created = stream_created.expect("the stream identity is known after checking it");
    // Before the reader queues anything; see [ReaderState::stream_created].
    *state.stream_created.lock().unwrap() = Some(stream_created.clone());
    let task = tokio::spawn(
        read_messages(
            ReaderContext {
                jetstream,
                ctx,
                consumer_name: consumer_info.name,
                state,
                stream_created,
                cancel: cancel.clone(),
            },
            messages,
            parser,
            snapshot_size,
        )
        .in_current_span(),
    );
    Ok(Box::new(Reader {
        cancel,
        task,
        consumer: handle,
    }))
}

enum ReplayOutcome {
    Completed,
    /// Disconnected, or failed fatally (already reported).
    Stopped,
}

/// Runs `future` to completion unless a `Disconnect` command arrives first,
/// in which case it returns `None`. Other commands that arrive meanwhile are
/// kept, in order, for the caller to read later.
async fn unless_disconnected<T>(
    command_receiver: &mut InputCommandReceiver<Metadata, ()>,
    future: impl Future<Output = T>,
) -> Result<Option<T>, AnyError> {
    tokio::pin!(future);
    let mut pending_commands = Vec::new();
    let result = loop {
        select! {
            output = &mut future => break Some(output),
            command = command_receiver.recv() => match command? {
                InputReaderCommand::Disconnect => break None,
                other => pending_commands.push(other),
            },
        }
    };
    for command in pending_commands.into_iter().rev() {
        command_receiver.put_back(command);
    }
    Ok(result)
}

/// Connects to the server.
async fn connect(ctx: &StreamContext) -> Result<jetstream::Context, ConnectorError> {
    let deadline = ctx
        .request_timeout
        .saturating_add(ctx.connection_config.connection_timeout().into());
    let connect = async {
        let options = translate_connect_options(&ctx.connection_config)
            .await
            .map_err(ConnectorError::Retryable)?;
        let client = options
            .connect(&ctx.connection_config.server_url)
            .await
            // The error can repeat the URL, credentials included.
            .map_err(|error| {
                ConnectorError::Retryable(anyhow!(
                    "Failed to connect to NATS server at {}: {}",
                    ctx.server,
                    error.kind()
                ))
            })?;
        let mut jetstream = jetstream::new(client);
        // Bounds every JetStream API request, not just the ones the client
        // library sends with the connection's request timeout.
        jetstream.set_timeout(ctx.request_timeout);
        Ok(jetstream)
    };
    tokio::time::timeout(deadline, connect)
        .await
        .unwrap_or_else(|_| {
            Err(ConnectorError::Retryable(anyhow!(
                "NATS initialization timed out after {deadline:?}"
            )))
        })
}

/// Builds the prefix for the names of the consumers a connector creates.
///
/// Uses the configured consumer name when present, otherwise the table name.
/// Every consumer gets this prefix plus a unique suffix (see
/// [`create_nats_consumer`]), so consumers are always named by us rather than
/// by the server. Server-assigned names are opaque, which makes it impossible
/// to attribute a consumer to a pipeline from the NATS side.
fn consumer_name_prefix(consumer_config: &cfg::ConsumerConfig, table_name: &str) -> String {
    let base = consumer_config
        .name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or(table_name);
    sanitize_consumer_name(base)
}

/// Replaces characters that JetStream rejects in consumer names.
///
/// JetStream forbids whitespace, `.`, `*`, `>`, `/`, and `\`. Feldera table
/// names may be quoted identifiers containing any of these, so anything
/// outside `[A-Za-z0-9_-]` is mapped to `_`. An input with no usable
/// characters yields `feldera`.
fn sanitize_consumer_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.trim_matches('_').is_empty() {
        "feldera".to_string()
    } else {
        sanitized
    }
}

/// `failures` consecutive failures, the first plus `failures - 1` retries.
fn retry_exhausted_error(
    retry_policy: &RetryPolicy,
    failures: u32,
    context: &str,
    last_error: AnyError,
) -> AnyError {
    anyhow!(
        "NATS {context} gave up after {} consecutive failed attempts (retry_max_attempts={}); last error: {:#}",
        failures.saturating_sub(1),
        retry_policy.max_attempts().unwrap_or(0),
        last_error,
    )
}

/// Creates the consumer to read from.
///
/// `deliver_policy` overrides the configured deliver policy;
/// `start_sequence` is only for messages.
async fn create_nats_consumer(
    jetstream: &jetstream::Context,
    consumer_config: &nats_consumer::pull::Config,
    ctx: &StreamContext,
    deliver_policy: Option<nats_consumer::DeliverPolicy>,
    start_sequence: u64,
) -> Result<PullConsumer, ConnectorError> {
    let mut consumer_config = consumer_config.clone();
    let stream_name = ctx.stream_name.as_str();
    if let Some(deliver_policy) = deliver_policy {
        consumer_config.deliver_policy = deliver_policy;
    }

    // Every consumer gets a unique name: the connector's prefix plus a
    // time-ordered UUID. The unique suffix avoids "consumer already exists"
    // errors on rapid restarts before the previous consumer is gone; the
    // prefix keeps the consumer attributable to its connector from the NATS
    // side.
    let consumer_name = format!("{}_{}", ctx.consumer_name_prefix, uuid::Uuid::now_v7());
    consumer_config.name = Some(consumer_name.clone());

    let create_result = jetstream
        .create_consumer_strict_on_stream(consumer_config.clone(), stream_name)
        .await;
    let error = match create_result {
        Ok(consumer) => {
            ctx.metrics.consumer_created();
            debug!(
                "Created consumer '{consumer_name}' on stream '{stream_name}' (start_sequence={start_sequence})"
            );
            return Ok(consumer);
        }
        Err(error) => error,
    };
    let kind = error.kind();
    if create_may_have_succeeded(&kind) {
        // On an overloaded cluster, consumer creation can take minutes and
        // still succeed long after the client gave up, leaving a consumer
        // that nobody reads. The server may have created it, so count it as
        // created before cleaning it up, or consumers_deleted_total runs
        // permanently ahead of consumers_created_total.
        ctx.metrics.consumer_created();
        let handle = ConsumerHandle {
            jetstream: jetstream.clone(),
            ctx: ctx.clone(),
            name: consumer_name.clone(),
        };
        handle.delete().await;
        clean_up_delayed_create(handle);
    }
    let error = anyhow!(error).context(format!(
        "Failed to create consumer '{consumer_name}' on stream '{stream_name}' (start_sequence={start_sequence}, deliver_policy={:?}, filter_subjects={:?})",
        consumer_config.deliver_policy, consumer_config.filter_subjects,
    ));
    Err(match kind {
        ConsumerCreateStrictErrorKind::JetStream(api_error)
            if is_permanent_api_error(&api_error) =>
        {
            ConnectorError::Fatal(error)
        }
        ConsumerCreateStrictErrorKind::InvalidName
        | ConsumerCreateStrictErrorKind::InvalidConsumerType => ConnectorError::Fatal(error),
        _ => ConnectorError::Retryable(error),
    })
}

/// Whether the server may have created the consumer despite the error.
///
/// A timeout or a failed request leaves the outcome unknown. `Other` means
/// the server accepted the create but the client could not interpret the
/// reply, so the consumer exists. The remaining kinds are server rejections.
fn create_may_have_succeeded(kind: &ConsumerCreateStrictErrorKind) -> bool {
    matches!(
        kind,
        ConsumerCreateStrictErrorKind::TimedOut
            | ConsumerCreateStrictErrorKind::Request
            | ConsumerCreateStrictErrorKind::Other
    )
}

/// A consumer this connector created, for deleting it.
struct ConsumerHandle {
    jetstream: jetstream::Context,
    ctx: StreamContext,
    name: String,
}

impl ConsumerHandle {
    /// Best-effort deletion.
    ///
    /// Ephemeral consumers expire on their own after the server's
    /// `inactive_threshold`, but on a struggling cluster that cleanup lags
    /// far behind, and every consumer the connector abandons adds to the
    /// load that made it abandon the consumer in the first place. Deleting
    /// explicitly keeps the server-side consumer count equal to the number
    /// of live readers.
    ///
    /// Failure is logged at `warn!` and counted in
    /// `input_connector_nats_consumer_delete_failures_total`, not propagated:
    /// the caller is already tearing down or retrying, and the consumer will
    /// still expire eventually.
    async fn delete(&self) {
        let (consumer_name, stream_name) = (&self.name, &self.ctx.stream_name);
        match tokio::time::timeout(
            CONSUMER_DELETE_TIMEOUT,
            self.jetstream
                .delete_consumer_from_stream(consumer_name, stream_name),
        )
        .await
        {
            Ok(Ok(_)) => {
                self.ctx.metrics.consumer_deleted();
                debug!("Deleted consumer '{consumer_name}' on stream '{stream_name}'");
            }
            Ok(Err(error)) => {
                self.ctx.metrics.consumer_delete_failed();
                warn!(
                    "Failed to delete consumer '{consumer_name}' on stream '{stream_name}', it will expire on its own: {error}"
                );
            }
            Err(_) => {
                self.ctx.metrics.consumer_delete_failed();
                warn!(
                    "Timed out after {CONSUMER_DELETE_TIMEOUT:?} deleting consumer '{consumer_name}' on stream '{stream_name}', it will expire on its own"
                );
            }
        }
    }
}

/// Grace period before re-checking whether a consumer we just deleted has
/// reappeared.
const DELAYED_CREATE_RECHECK_DELAY: Duration = Duration::from_secs(2);

/// Catches a create that a JetStream meta-leader change applies after a
/// delete meant to clean it up, undoing that delete.
///
/// Deleting after a timed-out create assumes the two requests land on the
/// server in that order. They usually do, but a meta-leader change between
/// them can apply the create after the delete, leaving an orphan that nobody
/// will ever read. A delete sent immediately after would race the same
/// reordering, so this instead waits out a grace period, then deletes the
/// consumer again if it exists.
///
/// Spawned rather than awaited: the create path should not block on this
/// best-effort cleanup.
fn clean_up_delayed_create(handle: ConsumerHandle) {
    tokio::spawn(async move {
        tokio::time::sleep(DELAYED_CREATE_RECHECK_DELAY).await;
        let reappeared = handle
            .jetstream
            .get_consumer_from_stream::<nats_consumer::Config, _, _>(
                &handle.name,
                &handle.ctx.stream_name,
            )
            .await
            .is_ok();
        if reappeared {
            handle.delete().await;
        }
    });
}

/// A running reader task and its consumer.
struct Reader {
    cancel: CancellationToken,
    task: JoinHandle<Result<(), ConnectorError>>,
    consumer: ConsumerHandle,
}

impl Reader {
    /// Stops the reader task and waits for it to end. Returns the reader's
    /// consumer, which the caller must delete.
    ///
    /// Bounded by task shutdown, not by anything server-side, so that a slow
    /// server cannot stall the worker's command loop after a `Pause`.
    async fn stop(self) -> ConsumerHandle {
        self.cancel.cancel();
        let _ = self.task.await;
        self.consumer
    }
}

/// What a reader task works with besides its messages and parser.
struct ReaderContext {
    jetstream: jetstream::Context,
    ctx: StreamContext,
    consumer_name: String,
    state: ReaderState,
    /// See [Metadata::stream_created].
    stream_created: String,
    cancel: CancellationToken,
}

/// Reads messages and queues them, parsed and tagged with their stream
/// sequence, until cancelled (`Ok`) or until reading fails (`Err`).
///
/// `snapshot_size` is the number of messages in the `LastPerSubject` snapshot
/// that the consumer delivers first, if it takes one.
async fn read_messages(
    reader: ReaderContext,
    mut messages: nats_consumer::pull::Stream,
    mut parser: Box<dyn Parser>,
    snapshot_size: Option<u64>,
) -> Result<(), ConnectorError> {
    let ReaderContext {
        jetstream,
        ctx,
        consumer_name,
        state,
        stream_created,
        cancel,
    } = reader;
    let health_check = || check_health(&jetstream, &ctx, &consumer_name, &stream_created);
    let finish_snapshot = || {
        state.snapshot_read.store(true, Ordering::Release);
        state
            .queue
            .push_with_aux((None, Vec::new()), Utc::now(), Queued::SnapshotComplete);
    };
    // The consumer is new, so its first message is delivery #1.
    let mut expected_delivery = 1;
    let mut snapshot_size = snapshot_size;
    if snapshot_size == Some(0) {
        finish_snapshot();
        snapshot_size = None;
    }
    loop {
        let next = select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            next = tokio::time::timeout(ctx.inactivity_timeout, messages.next()) => next,
        };
        let message = match next {
            // Nothing arrived for a while: make sure that is because the
            // stream is quiet, not because the server or consumer is gone.
            Err(_elapsed) => {
                let check = async {
                    let last_sequence = health_check().await.map_err(|error| {
                        error.with_context(format!(
                            "no message for {:?} and the health check failed",
                            ctx.inactivity_timeout
                        ))
                    })?;
                    state.healthy.store(true, Ordering::Release);
                    if snapshot_size.is_none() {
                        advance_past_quiet(
                            &jetstream,
                            &ctx,
                            &state,
                            &consumer_name,
                            expected_delivery - 1,
                            last_sequence,
                        )
                        .await;
                    }
                    Ok::<_, ConnectorError>(())
                };
                select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    result = check => result?,
                }
                continue;
            }
            Ok(None) => {
                return Err(ConnectorError::Retryable(anyhow!(
                    "the message stream of consumer '{consumer_name}' ended"
                )));
            }
            Ok(Some(Err(error))) if error.kind() == MessagesErrorKind::MissingHeartbeat => {
                // The server sends heartbeats while a pull request waits, so
                // missing ones mean the server, the consumer, or the
                // connection is in trouble. A slow but healthy client can
                // miss them too, so check before giving up the consumer.
                select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    result = health_check() => {
                        result.map_err(|error| error.with_context("missed heartbeats"))?;
                    }
                }
                continue;
            }
            Ok(Some(Err(error))) => {
                return Err(ConnectorError::Retryable(anyhow!(
                    "reading from consumer '{consumer_name}' failed: {error}"
                )));
            }
            Ok(Some(Ok(message))) => message,
        };
        let (stream_sequence, delivery, timestamp) = match message.info() {
            Ok(info) => (
                info.stream_sequence,
                info.consumer_sequence,
                info.published.unix_timestamp_nanos(),
            ),
            Err(error) => {
                return Err(ConnectorError::Retryable(anyhow!(
                    "message from consumer '{consumer_name}' has malformed metadata: {error}"
                )));
            }
        };
        if delivery != expected_delivery {
            // The server delivered messages that never reached us.
            return Err(ConnectorError::Retryable(anyhow!(
                "consumer '{consumer_name}' delivered message #{delivery} after #{}: messages were lost in transit, so the connector resumes from stream sequence {}",
                expected_delivery - 1,
                state.next_sequence.load(Ordering::Acquire)
            )));
        }
        expected_delivery += 1;
        let in_snapshot = snapshot_size.is_some_and(|size| delivery <= size);
        if stream_sequence >= state.next_sequence.load(Ordering::Acquire) {
            trace!("Got message #{stream_sequence}");
            state.queue.push_with_aux(
                parser.parse(&message.payload, None),
                Utc::now(),
                Queued::Message {
                    sequence: stream_sequence,
                    timestamp,
                    snapshot: in_snapshot,
                },
            );
            *state.last_read.lock().unwrap() = Some(Anchor::new(stream_sequence, timestamp));
            state
                .next_sequence
                .store(stream_sequence + 1, Ordering::Release);
        } else {
            // Only a new snapshot can deliver messages behind the position:
            // the last message of a subject that has not changed since the
            // connector read it.
            trace!("Skipping already-processed message #{stream_sequence}");
        }
        if snapshot_size.is_some_and(|size| delivery >= size) {
            finish_snapshot();
            snapshot_size = None;
        }
    }
}

/// Checks that the stream (still the same one) and the consumer exist.
///
/// Returns the stream's last sequence.
async fn check_health(
    jetstream: &jetstream::Context,
    ctx: &StreamContext,
    consumer_name: &str,
    stream_created: &str,
) -> Result<u64, ConnectorError> {
    let info = fetch_stream_info(jetstream, &ctx.stream_name).await?;
    if stream_identity(&info) != stream_created {
        // Replaced, or restored from a backup: the next reader finds out
        // which, from the messages.
        return Err(ConnectorError::Retryable(anyhow!(
            "stream '{}' now has creation time {}, so it was replaced or restored from a backup; checking it again",
            ctx.stream_name,
            info.created
        )));
    }
    jetstream
        .get_consumer_from_stream::<nats_consumer::Config, _, _>(consumer_name, &ctx.stream_name)
        .await
        .map_err(|error| {
            ConnectorError::Retryable(anyhow!(
                "consumer '{consumer_name}' is no longer available: {error}"
            ))
        })?;
    Ok(info.state.last_sequence)
}

/// Moves the reader's position past messages it does not read, up to
/// `last_sequence`, the stream's last sequence a moment ago.
///
/// The position only moves with the messages the connector reads, so with
/// filter subjects it can stay far behind the stream while other subjects
/// are busy. A retention limit then removes the messages after it, and a
/// later consumer at that position looks as if it lost messages: the
/// connector cannot tell that none of them matched its filters, and fails
/// rather than risk skipping data. While the reader is idle, this asks the
/// stream leader (not a replica, which may lag) for the next message on each
/// filter subject: none of the connector's messages exists before the first
/// of them, or before `last_sequence + 1` if there is none, so the position
/// can move there without skipping anything.
///
/// `processed` is the number of messages the reader has processed. If the
/// consumer has delivered more by the time the leader answered, one of them
/// may be a message that was deleted since, which the leader no longer
/// knows about, so the position stays.
///
/// Best effort: on an error, the position stays where it is.
async fn advance_past_quiet(
    jetstream: &jetstream::Context,
    ctx: &StreamContext,
    state: &ReaderState,
    consumer_name: &str,
    processed: u64,
    last_sequence: u64,
) {
    let from = state.next_sequence.load(Ordering::Acquire);
    if from == 0 || from > last_sequence {
        return;
    }
    let end = async {
        let end = replay::first_unread(jetstream, ctx, from, last_sequence + 1).await?;
        // Asked after the leader, so that it counts every delivery the
        // leader's answer could have missed.
        let delivered = jetstream
            .get_consumer_from_stream::<nats_consumer::Config, _, _>(
                consumer_name,
                &ctx.stream_name,
            )
            .await
            .map_err(|error| ConnectorError::Retryable(error.into()))?
            .cached_info()
            .delivered
            .consumer_sequence;
        Ok::<_, ConnectorError>(if delivered == processed { end } else { from })
    };
    match end.await {
        Ok(end) if end > from => {
            debug!("No message to read in stream sequences {from}..{end}, moving past them");
            state.next_sequence.store(end, Ordering::Release);
            state
                .queue
                .push_with_aux((None, Vec::new()), Utc::now(), Queued::Advance(end));
        }
        Ok(_) => {}
        Err(error) => debug!(
            "Could not check for unread messages after stream sequence {from}: {}",
            error.inner()
        ),
    }
}

async fn fetch_stream_info(
    jetstream: &jetstream::Context,
    stream_name: &str,
) -> Result<nats_stream::Info, ConnectorError> {
    match jetstream.get_stream(stream_name).await {
        Ok(stream) => Ok(stream.cached_info().clone()),
        Err(error) => {
            let fatal = match error.kind() {
                GetStreamErrorKind::JetStream(api_error) => is_permanent_api_error(&api_error),
                GetStreamErrorKind::EmptyName | GetStreamErrorKind::InvalidStreamName => true,
                GetStreamErrorKind::Request => false,
            };
            let error = anyhow!(error).context(format!("Failed to get stream '{stream_name}'"));
            Err(if fatal {
                ConnectorError::Fatal(error)
            } else {
                ConnectorError::Retryable(error)
            })
        }
    }
}

/// See [Metadata::stream_created].
fn stream_identity(info: &nats_stream::Info) -> String {
    info.created.unix_timestamp_nanos().to_string()
}

/// Checks that the stream is the one the connector read before, and records
/// its creation time in `recorded`.
///
/// `last_ingested` is the last message the connector ingested, if any. While
/// the stream still has a message at that sequence, the message decides,
/// because a restore from a backup can give the stream a new creation time
/// (nats-server 2.15 always does, and so do clustered restores on some
/// earlier versions): the same message means the same stream, another
/// message means another stream, or the stream restored from an older backup
/// and written to since. Otherwise the creation time decides.
async fn verify_stream_identity(
    jetstream: &jetstream::Context,
    stream_name: &str,
    info: &nats_stream::Info,
    recorded: &mut Option<String>,
    last_ingested: Option<&Anchor>,
) -> Result<(), ConnectorError> {
    if let Some(anchor) = last_ingested {
        let stream = jetstream
            .get_stream_no_info(stream_name)
            .await
            .map_err(|error| ConnectorError::Fatal(error.into()))?;
        match stream.get_raw_message(anchor.sequence).await {
            Ok(message) => {
                return check_anchor(
                    anchor,
                    message.time.unix_timestamp_nanos(),
                    info,
                    recorded,
                    stream_name,
                );
            }
            Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => {}
            Err(error) => return Err(replay::raw_message_error(error)),
        }
    }
    check_stream_identity(recorded, info, stream_name)
}

/// Checks the message that the stream holds at the sequence of `anchor`, a
/// message the connector ingested, which the server stored at `timestamp`.
/// See [verify_stream_identity].
fn check_anchor(
    anchor: &Anchor,
    timestamp: i128,
    info: &nats_stream::Info,
    recorded: &mut Option<String>,
    stream_name: &str,
) -> Result<(), ConnectorError> {
    if anchor.timestamp != timestamp.to_string() {
        return Err(ConnectorError::Fatal(anyhow!(
            "Stream '{stream_name}' is not the stream this pipeline read before: its message {} was stored {timestamp} ns after the Unix epoch, but the message {} that the pipeline ingested was stored {} ns after the epoch. The stream was deleted and recreated, or restored from a backup older than what the pipeline ingested and then written to, so the recorded sequence numbers no longer identify the messages the pipeline ingested. To read the stream from the start, reset the connector, for example by changing its configuration",
            anchor.sequence,
            anchor.sequence,
            anchor.timestamp
        )));
    }
    let actual = stream_identity(info);
    if let Some(previous) = recorded.as_ref()
        && *previous != actual
    {
        info!(
            "Stream '{stream_name}' has a new creation time ({}), as after a restore from a backup, but it still holds message {} that the pipeline ingested, unchanged, so the connector continues",
            info.created, anchor.sequence
        );
    }
    *recorded = Some(actual);
    Ok(())
}

/// Checks that the stream has the creation time that `expected` recorded, if
/// any, and records it otherwise.
fn check_stream_identity(
    expected: &mut Option<String>,
    info: &nats_stream::Info,
    stream_name: &str,
) -> Result<(), ConnectorError> {
    let actual = stream_identity(info);
    match expected {
        Some(expected) if *expected != actual => Err(ConnectorError::Fatal(anyhow!(
            "Stream '{stream_name}' is not the stream this pipeline read before: it was created at {}, but the pipeline's checkpoint and journal refer to a stream created {} ns after the Unix epoch, and the stream no longer has the last message the pipeline ingested, which would show whether it holds the same messages. A stream that is deleted and recreated numbers its messages from 1 again, and a restore from a backup can change the creation time, so the recorded sequence numbers may no longer identify the messages the pipeline ingested. To read the stream from the start, reset the connector, for example by changing its configuration",
            info.created,
            expected
        ))),
        Some(_) => Ok(()),
        None => {
            *expected = Some(actual);
            Ok(())
        }
    }
}

/// The part of a stream's state that bounds which sequences it can serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StreamState {
    messages: u64,
    first_sequence: u64,
    last_sequence: u64,
}

impl From<&nats_stream::Info> for StreamState {
    fn from(info: &nats_stream::Info) -> Self {
        Self {
            messages: info.state.messages,
            first_sequence: info.state.first_sequence,
            last_sequence: info.state.last_sequence,
        }
    }
}

/// Checks that reading can continue at `resume_cursor` without skipping or
/// repeating a message.
///
/// `filtered` says whether the connector reads only some subjects.
fn validate_resume_position(
    state: &StreamState,
    stream_name: &str,
    filtered: bool,
    resume_cursor: u64,
) -> Result<(), ConnectorError> {
    if resume_cursor == 0 {
        // Fresh starts use the configured deliver policy.
        return Ok(());
    }
    // An empty stream's next message gets `last_sequence + 1`, whatever
    // `first_sequence` says.
    let first_available = if state.messages == 0 {
        state.last_sequence + 1
    } else {
        state.first_sequence
    };
    let valid_upper = state.last_sequence.saturating_add(1);
    if resume_cursor < first_available {
        let filtered = if filtered {
            " The connector reads only some subjects of the stream and cannot tell whether any of the removed messages was on one of them; it moves its position past other subjects' messages while it runs, but not while the pipeline is stopped or the connector is paused."
        } else {
            ""
        };
        return Err(ConnectorError::Fatal(anyhow!(
            "Resume sequence {resume_cursor} is before earliest available sequence {first_available} for stream '{stream_name}': messages {resume_cursor} to {} were removed (by a retention limit, purge, or delete) before the pipeline read them, so it cannot continue exactly once.{filtered} To continue anyway, reset the connector, for example by changing its configuration",
            first_available - 1
        )));
    }
    if resume_cursor > valid_upper {
        return Err(ConnectorError::Fatal(anyhow!(
            "Resume sequence {resume_cursor} is after valid upper bound {valid_upper} for stream '{stream_name}': the stream no longer has messages the pipeline already read. Was it deleted and recreated?"
        )));
    }
    Ok(())
}

impl InputReader for NatsReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }

    fn take_completion(&self) -> Option<futures::future::BoxFuture<'static, ()>> {
        let worker = self.worker.lock().unwrap().take()?;
        Some(Box::pin(async move {
            if let Err(error) = worker.await {
                warn!("NATS input worker failed during shutdown: {error}");
            }
        }))
    }
}

#[cfg(test)]
mod naming_tests {
    use super::{consumer_name_prefix, sanitize_consumer_name};
    use feldera_types::transport::nats as cfg;
    use proptest::prelude::*;

    fn consumer_config(name: Option<&str>) -> cfg::ConsumerConfig {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "deliver_policy": "All",
        }))
        .unwrap()
    }

    #[test]
    fn sanitize_keeps_valid_names_unchanged() {
        assert_eq!(sanitize_consumer_name("orders"), "orders");
        assert_eq!(sanitize_consumer_name("Orders_v1-a"), "Orders_v1-a");
    }

    #[test]
    fn sanitize_replaces_characters_jetstream_rejects() {
        assert_eq!(sanitize_consumer_name("a.b*c>d/e\\f g"), "a_b_c_d_e_f_g");
        assert_eq!(sanitize_consumer_name("\"Quoted Table\""), "_Quoted_Table_");
        assert_eq!(sanitize_consumer_name("tåble"), "t_ble");
    }

    #[test]
    fn sanitize_falls_back_when_nothing_usable_remains() {
        assert_eq!(sanitize_consumer_name(""), "feldera");
        assert_eq!(sanitize_consumer_name("..."), "feldera");
        assert_eq!(sanitize_consumer_name("___"), "feldera");
    }

    #[test]
    fn prefix_prefers_configured_name_over_table_name() {
        assert_eq!(
            consumer_name_prefix(&consumer_config(Some("my.consumer")), "orders"),
            "my_consumer"
        );
    }

    #[test]
    fn prefix_uses_table_name_when_name_is_absent_or_empty() {
        assert_eq!(
            consumer_name_prefix(&consumer_config(None), "orders"),
            "orders"
        );
        assert_eq!(
            consumer_name_prefix(&consumer_config(Some("")), "orders"),
            "orders"
        );
        assert_eq!(consumer_name_prefix(&consumer_config(None), ""), "feldera");
    }

    proptest! {
        /// Whatever the input, the result is a non-empty name made only of
        /// characters JetStream accepts, and valid names pass through as-is.
        #[test]
        fn sanitized_names_are_always_valid(name in any::<String>()) {
            let sanitized = sanitize_consumer_name(&name);
            prop_assert!(!sanitized.is_empty());
            prop_assert!(sanitized
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'));
            prop_assert_eq!(sanitize_consumer_name(&sanitized), sanitized.clone());
        }

        #[test]
        fn valid_names_are_unchanged(name in "[A-Za-z0-9-][A-Za-z0-9_-]{0,31}") {
            prop_assert_eq!(sanitize_consumer_name(&name), name);
        }
    }
}

#[cfg(test)]
mod create_error_tests {
    use super::create_may_have_succeeded;
    use async_nats::jetstream::stream::ConsumerCreateStrictErrorKind as Kind;

    #[test]
    fn unanswered_or_misread_creates_may_have_succeeded() {
        assert!(create_may_have_succeeded(&Kind::TimedOut));
        assert!(create_may_have_succeeded(&Kind::Request));
        assert!(create_may_have_succeeded(&Kind::Other));
    }

    #[test]
    fn server_rejections_did_not_create_a_consumer() {
        assert!(!create_may_have_succeeded(&Kind::AlreadyExists));
        assert!(!create_may_have_succeeded(&Kind::InvalidName));
        assert!(!create_may_have_succeeded(&Kind::InvalidConsumerType));
    }
}
