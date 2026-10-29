//! Reading back the messages of a journaled step for replay.
//!
//! A replay must feed the circuit exactly the messages that the step fed it
//! the first time, and nothing else. The connector reads them by stream
//! sequence, with no consumer:
//!
//! - **Batched Direct Get** (ADR-31, nats-server 2.11 and later, streams with
//!   `allow_direct`): one request returns up to [BATCH_MESSAGES] messages of a
//!   subject filter from a start sequence, from any replica.
//! - **Stream leader `MSG.GET`** otherwise, one message per request. It is
//!   also the authority whenever a Direct Get answer looks wrong: a replica
//!   (or a mirror with `mirror_direct`) may lag behind the leader, so a
//!   message missing from its answer is not proof that the message is gone.
//!
//! Compared with replaying through a consumer, as earlier versions did, this
//! creates no server-side state (a restart that replays many steps used to
//! create and delete one consumer per step, each one making the server count
//! the stream's pending messages), cannot overshoot the step (a consumer that
//! the client library recreated before its first message restarted from the
//! beginning of the stream), and cannot wait forever for a message that was
//! deleted. Every message of the step is fetched and checked before any of it
//! reaches the circuit, so a failed attempt can be retried without feeding
//! the circuit the same records twice.

use super::{
    ConnectorError, Metadata, StreamContext, StreamState, check_stream_identity, connect,
    fetch_stream_info,
};
use anyhow::{Result as AnyResult, anyhow, bail};
use async_nats::{
    HeaderMap, StatusCode,
    client::Request,
    jetstream::{
        self,
        message::StreamMessage,
        stream::{LastRawMessageErrorKind, Stream},
    },
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use std::{collections::VecDeque, ops::Range, time::Duration};
use tracing::{debug, warn};

/// Most messages one Direct Get batch request asks for.
pub(super) const BATCH_MESSAGES: u64 = 512;

/// Soft limit on the payload bytes of one Direct Get batch. The server stops
/// the batch after the message that crosses it.
const BATCH_BYTES: usize = 8 * 1024 * 1024;

/// How many single-message requests run at once when a step lists its
/// messages explicitly.
const EXACT_FETCH_CONCURRENCY: usize = 16;

/// One message read back for replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ReplayMessage {
    pub sequence: u64,
    pub payload: Bytes,
}

/// How a request reads the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Batched Direct Get, from any replica.
    Direct,
    /// `STREAM.MSG.GET`, from the stream leader.
    Leader,
}

/// A connection for reading back journaled steps, reused across steps.
pub(super) struct ReplaySource {
    jetstream: jetstream::Context,
    stream: Stream<()>,
    stream_name: String,
    /// The consumer's filter subjects, or `>` for all subjects.
    subjects: Vec<String>,
    timeout: Duration,
    /// Whether the server understands batched Direct Get (2.11 or later).
    server_batches: bool,
}

impl ReplaySource {
    pub async fn connect(ctx: &StreamContext) -> Result<Self, ConnectorError> {
        let jetstream = connect(ctx).await?;
        let stream = jetstream
            .get_stream_no_info(&ctx.stream_name)
            .await
            .map_err(|error| ConnectorError::Fatal(error.into()))?;
        let version = jetstream.client().server_info().version;
        Ok(Self {
            jetstream,
            stream,
            stream_name: ctx.stream_name.clone(),
            subjects: read_subjects(ctx),
            timeout: ctx.request_timeout,
            server_batches: version_at_least(&version, (2, 11)),
        })
    }

    /// Reads exactly the messages of the step that `metadata` describes.
    ///
    /// `stream_created` is the identity of the stream the pipeline read
    /// before, if known; it is recorded if not.
    pub async fn fetch_step(
        &self,
        ctx: &StreamContext,
        metadata: &Metadata,
        stream_created: &mut Option<String>,
    ) -> Result<Vec<ReplayMessage>, ConnectorError> {
        let range = &metadata.sequence_numbers;
        let info = fetch_stream_info(&self.jetstream, &self.stream_name).await?;
        if let Some(recorded) = &metadata.stream_created {
            check_stream_identity(&mut Some(recorded.clone()), &info, &self.stream_name)?;
        }
        check_stream_identity(stream_created, &info, &self.stream_name)?;
        validate_replay_range(&StreamState::from(&info), &self.stream_name, range)?;
        let direct = self.server_batches && info.config.allow_direct;

        if let Some(sequences) = &metadata.sequences {
            return self.fetch_exact(sequences, direct).await;
        }

        if direct {
            match self
                .fetch_range(range, Mode::Direct)
                .await
                .and_then(|messages| {
                    verify_step(range, metadata.messages, &messages)
                        .map(|()| messages)
                        .map_err(ConnectorError::Fatal)
                }) {
                Ok(messages) => return Ok(messages),
                Err(error) => {
                    ctx.metrics.replay_leader_fallback();
                    warn!(
                        "Direct Get did not return sequences {range:?} of stream '{}' intact, reading them from the stream leader instead: {}",
                        self.stream_name,
                        error.inner()
                    );
                }
            }
        }

        let messages = self.fetch_range(range, Mode::Leader).await?;
        verify_step(range, metadata.messages, &messages).map_err(|error| {
            ConnectorError::Fatal(error.context(format!(
                "Cannot replay sequences {range:?} of stream '{}' exactly",
                self.stream_name
            )))
        })?;
        Ok(messages)
    }

    /// Reads the messages with exactly these sequence numbers.
    async fn fetch_exact(
        &self,
        sequences: &[u64],
        direct: bool,
    ) -> Result<Vec<ReplayMessage>, ConnectorError> {
        futures::stream::iter(sequences.iter().copied())
            .map(|sequence| self.fetch_one(sequence, direct))
            .buffered(EXACT_FETCH_CONCURRENCY)
            .try_collect()
            .await
    }

    async fn fetch_one(
        &self,
        sequence: u64,
        direct: bool,
    ) -> Result<ReplayMessage, ConnectorError> {
        if direct {
            match self.direct_batch(">", sequence, 1).await {
                Ok(batch) => {
                    if let Some(message) = batch.messages.into_iter().next()
                        && message.sequence == sequence
                    {
                        return Ok(message);
                    }
                }
                Err(error) => debug!(
                    "Direct Get of sequence {sequence} failed, asking the stream leader: {}",
                    error.inner()
                ),
            }
        }
        match self.stream.get_raw_message(sequence).await {
            Ok(message) => Ok(replay_message(message)),
            Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => {
                Err(ConnectorError::Fatal(anyhow!(
                    "Cannot replay message {sequence} of stream '{}': the stream no longer has it (it was deleted, purged, or removed by a retention limit after the pipeline ingested it)",
                    self.stream_name
                )))
            }
            Err(error) => Err(raw_message_error(error)),
        }
    }

    /// Reads every message in `range` that matches the subject filters, in
    /// stream order.
    ///
    /// Each filter subject is read on its own, from the server's per-subject
    /// index, and the results are merged by sequence. A consumer's filter
    /// subjects never overlap, so no message is read twice.
    async fn fetch_range(
        &self,
        range: &Range<u64>,
        mode: Mode,
    ) -> Result<Vec<ReplayMessage>, ConnectorError> {
        let mut cursors: Vec<Cursor> = self
            .subjects
            .iter()
            .map(|subject| Cursor {
                subject,
                next: range.start,
                buffer: VecDeque::new(),
                done: range.is_empty(),
            })
            .collect();
        let mut messages = Vec::new();
        loop {
            for cursor in &mut cursors {
                if cursor.buffer.is_empty() && !cursor.done {
                    self.refill(cursor, range.end, mode).await?;
                }
            }
            let Some(cursor) = cursors
                .iter_mut()
                .filter(|cursor| !cursor.buffer.is_empty())
                .min_by_key(|cursor| cursor.buffer[0].sequence)
            else {
                break;
            };
            messages.push(cursor.buffer.pop_front().unwrap());
        }
        Ok(messages)
    }

    /// Reads the next messages of `cursor` that come before `end`.
    async fn refill(
        &self,
        cursor: &mut Cursor<'_>,
        end: u64,
        mode: Mode,
    ) -> Result<(), ConnectorError> {
        let fetched = match mode {
            Mode::Direct => {
                self.direct_batch(
                    cursor.subject,
                    cursor.next,
                    (end - cursor.next).min(BATCH_MESSAGES),
                )
                .await?
            }
            Mode::Leader => {
                let message = self.leader_next(cursor.subject, cursor.next).await?;
                Batch {
                    messages: message.into_iter().collect(),
                    more: true,
                }
            }
        };
        if fetched.messages.is_empty() || !fetched.more {
            cursor.done = true;
        }
        for message in fetched.messages {
            if message.sequence < cursor.next {
                return Err(ConnectorError::Fatal(anyhow!(
                    "NATS returned message {} for a read from sequence {}",
                    message.sequence,
                    cursor.next
                )));
            }
            cursor.next = message.sequence + 1;
            if message.sequence >= end {
                cursor.done = true;
                break;
            }
            cursor.buffer.push_back(message);
        }
        if cursor.next >= end {
            cursor.done = true;
        }
        Ok(())
    }

    async fn leader_next(
        &self,
        subject: &str,
        sequence: u64,
    ) -> Result<Option<ReplayMessage>, ConnectorError> {
        leader_next(&self.stream, subject, sequence).await
    }

    /// One batched Direct Get request: up to `count` messages on `subject`
    /// from `start` on.
    ///
    /// The answer is a run of messages, each naming the sequence of the one
    /// before it in `Nats-Last-Sequence`, and an end-of-batch status. Core
    /// NATS does not guarantee delivery, so the chain is checked: a lost
    /// message must fail the request rather than shorten the step.
    async fn direct_batch(
        &self,
        subject: &str,
        start: u64,
        count: u64,
    ) -> Result<Batch, ConnectorError> {
        let request = serde_json::json!({
            "seq": start,
            "batch": count,
            "max_bytes": BATCH_BYTES,
            "next_by_subj": subject,
        });
        let client = self.jetstream.client();
        let read = async {
            let inbox = client.new_inbox();
            let mut replies = client
                .subscribe(inbox.clone())
                .await
                .map_err(|error| ConnectorError::Retryable(error.into()))?;
            self.jetstream
                .send_request(
                    format!("DIRECT.GET.{}", self.stream_name),
                    Request::new()
                        .inbox(inbox)
                        .payload(Bytes::from(request.to_string())),
                )
                .await
                .map_err(|error| ConnectorError::Retryable(error.into()))?;
            let mut decoder = BatchDecoder::new(&self.stream_name, start);
            loop {
                let reply = replies.next().await.ok_or_else(|| {
                    ConnectorError::Retryable(anyhow!(
                        "the connection closed during a Direct Get batch"
                    ))
                })?;
                if let Some(batch) = decoder
                    .push(
                        reply.status,
                        reply.description.as_deref(),
                        reply.headers.as_ref(),
                        reply.payload,
                    )
                    .map_err(|error| {
                        ConnectorError::Fatal(error.context(format!(
                            "invalid Direct Get answer for stream '{}' from sequence {start}",
                            self.stream_name
                        )))
                    })?
                {
                    return Ok(batch);
                }
            }
        };
        tokio::time::timeout(self.timeout, read).await.unwrap_or_else(|_| {
            Err(ConnectorError::Retryable(anyhow!(
                "Direct Get batch from sequence {start} of stream '{}' did not complete within {:?}",
                self.stream_name,
                self.timeout
            )))
        })
    }
}

struct Cursor<'a> {
    subject: &'a str,
    /// The next sequence to read.
    next: u64,
    buffer: VecDeque<ReplayMessage>,
    /// No more messages before the end of the range.
    done: bool,
}

/// The messages of one batch, and whether the subject has more after them.
#[derive(Debug, PartialEq, Eq)]
struct Batch {
    messages: Vec<ReplayMessage>,
    more: bool,
}

/// Checks the replies to one batched Direct Get request.
struct BatchDecoder<'a> {
    stream_name: &'a str,
    start: u64,
    /// Sequence of the last message received, `0` before the first.
    previous: u64,
    messages: Vec<ReplayMessage>,
    /// `Nats-Num-Pending` of the last message.
    pending: Option<u64>,
}

impl<'a> BatchDecoder<'a> {
    fn new(stream_name: &'a str, start: u64) -> Self {
        Self {
            stream_name,
            start,
            previous: 0,
            messages: Vec::new(),
            pending: None,
        }
    }

    /// Takes in one reply. Returns the batch once it is complete.
    fn push(
        &mut self,
        status: Option<StatusCode>,
        description: Option<&str>,
        headers: Option<&HeaderMap>,
        payload: Bytes,
    ) -> AnyResult<Option<Batch>> {
        match status.map_or(200, |status| status.as_u16()) {
            200 => {
                let stream = header(headers, "Nats-Stream")?;
                if stream != self.stream_name {
                    // A mirror with `mirror_direct` answers in the name of
                    // the mirror, and may lag behind the source.
                    bail!("answered by stream '{stream}'");
                }
                let sequence = number(headers, "Nats-Sequence")?;
                let last = number(headers, "Nats-Last-Sequence")?;
                if last != self.previous {
                    bail!(
                        "message {sequence} follows message {last}, but the previous message received was {}: a message of the batch was lost",
                        self.previous
                    );
                }
                if sequence < self.start || sequence <= self.previous {
                    bail!("message {sequence} is out of order");
                }
                self.pending = Some(number(headers, "Nats-Num-Pending")?);
                self.previous = sequence;
                self.messages.push(ReplayMessage { sequence, payload });
                Ok(None)
            }
            204 if description == Some("EOB") => {
                let last = number(headers, "Nats-Last-Sequence")?;
                if last != self.previous {
                    bail!(
                        "the batch ended after message {last}, but the last message received was {}: a message of the batch was lost",
                        self.previous
                    );
                }
                let pending = number(headers, "Nats-Num-Pending")?;
                Ok(Some(Batch {
                    messages: std::mem::take(&mut self.messages),
                    more: pending > 0,
                }))
            }
            404 if self.messages.is_empty() => Ok(Some(Batch {
                messages: Vec::new(),
                more: false,
            })),
            status => bail!(
                "unexpected status {status} ({})",
                description.unwrap_or_default()
            ),
        }
    }
}

fn header<'h>(headers: Option<&'h HeaderMap>, name: &str) -> AnyResult<&'h str> {
    headers
        .and_then(|headers| headers.get_last(name))
        .map(|value| value.as_str())
        .ok_or_else(|| anyhow!("a message lacks the `{name}` header"))
}

fn number(headers: Option<&HeaderMap>, name: &str) -> AnyResult<u64> {
    let value = header(headers, name)?;
    value
        .parse()
        .map_err(|_| anyhow!("header `{name}` is {value:?}, not a number"))
}

/// The subjects the connector reads: its filter subjects, or all of them.
fn read_subjects(ctx: &StreamContext) -> Vec<String> {
    if ctx.filter_subjects.is_empty() {
        vec![">".to_string()]
    } else {
        ctx.filter_subjects.clone()
    }
}

/// The first message at or after `sequence` on `subject`, from the stream
/// leader.
async fn leader_next(
    stream: &Stream<()>,
    subject: &str,
    sequence: u64,
) -> Result<Option<ReplayMessage>, ConnectorError> {
    match stream
        .raw_message_builder()
        .sequence(sequence)
        .next_by_subject(subject)
        .send()
        .await
    {
        Ok(message) => Ok(Some(replay_message(message))),
        Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
        Err(error) => Err(raw_message_error(error)),
    }
}

/// The sequence of the first message from `from` on that the connector
/// reads, or `end` if there is none before `end`, according to the stream
/// leader.
pub(super) async fn first_unread(
    jetstream: &jetstream::Context,
    ctx: &StreamContext,
    from: u64,
    end: u64,
) -> Result<u64, ConnectorError> {
    let stream = jetstream
        .get_stream_no_info(&ctx.stream_name)
        .await
        .map_err(|error| ConnectorError::Fatal(error.into()))?;
    let mut first = end;
    for subject in read_subjects(ctx) {
        if let Some(message) = leader_next(&stream, &subject, from).await? {
            first = first.min(message.sequence);
        }
    }
    Ok(first)
}

fn replay_message(message: StreamMessage) -> ReplayMessage {
    ReplayMessage {
        sequence: message.sequence,
        payload: message.payload,
    }
}

fn raw_message_error(error: jetstream::stream::RawMessageError) -> ConnectorError {
    match error.kind() {
        LastRawMessageErrorKind::JetStream(api_error)
            if super::config_utils::is_permanent_api_error(&api_error) =>
        {
            ConnectorError::Fatal(error.into())
        }
        _ => ConnectorError::Retryable(error.into()),
    }
}

/// Checks that `messages`, read for `range`, are the messages of the step.
///
/// The step's first and last messages delimit the range, so both must be
/// there. `expected` is the step's message count, which metadata written by
/// earlier versions lacks; the controller still compares the replayed
/// records' count and hash with the journal in that case.
fn verify_step(
    range: &Range<u64>,
    expected: Option<u64>,
    messages: &[ReplayMessage],
) -> AnyResult<()> {
    if range.is_empty() {
        return Ok(());
    }
    let found = |messages: &[ReplayMessage]| match (messages.first(), messages.last()) {
        (Some(first), Some(last)) => format!(
            "{} messages, sequences {} to {}",
            messages.len(),
            first.sequence,
            last.sequence
        ),
        _ => "no messages".to_string(),
    };
    let ingested = match expected {
        Some(count) => format!(
            "{count} messages, sequences {} to {}",
            range.start,
            range.end - 1
        ),
        None => format!("sequences {} to {}", range.start, range.end - 1),
    };
    let complete = messages.first().map(|m| m.sequence) == Some(range.start)
        && messages.last().map(|m| m.sequence) == Some(range.end - 1)
        && expected.is_none_or(|count| count == messages.len() as u64);
    if !complete {
        bail!(
            "the pipeline ingested {ingested}, but the stream now has {} of them: messages were deleted, purged, or removed by a retention limit after the pipeline ingested them",
            found(messages)
        );
    }
    Ok(())
}

/// Checks that the stream still holds every sequence of `range`.
pub(super) fn validate_replay_range(
    state: &StreamState,
    stream_name: &str,
    range: &Range<u64>,
) -> Result<(), ConnectorError> {
    if range.is_empty() {
        return Ok(());
    }
    if state.messages == 0 {
        return Err(ConnectorError::Fatal(anyhow!(
            "Replay requested sequences {range:?} from stream '{stream_name}', but the stream is empty"
        )));
    }
    let (first, last) = (range.start, range.end - 1);
    if first < state.first_sequence || first > state.last_sequence {
        return Err(ConnectorError::Fatal(anyhow!(
            "Replay start sequence {first} is outside available stream range [{}, {}] for stream '{stream_name}'",
            state.first_sequence,
            state.last_sequence
        )));
    }
    if last > state.last_sequence {
        return Err(ConnectorError::Fatal(anyhow!(
            "Replay end sequence {last} exceeds available stream tail {} for stream '{stream_name}'",
            state.last_sequence
        )));
    }
    Ok(())
}

/// Whether a `nats-server` version string is at least `major.minor`.
fn version_at_least(version: &str, (major, minor): (u64, u64)) -> bool {
    let mut parts = version
        .trim_start_matches('v')
        .split(|c: char| !c.is_ascii_digit())
        .map(|part| part.parse::<u64>().ok());
    match (parts.next().flatten(), parts.next().flatten()) {
        (Some(have_major), Some(have_minor)) => (have_major, have_minor) >= (major, minor),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(code: u16) -> StatusCode {
        StatusCode::from_u16(code).unwrap()
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, *value);
        }
        headers
    }

    fn data(
        decoder: &mut BatchDecoder,
        sequence: u64,
        last: u64,
        pending: u64,
    ) -> AnyResult<Option<Batch>> {
        decoder.push(
            None,
            None,
            Some(&headers(&[
                ("Nats-Stream", "s"),
                ("Nats-Sequence", &sequence.to_string()),
                ("Nats-Last-Sequence", &last.to_string()),
                ("Nats-Num-Pending", &pending.to_string()),
            ])),
            Bytes::from(format!("m{sequence}")),
        )
    }

    fn eob(decoder: &mut BatchDecoder, last: u64, pending: u64) -> AnyResult<Option<Batch>> {
        decoder.push(
            Some(status(204)),
            Some("EOB"),
            Some(&headers(&[
                ("Nats-Last-Sequence", &last.to_string()),
                ("Nats-Num-Pending", &pending.to_string()),
            ])),
            Bytes::new(),
        )
    }

    fn message(sequence: u64) -> ReplayMessage {
        ReplayMessage {
            sequence,
            payload: Bytes::from(format!("m{sequence}")),
        }
    }

    #[test]
    fn decoder_returns_a_chained_batch() {
        let mut decoder = BatchDecoder::new("s", 3);
        assert_eq!(data(&mut decoder, 3, 0, 5).unwrap(), None);
        assert_eq!(data(&mut decoder, 7, 3, 4).unwrap(), None);
        assert_eq!(
            eob(&mut decoder, 7, 4).unwrap(),
            Some(Batch {
                messages: vec![message(3), message(7)],
                more: true,
            })
        );
    }

    #[test]
    fn decoder_reports_the_end_of_the_subject() {
        let mut decoder = BatchDecoder::new("s", 1);
        data(&mut decoder, 1, 0, 0).unwrap();
        assert!(!eob(&mut decoder, 1, 0).unwrap().unwrap().more);

        let mut decoder = BatchDecoder::new("s", 1);
        let batch = decoder
            .push(
                Some(status(404)),
                Some("Message Not Found"),
                None,
                Bytes::new(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            batch,
            Batch {
                messages: vec![],
                more: false
            }
        );
    }

    /// An empty payload is a message, not the end of the batch.
    #[test]
    fn decoder_keeps_empty_messages() {
        let mut decoder = BatchDecoder::new("s", 1);
        decoder
            .push(
                None,
                None,
                Some(&headers(&[
                    ("Nats-Stream", "s"),
                    ("Nats-Sequence", "1"),
                    ("Nats-Last-Sequence", "0"),
                    ("Nats-Num-Pending", "1"),
                ])),
                Bytes::new(),
            )
            .unwrap();
        data(&mut decoder, 2, 1, 0).unwrap();
        let batch = eob(&mut decoder, 2, 0).unwrap().unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert!(batch.messages[0].payload.is_empty());
    }

    #[test]
    fn decoder_rejects_a_lost_message() {
        let mut decoder = BatchDecoder::new("s", 1);
        data(&mut decoder, 1, 0, 2).unwrap();
        // Message 2 never arrived.
        assert!(data(&mut decoder, 3, 2, 0).is_err());

        let mut decoder = BatchDecoder::new("s", 1);
        data(&mut decoder, 1, 0, 1).unwrap();
        // The batch ended after a message that never arrived.
        assert!(eob(&mut decoder, 2, 0).is_err());

        // The first message was lost.
        let mut decoder = BatchDecoder::new("s", 1);
        assert!(data(&mut decoder, 2, 1, 0).is_err());
    }

    #[test]
    fn decoder_rejects_other_streams_and_statuses() {
        let mut decoder = BatchDecoder::new("other", 1);
        assert!(data(&mut decoder, 1, 0, 0).is_err());

        let mut decoder = BatchDecoder::new("s", 5);
        assert!(data(&mut decoder, 4, 0, 0).is_err(), "before the start");

        let mut decoder = BatchDecoder::new("s", 1);
        assert!(
            decoder
                .push(Some(status(408)), Some("Bad Request"), None, Bytes::new())
                .is_err()
        );

        // A 404 after data means the answer is inconsistent.
        let mut decoder = BatchDecoder::new("s", 1);
        data(&mut decoder, 1, 0, 1).unwrap();
        assert!(
            decoder
                .push(Some(status(404)), None, None, Bytes::new())
                .is_err()
        );
    }

    #[test]
    fn verify_step_accepts_exactly_the_step() {
        let messages = [message(3), message(5), message(9)];
        verify_step(&(3..10), Some(3), &messages).unwrap();
        verify_step(&(3..10), None, &messages).unwrap();
        verify_step(&(4..4), Some(0), &[]).unwrap();
    }

    #[test]
    fn verify_step_rejects_missing_messages() {
        let messages = [message(3), message(5), message(9)];
        // The last message is gone.
        assert!(verify_step(&(3..11), Some(4), &messages).is_err());
        // The first message is gone.
        assert!(verify_step(&(2..10), Some(4), &messages).is_err());
        // A message in between is gone.
        let error = verify_step(&(3..10), Some(4), &messages).unwrap_err();
        assert!(
            error.to_string().contains(
                "ingested 4 messages, sequences 3 to 9, but the stream now has 3 messages, sequences 3 to 9"
            ),
            "{error}"
        );
        assert!(verify_step(&(3..10), Some(3), &[]).is_err());
    }

    #[test]
    fn versions_compare_by_major_and_minor() {
        assert!(version_at_least("2.11.0", (2, 11)));
        assert!(version_at_least("v2.15.2", (2, 11)));
        assert!(version_at_least("3.0.0-beta", (2, 11)));
        assert!(version_at_least("2.12.0-RC.1", (2, 11)));
        assert!(!version_at_least("2.10.24", (2, 11)));
        assert!(!version_at_least("", (2, 11)));
        assert!(!version_at_least("unknown", (2, 11)));
    }
}
