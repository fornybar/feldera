//! File recovery through its public reader protocol and real engine input handles.

use super::{FileInputEndpoint, test::relation};
use crate::{
    format::{LineSplitter, get_input_format},
    test::{engine_test, parser, published_schema},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use feldera_adapterlib::{
    format::{
        BufferSize, MessageOrientedPreprocessedParser, ParseError, Parser, Splitter,
        StreamingPreprocessedParser,
    },
    preprocess::{Preprocessor, PreprocessorWatchdog},
    test::{ConnectorTestEngine, InputOptions, TestCollection, Updates, wait},
    transport::{
        InputConsumer, InputReader, InputReaderCommand, Resume, TransportInputEndpoint, Watermark,
    },
};
use feldera_types::{adapter_stats::ConnectorHealth, config::FtModel};
use serde_json::{Value, json};
use std::{
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tempfile::NamedTempFile;

const TIMEOUT: Duration = Duration::from_secs(10);
const DATA: &[u8] = b"s,b,i\nfirst,true,11\nsecond,false,-37\nthird,true,809\n";
const ROWS: [&str; 3] = [
    r#"{"s":"first","b":true,"i":11}"#,
    r#"{"s":"second","b":false,"i":-37}"#,
    r#"{"s":"third","b":true,"i":809}"#,
];

#[derive(Default)]
struct Progress {
    buffered: AtomicUsize,
    eoi: AtomicBool,
    error: Mutex<Option<(bool, String)>>,
}

#[derive(Clone)]
struct Consumer {
    progress: Arc<Progress>,
    queued: mpsc::Sender<(BufferSize, Resume)>,
    replayed: mpsc::Sender<(BufferSize, u64)>,
}

impl InputConsumer for Consumer {
    fn max_batch_size(&self) -> usize {
        1
    }
    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
    fn parse_errors(&self, errors: Vec<ParseError>) {
        assert!(errors.is_empty(), "{errors:?}");
    }
    fn buffered(&self, amt: BufferSize) {
        self.progress
            .buffered
            .fetch_add(amt.records, Ordering::Release);
    }
    fn replayed(&self, amt: BufferSize, hash: u64) {
        let _ = self.replayed.send((amt, hash));
    }
    fn extended(&self, amt: BufferSize, resume: Option<Resume>, _watermarks: Vec<Watermark>) {
        let _ = self.queued.send((amt, resume.unwrap()));
    }
    fn eoi(&self) {
        self.progress.eoi.store(true, Ordering::Release);
    }
    fn request_step(&self) {}
    fn start_transaction(&self, _label: Option<&str>) {}
    fn commit_transaction(&self) {}
    fn completion_watcher(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<feldera_types::coordination::Completion>> {
        None
    }
    fn error(&self, fatal: bool, error: anyhow::Error, _tag: Option<&'static str>) {
        *self.progress.error.lock().unwrap() = Some((fatal, error.to_string()));
    }
    fn update_connector_health(&self, _health: ConnectorHealth) {}
}

struct Reader {
    reader: Box<dyn InputReader>,
    collection: Box<dyn TestCollection>,
    progress: Arc<Progress>,
    queued: mpsc::Receiver<(BufferSize, Resume)>,
    replayed: mpsc::Receiver<(BufferSize, u64)>,
}

impl Reader {
    fn open(
        engine: &dyn ConnectorTestEngine,
        file: &NamedTempFile,
        buffer_size: usize,
        follow: bool,
        resume: Option<Value>,
        preprocessing: Option<bool>,
    ) -> Self {
        let collection = engine
            .input_collection(&relation(), &InputOptions::default())
            .unwrap();
        let format = serde_json::from_value(json!({
            "name": "csv", "config": { "headers": true, "comment": "#" }
        }))
        .unwrap();
        let parser: Box<dyn Parser> = if let Some(message_oriented) = preprocessing {
            // The header is in a separate base64 message, so raw CSV sniffing cannot work.
            let parser = get_input_format("csv")
                .unwrap()
                .new_parser(
                    "source",
                    &collection.input_handle(),
                    &json!({ "headers": true }),
                )
                .unwrap();
            let watchdog = PreprocessorWatchdog::new(
                &serde_json::from_value(json!({
                    "name": "base64-lines", "config": {}, "message_oriented": message_oriented
                }))
                .unwrap(),
            );
            if message_oriented {
                Box::new(MessageOrientedPreprocessedParser::new(
                    Box::new(Base64Lines),
                    parser,
                    watchdog,
                ))
            } else {
                Box::new(StreamingPreprocessedParser::new(
                    Box::new(Base64Lines),
                    parser,
                    watchdog,
                ))
            }
        } else {
            Box::new(parser(collection.as_ref(), &format))
        };
        let (queued_tx, queued) = mpsc::channel();
        let (replayed_tx, replayed) = mpsc::channel();
        let progress = Arc::new(Progress::default());
        let reader = FileInputEndpoint::new(
            serde_json::from_value(json!({
                "path": file.path(), "buffer_size_bytes": buffer_size, "follow": follow
            }))
            .unwrap(),
        )
        .open(
            Box::new(Consumer {
                progress: progress.clone(),
                queued: queued_tx,
                replayed: replayed_tx,
            }),
            parser,
            relation(),
            resume,
        )
        .unwrap();
        Self {
            reader,
            collection,
            progress,
            queued,
            replayed,
        }
    }

    fn queue(&self) -> (BufferSize, Resume) {
        self.reader.queue(false);
        self.queued.recv_timeout(TIMEOUT).unwrap()
    }

    fn read_to_end(&self) {
        self.reader.extend();
        wait(|| self.progress.eoi.load(Ordering::Acquire), 10_000).unwrap();
        assert!(self.progress.error.lock().unwrap().is_none());
    }

    fn assert_rows(&self, rows: &[&str]) {
        let expected = Updates::from_json(
            published_schema(&relation()),
            &rows.iter().map(|row| (1, *row)).collect::<Vec<_>>(),
        )
        .unwrap();
        let actual = self.collection.updates().unwrap();
        println!(
            "File recovery actual={:?}, expected={:?}",
            actual.zset().unwrap(),
            expected.zset().unwrap()
        );
        assert_eq!(actual, expected);
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.reader.disconnect();
        wait(|| self.reader.is_closed(), 10_000).unwrap();
    }
}

struct Base64Lines;
impl Preprocessor for Base64Lines {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        (
            BASE64
                .decode(data.strip_suffix(b"\n").unwrap_or(data))
                .unwrap(),
            vec![],
        )
    }
    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Self)
    }
    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(LineSplitter))
    }
}

#[engine_test]
pub fn csv_resume_before_read_ahead(engine: &dyn ConnectorTestEngine) {
    for buffer_size in [1, 8192] {
        let file = NamedTempFile::new().unwrap();
        file.as_file().write_all(DATA).unwrap();
        let original = Reader::open(engine, &file, buffer_size, false, None, None);
        original.read_to_end();
        assert_eq!(original.progress.buffered.load(Ordering::Acquire), 3);
        let (amt, first) = original.queue();
        assert_eq!(amt.records, 1);
        original.assert_rows(&ROWS[..1]);
        drop(original);

        let restored = Reader::open(engine, &file, buffer_size, false, first.into_seek(), None);
        restored.read_to_end();
        while restored.queue().0.records != 0 {}
        restored.assert_rows(&ROWS[1..]);
    }
}

#[engine_test]
pub fn csv_replay_recorded_ranges(engine: &dyn ConnectorTestEngine) {
    for buffer_size in [1, 8192] {
        let file = NamedTempFile::new().unwrap();
        file.as_file().write_all(DATA).unwrap();
        let original = Reader::open(engine, &file, buffer_size, false, None, None);
        let zero = original.queue();
        assert_eq!(zero.0.records, 0);
        original.read_to_end();
        let ranges: Vec<_> = (0..4).map(|_| original.queue()).collect();
        assert_eq!(
            ranges
                .iter()
                .map(|entry| entry.0.records)
                .collect::<Vec<_>>(),
            [1, 1, 1, 0]
        );
        original.assert_rows(&ROWS);
        drop(original);

        let restored = Reader::open(engine, &file, buffer_size, false, None, None);
        // Empty at zero; then an empty range at EOF, a backwards gap, and contiguous ranges.
        // Every range came from Queue, rather than a hand-constructed byte offset.
        for (entry, rows) in [
            (&zero, &ROWS[0..0]),
            (&ranges[3], &ROWS[0..0]),
            (&ranges[1], &ROWS[1..2]),
            (&ranges[2], &ROWS[2..3]),
            (&ranges[3], &ROWS[0..0]),
        ] {
            let Resume::Replay { seek, replay, hash } = &entry.1 else {
                panic!("File must support replay")
            };
            restored.reader.request(InputReaderCommand::Replay {
                metadata: seek.clone(),
                data: replay.clone(),
            });
            let (amt, actual_hash) = restored.replayed.recv_timeout(TIMEOUT).unwrap();
            println!(
                "File replay range={seek}, records={}, hash={actual_hash}, expected_hash={hash}",
                amt.records
            );
            assert_eq!(amt.records, entry.0.records);
            assert_eq!(actual_hash, *hash);
            restored.assert_rows(rows);
            restored.collection.reset();
        }
        file.as_file()
            .write_all(b"continuation,false,997\n")
            .unwrap();
        restored.read_to_end();
        assert_eq!(restored.queue().0.records, 1);
        assert_eq!(restored.queue().0.records, 0);
        restored.assert_rows(&[r#"{"s":"continuation","b":false,"i":997}"#]);
    }
}

fn empty_prefix(engine: &dyn ConnectorTestEngine, prefix: &[u8], pending_header: bool) {
    for buffer_size in [1, 8192] {
        let file = NamedTempFile::new().unwrap();
        file.as_file().write_all(prefix).unwrap();
        let original = Reader::open(engine, &file, buffer_size, false, None, None);
        original.read_to_end();
        let (amt, anchor) = original.queue();
        assert_eq!(amt.records, 0);
        assert_eq!(anchor.seek().unwrap()["offsets"]["end"], prefix.len());
        drop(original);
        if pending_header {
            // Valid-looking data makes an incorrectly retained header observable.
            file.as_file().write_all(b"not-data,true,501\n").unwrap();
        }
        file.as_file().write_all(b"first,false,-37\n").unwrap();
        let restored = Reader::open(engine, &file, buffer_size, true, anchor.into_seek(), None);
        restored.reader.extend();
        // Only one data row exists: a stale header splitter cannot wait for a second row.
        wait(
            || restored.progress.buffered.load(Ordering::Acquire) > 0,
            10_000,
        )
        .unwrap();
        assert_eq!(restored.queue().0.records, 1);
        restored.assert_rows(&[r#"{"s":"first","b":false,"i":-37}"#]);
    }
}

#[engine_test]
pub fn csv_resume_after_header_only(engine: &dyn ConnectorTestEngine) {
    empty_prefix(engine, b"s,b,i\n", false);
    empty_prefix(engine, b"s,b,i", false);
}

#[engine_test]
pub fn csv_resume_before_header(engine: &dyn ConnectorTestEngine) {
    empty_prefix(engine, b"", true);
    empty_prefix(engine, b"# comment only\n", true);
}

#[engine_test]
pub fn csv_resume_preprocessed_prefix(engine: &dyn ConnectorTestEngine) {
    for message_oriented in [true, false] {
        let file = NamedTempFile::new().unwrap();
        // Header-only decoded message deliberately has no newline.
        for row in [b"s,b,i".as_slice(), b"first,true,11\n"] {
            writeln!(file.as_file(), "{}", BASE64.encode(row)).unwrap();
        }
        let original = Reader::open(engine, &file, 1, false, None, Some(message_oriented));
        original.read_to_end();
        let (amt, anchor) = original.queue();
        assert_eq!(amt.records, 1);
        original.assert_rows(&ROWS[..1]);
        drop(original);
        writeln!(file.as_file(), "{}", BASE64.encode(b"second,false,-37\n")).unwrap();
        let restored = Reader::open(
            engine,
            &file,
            1,
            false,
            anchor.into_seek(),
            Some(message_oriented),
        );
        restored.read_to_end();
        while restored.queue().0.records != 0 {}
        restored.assert_rows(&ROWS[1..2]);
    }
}

#[engine_test]
pub fn csv_resume_rejects_truncated_prefix(engine: &dyn ConnectorTestEngine) {
    let file = NamedTempFile::new().unwrap();
    file.as_file().write_all(DATA).unwrap();
    let original = Reader::open(engine, &file, 1, false, None, None);
    original.read_to_end();
    let (_, anchor) = original.queue();
    drop(original);
    file.as_file().set_len(1).unwrap();
    let restored = Reader::open(engine, &file, 1, false, anchor.into_seek(), None);
    restored.reader.extend();
    wait(
        || restored.reader.is_closed() || restored.progress.eoi.load(Ordering::Acquire),
        10_000,
    )
    .unwrap();
    assert!(restored.collection.is_empty());
    let error = restored.progress.error.lock().unwrap();
    println!("File truncated prefix error={error:?}");
    assert!(
        error
            .as_ref()
            .is_some_and(|(fatal, error)| *fatal && error.contains("truncated"))
    );
}
