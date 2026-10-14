//! Shared log capture for tests that assert on tracing events.
//!
//! One global subscriber is installed once per test binary (`Once`). It writes to a
//! per-thread buffer, so callsite interest is never cached as "disabled" by a parallel
//! test that has no capture, and parallel tests never see each other's events. Use it
//! from current-thread runtime tests only: the buffer belongs to the test thread, and
//! detached tasks of such a test run on that thread.
//!
//! The per-thread buffer cannot see an event emitted on another thread (a `spawn_blocking`
//! thread, a multi-thread worker). [`capture_logs_containing`] covers those: a process-wide
//! capture of the events whose text contains a unique sentinel, from every thread.

use std::cell::RefCell;
use std::io::Write;
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::MakeWriter;

/// Process-wide captures: every event whose text contains the needle goes to its buffer.
static SENTINELS: Mutex<Vec<(String, LogBuffer)>> = Mutex::new(Vec::new());

thread_local! {
    static CURRENT_LOG: RefCell<Option<LogBuffer>> = const { RefCell::new(None) };
}

/// Text written to the log by the capturing thread.
#[derive(Clone, Default)]
pub struct LogBuffer(Arc<Mutex<Vec<u8>>>);

struct RoutedWriter;

impl Write for RoutedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CURRENT_LOG.with(|current| {
            if let Some(buffer) = current.borrow().as_ref() {
                buffer.0.lock().expect("log lock").extend_from_slice(buf);
            }
        });
        let text = String::from_utf8_lossy(buf);
        for (needle, buffer) in SENTINELS.lock().expect("sentinel lock").iter() {
            if text.contains(needle.as_str()) {
                buffer.0.lock().expect("log lock").extend_from_slice(buf);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for RoutedWriter {
    type Writer = RoutedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        RoutedWriter
    }
}

impl LogBuffer {
    /// The whole captured text.
    pub fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log lock").clone()).expect("utf8 log")
    }

    /// The captured lines that contain `message`.
    pub fn lines_with(&self, message: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(message))
            .map(str::to_owned)
            .collect()
    }

    /// Positive control: emits a probe event and asserts that it was captured. Call it
    /// before asserting that an event is absent, so a broken capture cannot make a
    /// negative assertion pass vacuously.
    ///
    /// # Panics
    ///
    /// Panics when the probe event was not captured.
    pub fn assert_live(&self) {
        tracing::warn!("capture_probe");
        assert!(
            !self.lines_with("capture_probe").is_empty(),
            "log capture is not live: the probe event was lost"
        );
    }
}

/// Stops routing to the buffer when dropped.
pub struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CURRENT_LOG.with(|current| *current.borrow_mut() = None);
    }
}

fn install() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(RoutedWriter)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other global tracing subscriber in this test binary");
    });
}

/// The events of EVERY thread whose text contains one sentinel (a process-wide capture).
pub struct SentinelCapture {
    buffer: LogBuffer,
    needle: String,
}

impl SentinelCapture {
    /// The captured text.
    pub fn text(&self) -> String {
        self.buffer.text()
    }

    /// Positive control: a probe event carrying the sentinel is emitted from a blocking-pool
    /// thread and must be captured, so a capture that cannot see other threads cannot make a
    /// negative assertion pass vacuously.
    ///
    /// # Panics
    ///
    /// Panics when the probe was not captured.
    pub async fn assert_live_on_a_blocking_thread(&self) {
        let needle = self.needle.clone();
        tokio::task::spawn_blocking(move || tracing::warn!("blocking_probe {needle}"))
            .await
            .expect("the probe task");
        assert!(
            !self.buffer.lines_with("blocking_probe").is_empty(),
            "the capture does not see events of a blocking thread"
        );
    }
}

/// Stops the process-wide capture when dropped.
pub struct SentinelGuard {
    needle: String,
}

impl Drop for SentinelGuard {
    fn drop(&mut self) {
        SENTINELS
            .lock()
            .expect("sentinel lock")
            .retain(|(needle, _)| *needle != self.needle);
    }
}

/// Starts capturing, from every thread of the process, the events whose text contains
/// `sentinel`. Use a sentinel no other test writes.
///
/// # Panics
///
/// Panics when another global tracing subscriber is already installed in the binary.
pub fn capture_logs_containing(sentinel: &str) -> (SentinelCapture, SentinelGuard) {
    install();
    let buffer = LogBuffer::default();
    SENTINELS
        .lock()
        .expect("sentinel lock")
        .push((sentinel.to_owned(), buffer.clone()));
    (
        SentinelCapture {
            buffer,
            needle: sentinel.to_owned(),
        },
        SentinelGuard {
            needle: sentinel.to_owned(),
        },
    )
}

/// Starts capturing the events of the current thread.
///
/// # Panics
///
/// Panics when another global tracing subscriber is already installed in the binary.
pub fn capture_logs() -> (LogBuffer, CaptureGuard) {
    install();
    let buffer = LogBuffer::default();
    CURRENT_LOG.with(|current| *current.borrow_mut() = Some(buffer.clone()));
    (buffer, CaptureGuard)
}

/// Whether `line` carries `field=value`, with or without quotes around the value.
pub fn has_field(line: &str, field: &str, value: &str) -> bool {
    field_value(line, field).is_some_and(|v| v == value)
}

/// The value of `field` in `line`, without surrounding quotes. Values hold no spaces
/// in the events asserted on.
pub fn field_value(line: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(prefix.as_str()))
        .map(|v| v.trim_matches('"').to_owned())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "log_capture_tests.rs"]
mod log_capture_tests;
