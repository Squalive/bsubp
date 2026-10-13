mod backup;
mod list;
mod pack;
mod restore;

use aws_config::BehaviorVersion;
use aws_sdk_s3::{self as s3};
use clap::Parser;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle, TermLike};
use std::io::{IsTerminal, Write};
use std::sync::Mutex;
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const BUCKET_NAME: &str = "bs-private-backup";

/// How often a non-terminal progress bar may emit a log line. Progress bars
/// normally repaint in place 20 times a second; writing that into a CI log
/// would be both unreadable and enormous, so non-terminal output is capped.
const CI_REFRESH: Duration = Duration::from_millis(200);
/// Width assumed when there is no terminal to measure.
///
/// This must be comfortably wider than the rendered template: `indicatif` pads
/// each line up to this width and computes `width - used`, so reporting too
/// small a width panics inside its draw, and the panic is swallowed, leaving
/// no output at all.
const LOG_LINE_WIDTH: u16 = 200;

/// Handle that configures progress rendering for the current environment.
#[derive(Clone)]
pub struct Progress {
    mode: Mode,
}

#[derive(Clone, Copy)]
enum Mode {
    /// A real terminal: repaint the bar in place.
    Terminal,
    /// Not a terminal (CI, `| tee`, redirected logs): append one line per
    /// update instead of emitting cursor-control escape codes.
    Log,
}

impl Progress {
    pub fn detect() -> Self {
        let mode = if std::io::stderr().is_terminal() {
            Mode::Terminal
        } else {
            Mode::Log
        };
        Self { mode }
    }

    fn bar(&self, total: u64, template: &str, message: &str) -> ProgressBar {
        let (template, target) = match self.mode {
            Mode::Terminal => (template.to_string(), ProgressDrawTarget::stderr()),
            Mode::Log => (
                // A trailing newline is what makes each update its own log line.
                format!("{template}\n"),
                ProgressDrawTarget::term_like_with_hz(Box::new(LogWriter::stderr()), refresh_hz()),
            ),
        };

        let bar =
            ProgressBar::with_draw_target(if total == 0 { None } else { Some(total) }, target);
        bar.set_style(
            ProgressStyle::with_template(&template)
                .unwrap()
                .progress_chars("=>-"),
        );
        if !message.is_empty() {
            bar.set_message(message.to_string());
        }

        // A steady tick keeps the log moving even when nothing else triggers a
        // draw; it also animates the spinner.
        if matches!(self.mode, Mode::Log) {
            bar.enable_steady_tick(CI_REFRESH);
        }
        bar
    }

    /// Progress bar measured in items (files, parts).
    pub fn bar_items(&self, total: u64, message: &str) -> ProgressBar {
        self.bar(
            total,
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
            message,
        )
    }

    /// Progress bar measured in bytes.
    pub fn bar_bytes(&self, total: u64, message: &str) -> ProgressBar {
        self.bar(
            total,
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}",
            message,
        )
    }
}

/// Refresh rate for non-terminal output, in updates per second (at least 1).
fn refresh_hz() -> u8 {
    (1000 / CI_REFRESH.as_millis().max(1)).clamp(1, 255) as u8
}

/// A [`TermLike`] that renders progress as plain, newline-terminated lines.
///
/// Progress bars are hidden by default when stderr is not a terminal, which is
/// exactly the case in CI, so nothing would reach the job log. This target
/// keeps updates visible there without writing escape sequences that would end
/// up as literal garbage in a log file.
struct LogWriter<W: Write + Send + Sync> {
    /// The line currently being composed. `indicatif` paints one line with
    /// `clear_line` + `write_str` (+ `\r`) and a newline, so buffering here
    /// turns one repaint into one clean log line instead of several fragments.
    buffer: Mutex<String>,
    sink: Mutex<W>,
}

/// `TermLike` requires `Debug`, but a sink need not implement it.
impl<W: Write + Send + Sync> std::fmt::Debug for LogWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogWriter").finish_non_exhaustive()
    }
}

impl<W: Write + Send + Sync> LogWriter<W> {
    fn new(sink: W) -> Self {
        Self {
            buffer: Mutex::new(String::new()),
            sink: Mutex::new(sink),
        }
    }

    fn emit(&self, line: &str) {
        if let Ok(mut sink) = self.sink.lock() {
            let _ = writeln!(sink, "{line}");
        }
    }
}

impl LogWriter<std::io::Stderr> {
    fn stderr() -> Self {
        Self::new(std::io::stderr())
    }
}

impl<W: Write + Send + Sync> TermLike for LogWriter<W> {
    fn width(&self) -> u16 {
        LOG_LINE_WIDTH
    }

    fn height(&self) -> u16 {
        u16::MAX
    }

    // Log output is append-only, so there is no cursor to move.
    fn move_cursor_up(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_down(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_right(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_left(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn write_line(&self, s: &str) -> std::io::Result<()> {
        let line = {
            let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
            if buffer.is_empty() {
                s.to_string()
            } else {
                if !s.is_empty() {
                    buffer.push_str(s);
                }
                std::mem::take(&mut *buffer)
            }
        };
        self.emit(&line);
        Ok(())
    }

    /// Defer the line until the repaint completes, so one repaint produces one
    /// log line instead of a fragment per call.
    fn write_str(&self, s: &str) -> std::io::Result<()> {
        let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        buffer.push_str(s);
        Ok(())
    }

    fn clear_line(&self) -> std::io::Result<()> {
        let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        buffer.clear();
        Ok(())
    }

    /// `indicatif` finishes every repaint with `flush`, which is therefore
    /// where a plain `TermLike` target must publish its line. It never calls
    /// `write_line` on this kind of target, so relying on that would silently
    /// emit nothing at all.
    fn flush(&self) -> std::io::Result<()> {
        let buffered = {
            let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *buffer)
        };

        // Progress lines are one visual line; drop the carriage return the
        // renderer uses to return to column zero.
        let line = buffered.trim_end_matches(['\r', '\n']);
        if !line.trim().is_empty() {
            self.emit(line);
        }

        let mut sink = self.sink.lock().unwrap_or_else(|e| e.into_inner());
        sink.flush()
    }
}

#[derive(clap::Parser)]
#[command(
    name = "bs_unlicensed_backup",
    version,
    about = "Backup private banana shooter unlicensed assets to cloudflare"
)]
struct Args {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Clone, clap::Subcommand)]
enum Commands {
    /// Backup a new snapshot to the cloud
    Backup,
    /// List available snapshots from cloud
    List,
    /// Restore a snapshot from the cloud
    Restore {
        /// Snapshot ID; if omitted, you'll be prompted to pick one
        id: Option<String>,
        #[arg(short, long)]
        confirm: bool,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    // Decided once: a piped run (CI log capture, `| tee`) gets line-oriented
    // progress instead of in-place repaints.
    let progress = Progress::detect();

    let client = build_client().await;

    match args.commands {
        Commands::Backup => {
            if let Err(e) = backup::run(&client, &progress).await {
                tracing::error!(error = %e, "Failed to backup unlicensed private snapshot");
            }
        }
        Commands::List => {
            if let Err(e) = list::run(&client).await {
                tracing::error!(error = %e, "Failed to list snapshots")
            }
        }
        Commands::Restore { id, confirm } => {
            if let Err(e) = restore::run(&client, id, !confirm, &progress).await {
                tracing::error!(error = %e, "Failed to restore snapshot")
            }
        }
    }
}

async fn build_client() -> s3::Client {
    let account_id = std::env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID must be set");
    let access_key_id = std::env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID must be set");
    let secret_access_key =
        std::env::var("R2_SECRET_ACCESS_KEY").expect("R2_SECRET_ACCESS_KEY must be set");

    // Configure the R2 endpoint
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!("https://{}.r2.cloudflarestorage.com", account_id))
        .credentials_provider(s3::config::Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "R2",
        ))
        .region("auto")
        .load()
        .await;

    s3::Client::new(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Shared handle so a test can read back what the writer emitted.
    #[derive(Clone, Debug)]
    struct SharedSink(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn writer() -> (LogWriter<SharedSink>, SharedSink) {
        let sink = SharedSink(Arc::new(Mutex::new(Vec::new())));
        (LogWriter::new(sink.clone()), sink)
    }

    fn emitted(sink: &SharedSink) -> String {
        String::from_utf8(sink.0.lock().unwrap().clone()).unwrap()
    }

    /// A repaint is `clear_line` + `write_str` calls, so those fragments must
    /// collapse into exactly one log line per update.
    #[test]
    fn log_writer_emits_one_line_per_repaint() {
        let (writer, sink) = writer();

        // First repaint, the way indicatif paints: clear, then fragments.
        writer.clear_line().unwrap();
        writer.write_str("[=>-] 1/2 a.png").unwrap();
        writer.write_str("\r").unwrap();
        writer.write_line("").unwrap();

        // Second repaint, completing the bar.
        writer.clear_line().unwrap();
        writer.write_str("[==>] 2/2 b.png").unwrap();
        writer.write_line("").unwrap();
        writer.flush().unwrap();

        let output = emitted(&sink);
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "expected one line per repaint, got {output:?}"
        );
        assert!(
            lines[0].starts_with("[=>-] 1/2 a.png"),
            "got {:?}",
            lines[0]
        );
        assert!(
            lines[1].starts_with("[==>] 2/2 b.png"),
            "got {:?}",
            lines[1]
        );
    }

    /// Cursor control must not emit escape codes into a plain log.
    #[test]
    fn log_writer_never_emits_escape_sequences() {
        let (writer, sink) = writer();

        writer.move_cursor_up(3).unwrap();
        writer.move_cursor_down(1).unwrap();
        writer.move_cursor_left(2).unwrap();
        writer.move_cursor_right(4).unwrap();
        writer.write_str("progress").unwrap();
        writer.write_line("").unwrap();

        let output = emitted(&sink);
        assert!(!output.contains('\u{1b}'), "output had escapes: {output:?}");
        assert_eq!(output.trim_end(), "progress");
    }

    /// A draw target that reports zero height would make indicatif truncate
    /// every bar, so the log target must report usable dimensions.
    #[test]
    fn log_writer_reports_usable_dimensions() {
        let (writer, _sink) = writer();
        assert!(writer.width() > 0);
        assert!(writer.height() > 1);
    }

    /// The end-to-end claim: drive a real `ProgressBar` on this target and
    /// confirm it actually renders, since indicatif's own stderr target would
    /// silently hide the bar here (tests never have a terminal).
    #[test]
    fn progress_bar_renders_when_stderr_is_not_a_terminal() {
        let (writer, sink) = writer();
        let target = ProgressDrawTarget::term_like_with_hz(Box::new(writer), 20);
        let bar = ProgressBar::with_draw_target(Some(5), target);
        bar.set_style(
            ProgressStyle::with_template("{pos}/{len} {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        bar.set_message("part 0000");

        bar.set_position(3);
        bar.force_draw();
        bar.set_position(5);
        bar.finish_with_message("parts downloaded");

        let output = emitted(&sink);
        assert!(
            !output.is_empty(),
            "no progress output reached the log; the bar was likely hidden"
        );
        assert!(
            output.contains("part 0000"),
            "message missing from log output: {output:?}"
        );
        assert!(
            output.contains("5/5"),
            "final state missing from log output: {output:?}"
        );
        assert!(!output.contains('\u{1b}'), "escapes leaked: {output:?}");
    }

    /// The rate limit is what keeps a CI log readable; it must stay capped.
    #[test]
    fn log_refresh_rate_is_bounded() {
        assert_eq!(refresh_hz(), 5);
        assert!(
            CI_REFRESH.as_millis() >= 100,
            "log updates must be throttled"
        );
    }
}
