use std::{
    fmt::Write,
    fs::File,
    io::Write as IoWrite,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use indicatif::{
    HumanBytes,
    HumanDuration,
    MultiProgress,
    ProgressBar,
    ProgressDrawTarget,
    ProgressState,
    ProgressStyle,
};
use once_cell::sync::{Lazy, OnceCell};
use serde::Serialize;
use tracing::warn;

use crate::{get_done, util::printable_base10_digits, Verbosity};

const PROGRESS_CHARS: &str = if cfg!(windows) {
    "█▓▒░  "
} else {
    "█▉▊▋▌▍▎▏  "
};

const INDICATIF_PROGRESS_TEMPLATE: &str = "{elapsed_precise:.bold} \
                                           {prefix}▐{wide_bar:.blue/white.dim}▌ {percent:.bold} \
                                           {pos} ({fps:.bold}, eta {fixed_eta}{msg})";

const INDICATIF_SC_SPINNER_TEMPLATE: &str =
    "{elapsed_precise:.bold} [{wide_bar:.blue/white.dim}]  {pos} frames ({fps:.bold})";

static PROGRESS_BAR: OnceCell<ProgressBar> = OnceCell::new();
static AUDIO_BYTES: OnceCell<u64> = OnceCell::new();
static PROGRESS_JSONL: Lazy<Mutex<Option<ProgressJsonl>>> = Lazy::new(|| Mutex::new(None));

struct ProgressJsonl {
    file:           File,
    delay:          Duration,
    last_emit:      Option<Instant>,
    start:          Instant,
    total_frames:   usize,
    resume_frames:  u64,
    chunks_total:   u32,
    pos:            u64,
    kbps:           f64,
    est_size_bytes: Option<u64>,
    failed:         bool,
    finished:       bool,
}

#[derive(Serialize)]
struct ProgressJsonlEvent {
    event:          &'static str,
    ts:             f64,
    percent:        f64,
    pos:            u64,
    total:          u64,
    fps:            f64,
    eta:            String,
    kbps:           f64,
    est_size:       Option<String>,
    est_size_bytes: Option<u64>,
    elapsed:        String,
    chunks_done:    u32,
    chunks_total:   u32,
}

fn round_to(value: f64, places: i32) -> f64 {
    let factor = 10_f64.powi(places);
    (value * factor).round() / factor
}

fn unix_timestamp_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}

fn format_eta(remaining_frames: u64, fps: f64) -> String {
    if remaining_frames == 0 {
        return "0s".to_string();
    }

    if fps <= f64::EPSILON {
        return "unknown".to_string();
    }

    format!(
        "{:#}",
        HumanDuration(Duration::from_secs_f64(remaining_frames as f64 / fps))
    )
}

impl ProgressJsonl {
    fn emit(&mut self, force: bool) {
        if self.failed {
            return;
        }

        let now = Instant::now();
        if !force
            && let Some(last_emit) = self.last_emit
            && now.duration_since(last_emit) < self.delay
        {
            return;
        }

        self.last_emit = Some(now);

        let total = self.total_frames as u64;
        let pos = self.pos.min(total);
        let encoded_since_resume = pos.saturating_sub(self.resume_frames);
        let elapsed = self.start.elapsed().as_secs_f64();
        let fps = if encoded_since_resume == 0 || elapsed <= f64::EPSILON {
            0.0
        } else {
            encoded_since_resume as f64 / elapsed
        };

        let remaining = total.saturating_sub(pos);
        let event = ProgressJsonlEvent {
            event: "progress",
            ts: unix_timestamp_secs(),
            percent: if total == 0 {
                0.0
            } else {
                round_to(pos as f64 * 100.0 / total as f64, 1)
            },
            pos,
            total,
            fps: round_to(fps, 2),
            eta: format_eta(remaining, fps),
            kbps: round_to(self.kbps, 1),
            est_size: self.est_size_bytes.map(|bytes| HumanBytes(bytes).to_string()),
            est_size_bytes: self.est_size_bytes,
            elapsed: format!("{:#}", HumanDuration(self.start.elapsed())),
            chunks_done: get_done().done.len() as u32,
            chunks_total: self.chunks_total,
        };

        let write_result = (|| -> anyhow::Result<()> {
            serde_json::to_writer(&mut self.file, &event)?;
            writeln!(&mut self.file)?;
            self.file.flush()?;
            Ok(())
        })();

        if let Err(e) = write_result {
            self.failed = true;
            warn!("Failed to write progress JSONL event: {e}");
        }
    }
}

pub fn init_progress_jsonl(
    path: &Path,
    delay_secs: f64,
    _frame_rate: f64,
    total_frames: usize,
    resume_frames: u64,
    chunks: (u32, u32),
) -> anyhow::Result<()> {
    let file = File::create(path)?;
    let mut state = ProgressJsonl {
        file,
        delay: Duration::from_secs_f64(delay_secs),
        last_emit: None,
        start: Instant::now(),
        total_frames,
        resume_frames,
        chunks_total: chunks.1,
        pos: resume_frames,
        kbps: 0.0,
        est_size_bytes: None,
        failed: false,
        finished: false,
    };
    state.emit(true);

    *PROGRESS_JSONL.lock().expect("mutex should acquire lock") = Some(state);
    Ok(())
}

fn update_progress_jsonl_pos(pos: u64, force: bool) {
    if let Some(jsonl) = PROGRESS_JSONL.lock().expect("mutex should acquire lock").as_mut() {
        jsonl.pos = pos;
        jsonl.emit(force);
    }
}

fn inc_progress_jsonl(inc: u64) {
    if let Some(jsonl) = PROGRESS_JSONL.lock().expect("mutex should acquire lock").as_mut() {
        jsonl.pos = jsonl.pos.saturating_add(inc);
        jsonl.emit(false);
    }
}

pub fn inc_jsonl_progress(inc: u64) {
    inc_progress_jsonl(inc);
}

fn dec_progress_jsonl(dec: u64) {
    if let Some(jsonl) = PROGRESS_JSONL.lock().expect("mutex should acquire lock").as_mut() {
        jsonl.pos = jsonl.pos.saturating_sub(dec);
        jsonl.emit(false);
    }
}

fn update_progress_jsonl_estimates(
    completed_frames: usize,
    kbps: f64,
    est_size_bytes: u64,
    force: bool,
) {
    if let Some(jsonl) = PROGRESS_JSONL.lock().expect("mutex should acquire lock").as_mut() {
        jsonl.kbps = kbps;
        jsonl.est_size_bytes = Some(est_size_bytes);
        jsonl.pos = jsonl.pos.max(completed_frames as u64);
        jsonl.emit(force);
    }
}

fn finish_progress_jsonl() {
    if let Some(jsonl) = PROGRESS_JSONL.lock().expect("mutex should acquire lock").as_mut() {
        if jsonl.finished {
            return;
        }
        jsonl.emit(true);
        jsonl.finished = true;
    }
}

pub fn set_audio_size(val: u64) {
    AUDIO_BYTES.get_or_init(|| val);
}

pub fn get_audio_size() -> u64 {
    *AUDIO_BYTES.get().unwrap_or(&0u64)
}

#[allow(clippy::unwrap_used, reason = "many unwraps on `write!` to terminal")]
fn pretty_progress_style(resume_frames: u64) -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(INDICATIF_PROGRESS_TEMPLATE)
        .expect("template is valid")
        .with_key("fps", move |state: &ProgressState, w: &mut dyn Write| {
            let resume_pos = if state.pos() < resume_frames {
                resume_frames
            } else {
                state.pos() - resume_frames
            };
            if resume_pos == 0 || state.elapsed().as_secs_f32() < f32::EPSILON {
                write!(w, "0 fps").unwrap();
            } else {
                let fps = resume_pos as f32 / state.elapsed().as_secs_f32();
                if fps < 1.0 {
                    write!(w, "{:.2} s/fr", 1.0 / fps).unwrap();
                } else {
                    write!(w, "{fps:.2} fps").unwrap();
                }
            }
        })
        .with_key(
            "fixed_eta",
            move |state: &ProgressState, w: &mut dyn Write| {
                let resume_pos = if state.pos() < resume_frames {
                    resume_frames
                } else {
                    state.pos() - resume_frames
                };
                if resume_pos == 0 || state.elapsed().as_secs_f32() < f32::EPSILON {
                    write!(w, "unknown").unwrap();
                } else {
                    let spf = state.elapsed().as_secs_f32() / resume_pos as f32;
                    let remaining = state.len().unwrap_or(0) - state.pos();
                    write!(
                        w,
                        "{:#}",
                        HumanDuration(Duration::from_secs_f32(spf * remaining as f32))
                    )
                    .unwrap();
                }
            },
        )
        .with_key("pos", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{}/{}", state.pos(), state.len().unwrap_or(0)).unwrap();
        })
        .with_key("percent", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:>3.0}%", state.fraction() * 100_f32).unwrap();
        })
        .progress_chars(PROGRESS_CHARS)
}

#[allow(clippy::unwrap_used, reason = "many unwraps on `write!` to terminal")]
fn spinner_style(resume_frames: u64) -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template(INDICATIF_SC_SPINNER_TEMPLATE)
        .expect("template is valid")
        .with_key("fps", move |state: &ProgressState, w: &mut dyn Write| {
            let resume_pos = if state.pos() < resume_frames {
                resume_frames
            } else {
                state.pos() - resume_frames
            };
            if resume_pos == 0 || state.elapsed().as_secs_f32() < f32::EPSILON {
                write!(w, "0 fps").unwrap();
            } else {
                let fps = resume_pos as f32 / state.elapsed().as_secs_f32();
                if fps < 1.0 {
                    write!(w, "{:.2} s/fr", 1.0 / fps).unwrap();
                } else {
                    write!(w, "{fps:.2} fps",).unwrap();
                }
            }
        })
        .with_key("pos", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{}", state.pos()).unwrap();
        })
        .progress_chars(PROGRESS_CHARS)
}

/// Initialize progress bar
/// Enables steady 100 ms tick
pub fn init_progress_bar(len: u64, resume_frames: u64, chunks: Option<(u32, u32)>) {
    let pb = if len > 0 {
        PROGRESS_BAR
            .get_or_init(|| ProgressBar::new(len).with_style(pretty_progress_style(resume_frames)))
    } else {
        // Avoid showing `xxx/0` if we don't know the length yet.
        // Affects scenechange progress.
        PROGRESS_BAR.get_or_init(|| ProgressBar::new(len).with_style(spinner_style(resume_frames)))
    };
    pb.set_draw_target(ProgressDrawTarget::stderr());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.reset();
    pb.reset_eta();
    pb.reset_elapsed();
    pb.set_position(0);
    if let Some((done, chunks)) = chunks {
        pb.set_prefix(format!("[{done}/{chunks} Chunks] "));
    }
}

pub fn convert_to_progress(resume_frames: u64) {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.set_style(pretty_progress_style(resume_frames));
    }
}

pub fn inc_bar(inc: u64) {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.inc(inc);
    }
    inc_progress_jsonl(inc);
}

pub fn dec_bar(dec: u64) {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.set_position(pb.position().saturating_sub(dec));
    }

    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get() {
        let pb = pbs.last().expect("at least one progress bar exists");
        pb.set_position(pb.position().saturating_sub(dec));
    }

    dec_progress_jsonl(dec);
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "https://github.com/rust-lang/rust-clippy/issues/12786"
)]
pub fn update_bar_info(kbps: f64, est_size: HumanBytes, chunks: Option<(u32, u32)>) {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.set_message(format!(", {kbps:.1} Kbps, est. {est_size}"));
        if let Some((done, chunks)) = chunks {
            pb.set_prefix(format!("[{done}/{chunks} Chunks] "));
        }
    }
}

pub fn set_pos(pos: u64) {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.set_position(pos);
    }
    update_progress_jsonl_pos(pos, false);
}

pub fn finish_progress_bar() {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.finish();
    }

    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get() {
        for pb in pbs {
            pb.finish();
        }
    }

    finish_progress_jsonl();
}

static MULTI_PROGRESS_BAR: OnceCell<(MultiProgress, Vec<ProgressBar>)> = OnceCell::new();

pub fn set_len(len: u64) {
    let pb = PROGRESS_BAR.get().expect("progress bar exists");
    pb.set_length(len);
}

pub fn reset_bar_at(pos: u64) {
    if let Some(pb) = PROGRESS_BAR.get() {
        pb.reset();
        pb.set_position(pos);
        pb.reset_eta();
        pb.reset_elapsed();
    }
    update_progress_jsonl_pos(pos, false);
}

pub fn reset_mp_bar_at(pos: u64) {
    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get()
        && let Some(pb) = pbs.last()
    {
        pb.reset();
        pb.set_position(pos);
        pb.reset_eta();
        pb.reset_elapsed();
    }
    update_progress_jsonl_pos(pos, false);
}

pub fn init_multi_progress_bar(len: u64, workers: usize, resume_frames: u64, chunks: (u32, u32)) {
    MULTI_PROGRESS_BAR.get_or_init(|| {
        let mpb = MultiProgress::new();

        let mut pbs = Vec::new();

        let digits = printable_base10_digits(chunks.1 as usize) as usize;

        for _ in 1..=workers {
            let pb = ProgressBar::hidden().with_style(
                ProgressStyle::default_spinner()
                    .template("{prefix:.dim} {msg}")
                    .expect("template is valid"),
            );
            pb.set_prefix(format!("[Idle  {digits:digits$}]"));
            pbs.push(mpb.add(pb));
        }

        let pb = ProgressBar::hidden();
        pb.set_style(pretty_progress_style(resume_frames));
        pb.enable_steady_tick(Duration::from_millis(100));
        pb.reset_elapsed();
        pb.reset_eta();
        pb.set_position(0);
        pb.set_length(len);
        pb.reset();
        pb.set_prefix(format!(
            "[{done}/{total} Chunks] ",
            done = chunks.0,
            total = chunks.1
        ));
        pbs.push(mpb.add(pb));

        mpb.set_draw_target(ProgressDrawTarget::stderr());

        (mpb, pbs)
    });
}

pub fn update_mp_chunk(worker_idx: usize, chunk: usize, padding: usize) {
    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get() {
        pbs[worker_idx].set_prefix(format!("[Chunk {chunk:>padding$}]"));
    }
}

pub fn update_mp_msg(worker_idx: usize, msg: String) {
    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get() {
        pbs[worker_idx].set_message(msg);
    }
}

pub fn inc_mp_bar(inc: u64) {
    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get() {
        pbs.last().expect("at least one progress bar exists").inc(inc);
    }
    inc_progress_jsonl(inc);
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "https://github.com/rust-lang/rust-clippy/issues/12786"
)]
pub fn update_mp_bar_info(kbps: f64, est_size: HumanBytes, chunks: (u32, u32)) {
    if let Some((_, pbs)) = MULTI_PROGRESS_BAR.get() {
        let pb = pbs.last().expect("at least one progress bar exists");
        pb.set_message(format!(", {kbps:.1} Kbps, est. {est_size}"));
        pb.set_prefix(format!(
            "[{done}/{total} Chunks] ",
            done = chunks.0,
            total = chunks.1
        ));
    }
}

pub fn update_progress_bar_estimates(
    frame_rate: f64,
    total_frames: usize,
    verbosity: Verbosity,
    chunks: (u32, u32),
) {
    let completed_frames: usize =
        get_done().done.iter().map(|ref_multi| ref_multi.value().frames).sum();
    if completed_frames == 0 {
        // avoid division by 0
        return;
    }

    let total_size: u64 = get_done()
        .done
        .iter()
        .map(|ref_multi| ref_multi.value().size_bytes)
        .sum::<u64>();
    let seconds_completed = completed_frames as f64 / frame_rate;
    let kbps = total_size as f64 * 8. / 1000. / seconds_completed;
    let progress = completed_frames as f64 / total_frames as f64;

    let audio_size_byte = get_audio_size();

    let est_size = total_size as f64 / progress + audio_size_byte as f64;
    if verbosity == Verbosity::Normal {
        update_bar_info(kbps, HumanBytes(est_size as u64), Some(chunks));
    } else if verbosity == Verbosity::Verbose {
        update_mp_bar_info(kbps, HumanBytes(est_size as u64), chunks);
    }

    update_progress_jsonl_estimates(completed_frames, kbps, est_size as u64, false);
}
