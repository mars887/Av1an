use std::{
    fmt::{Debug, Display},
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicU8, Ordering},
        mpsc::Sender,
        Arc,
        Condvar,
        Mutex,
    },
    thread::{self, available_parallelism, JoinHandle},
    time::Duration,
};

use anyhow::{bail, Context};
use cfg_if::cfg_if;
use smallvec::SmallVec;
use sysinfo::System;
use thiserror::Error;
use tracing::{debug, error, warn};

use crate::{
    context::Av1anContext,
    ffmpeg::{compose_ffmpeg_pipe, get_num_frames},
    finish_progress_bar,
    get_done,
    progress_bar::{
        dec_bar,
        inc_bar,
        inc_jsonl_progress,
        inc_mp_bar,
        update_mp_chunk,
        update_mp_msg,
        update_progress_bar_estimates,
    },
    settings::{InputPixelFormat, PixelFormatConverter},
    spawn_tracked,
    terminate_active_child_processes,
    util::printable_base10_digits,
    Chunk,
    DoneChunk,
    Instant,
    RegisteredChildProcess,
    Verbosity,
};

#[derive(Debug)]
pub struct Broker<'a> {
    pub chunk_queue: Vec<Chunk>,
    pub project:     &'a Av1anContext,
}

#[derive(Clone)]
pub enum StringOrBytes {
    String(String),
    Bytes(Vec<u8>),
}

impl Debug for StringOrBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(s) => {
                if f.alternate() {
                    f.write_str(&textwrap::indent(s, "        "))?; // 8 spaces
                } else {
                    f.write_str(s)?;
                }
            },
            Self::Bytes(b) => write!(f, "raw bytes: {b:?}")?,
        }

        Ok(())
    }
}

impl From<Vec<u8>> for StringOrBytes {
    fn from(bytes: Vec<u8>) -> Self {
        #[expect(
            clippy::option_if_let_else,
            reason = "https://github.com/rust-lang/rust-clippy/issues/15142"
        )]
        if let Ok(res) = simdutf8::basic::from_utf8(&bytes) {
            Self::String(res.to_string())
        } else {
            Self::Bytes(bytes)
        }
    }
}

impl From<String> for StringOrBytes {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

#[derive(Error, Debug)]
pub struct EncoderCrash {
    pub exit_status:        ExitStatus,
    pub stdout:             StringOrBytes,
    pub stderr:             StringOrBytes,
    pub source_pipe_stderr: StringOrBytes,
    pub ffmpeg_pipe_stderr: Option<StringOrBytes>,
}

impl Display for EncoderCrash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "encoder crashed: {}\nstdout:\n{:#?}\nstderr:\n{:#?}\nsource pipe stderr:\n{:#?}",
            self.exit_status, self.stdout, self.stderr, self.source_pipe_stderr,
        )?;

        if let Some(ffmpeg_pipe_stderr) = &self.ffmpeg_pipe_stderr {
            write!(f, "\nffmpeg pipe stderr:\n{ffmpeg_pipe_stderr:#?}")?;
        }

        Ok(())
    }
}

const FRAME_RECORD_RESERVE_OVERHEAD: u64 = 1024;

#[derive(Clone)]
struct RawSpoolBudget {
    inner:                  Arc<RawSpoolBudgetInner>,
    use_disk_cache:         bool,
    raw_spool_min_free_ram: u64,
}

struct RawSpoolBudgetInner {
    state:  Mutex<RawSpoolBudgetState>,
    wake:   Condvar,
    system: Mutex<System>,
    limit:  u64,
}

#[derive(Default)]
struct RawSpoolBudgetState {
    memory_bytes: u64,
    disk_bytes:   u64,
    shutdown:     bool,
}

#[derive(Clone, Copy)]
enum RawFrameStorageKind {
    Memory,
    Disk,
}

#[derive(Clone, Copy)]
struct RawFrameReservation {
    kind:  RawFrameStorageKind,
    bytes: u64,
}

impl RawSpoolBudget {
    fn new(limit: u64, raw_spool_min_free_ram: u64, use_disk_cache: bool) -> Self {
        Self {
            inner: Arc::new(RawSpoolBudgetInner {
                state: Mutex::new(RawSpoolBudgetState::default()),
                wake: Condvar::new(),
                system: Mutex::new(System::new()),
                limit,
            }),
            use_disk_cache,
            raw_spool_min_free_ram,
        }
    }

    fn reserve(&self, bytes: u64) -> anyhow::Result<RawFrameReservation> {
        if bytes > self.inner.limit {
            bail!(
                "a single raw frame needs {bytes} bytes, which is larger than --raw-spool-limit"
            );
        }

        let mut state = self.inner.state.lock().expect("mutex should acquire lock");
        loop {
            if state.shutdown {
                bail!("raw frame buffer is shutting down");
            }

            let used = state.memory_bytes.saturating_add(state.disk_bytes);
            if used.saturating_add(bytes) <= self.inner.limit {
                if self.can_reserve_memory(bytes) {
                    state.memory_bytes = state.memory_bytes.saturating_add(bytes);
                    return Ok(RawFrameReservation {
                        kind: RawFrameStorageKind::Memory,
                        bytes,
                    });
                }

                if self.use_disk_cache {
                    state.disk_bytes = state.disk_bytes.saturating_add(bytes);
                    return Ok(RawFrameReservation {
                        kind: RawFrameStorageKind::Disk,
                        bytes,
                    });
                }
            }

            let (new_state, _) = self
                .inner
                .wake
                .wait_timeout(state, Duration::from_millis(250))
                .expect("condvar should wait");
            state = new_state;
        }
    }

    fn release(&self, reservation: RawFrameReservation) {
        let mut state = self.inner.state.lock().expect("mutex should acquire lock");
        match reservation.kind {
            RawFrameStorageKind::Memory => {
                state.memory_bytes = state.memory_bytes.saturating_sub(reservation.bytes);
            },
            RawFrameStorageKind::Disk => {
                state.disk_bytes = state.disk_bytes.saturating_sub(reservation.bytes);
            },
        }
        self.inner.wake.notify_all();
    }

    fn shutdown(&self) {
        let mut state = self.inner.state.lock().expect("mutex should acquire lock");
        state.shutdown = true;
        self.inner.wake.notify_all();
    }

    fn can_reserve_memory(&self, bytes: u64) -> bool {
        if self.raw_spool_min_free_ram == 0 {
            return true;
        }

        let mut system = self.inner.system.lock().expect("mutex should acquire lock");
        system.refresh_memory();
        system.available_memory().saturating_sub(bytes) >= self.raw_spool_min_free_ram
    }
}

struct ChunkAttempt {
    chunk:   Chunk,
    attempt: usize,
}

struct EncodeJob {
    chunk:              Chunk,
    attempt:            usize,
    y4m_header:         Vec<u8>,
    frames:             crossbeam_channel::Receiver<FrameMessage>,
    source_pipe_stderr: Arc<Mutex<String>>,
    ffmpeg_pipe_stderr: Option<Arc<Mutex<String>>>,
}

enum FrameMessage {
    Frame(BufferedFrame),
    End(Result<(), String>),
}

struct BufferedFrame {
    storage:     Option<BufferedFrameStorage>,
    reservation: Option<RawFrameReservation>,
    budget:      RawSpoolBudget,
}

enum BufferedFrameStorage {
    Memory(Vec<u8>),
    Disk(PathBuf),
}

impl BufferedFrame {
    fn write_to(mut self, writer: &mut impl Write) -> std::io::Result<()> {
        match self.storage.as_ref().expect("buffered frame should have storage") {
            BufferedFrameStorage::Memory(bytes) => writer.write_all(bytes)?,
            BufferedFrameStorage::Disk(path) => {
                let mut file = File::open(path)?;
                std::io::copy(&mut file, writer)?;
                fs::remove_file(path)?;
                self.storage = None;
            },
        }

        Ok(())
    }
}

impl Drop for BufferedFrame {
    fn drop(&mut self) {
        if let Some(BufferedFrameStorage::Disk(path)) = self.storage.take() {
            let _ = fs::remove_file(path);
        }
        if let Some(reservation) = self.reservation.take() {
            self.budget.release(reservation);
        }
    }
}

enum PipelineResult {
    Done(Chunk),
    Failed {
        chunk:   Chunk,
        attempt: usize,
        error:   anyhow::Error,
        frames:  u64,
    },
}

struct SourcePipeline {
    stdout:             Option<Box<dyn Read + Send>>,
    source_pipe:        Child,
    source_guard:       RegisteredChildProcess,
    ffmpeg_pipe:        Option<Child>,
    ffmpeg_guard:       Option<RegisteredChildProcess>,
    source_pipe_stderr: Arc<Mutex<String>>,
    ffmpeg_pipe_stderr: Option<Arc<Mutex<String>>>,
    stderr_threads:     Vec<JoinHandle<()>>,
}

impl SourcePipeline {
    fn wait(mut self) -> anyhow::Result<()> {
        drop(self.stdout.take());

        let ffmpeg_status = if let Some(ffmpeg_pipe) = self.ffmpeg_pipe.as_mut() {
            Some(ffmpeg_pipe.wait().context("failed to wait for ffmpeg pipe")?)
        } else {
            None
        };
        let source_status = self.source_pipe.wait().context("failed to wait for source pipe")?;

        for handle in self.stderr_threads.drain(..) {
            let _ = handle.join();
        }

        drop(self.ffmpeg_guard.take());
        drop(self.source_guard);

        if !source_status.success() {
            bail!("source pipe exited with {source_status}");
        }
        if let Some(ffmpeg_status) = ffmpeg_status
            && !ffmpeg_status.success()
        {
            bail!("ffmpeg pipe exited with {ffmpeg_status}");
        }

        Ok(())
    }
}

fn spawn_stderr_reader(stderr: ChildStderr, output: Arc<Mutex<String>>) -> JoinHandle<()> {
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            let mut lock = output.lock().expect("mutex should acquire lock");
            lock.push_str(&line);
            lock.push('\n');
        }
    })
}

fn y4m_frame_payload_size(width: usize, height: usize, colorspace: y4m::Colorspace) -> u64 {
    let bytes_per_sample = colorspace.get_bytes_per_sample();
    let y_plane_size = width * height * bytes_per_sample;
    let c420_chroma_size = width.div_ceil(2) * height.div_ceil(2) * bytes_per_sample;
    let c422_chroma_size = width.div_ceil(2) * height * bytes_per_sample;

    let size = match colorspace {
        y4m::Colorspace::Cmono | y4m::Colorspace::Cmono12 => y_plane_size,
        y4m::Colorspace::C420
        | y4m::Colorspace::C420p10
        | y4m::Colorspace::C420p12
        | y4m::Colorspace::C420jpeg
        | y4m::Colorspace::C420paldv
        | y4m::Colorspace::C420mpeg2 => y_plane_size + c420_chroma_size * 2,
        y4m::Colorspace::C422 | y4m::Colorspace::C422p10 | y4m::Colorspace::C422p12 => {
            y_plane_size + c422_chroma_size * 2
        },
        y4m::Colorspace::C444 | y4m::Colorspace::C444p10 | y4m::Colorspace::C444p12 => {
            y_plane_size * 3
        },
        _ => y_plane_size + c420_chroma_size * 2,
    };

    size as u64
}

fn write_y4m_frame_record(mut writer: impl Write, frame: &y4m::Frame<'_>) -> std::io::Result<()> {
    writer.write_all(b"FRAME")?;
    if let Some(params) = frame.get_raw_params() {
        writer.write_all(b" ")?;
        writer.write_all(params)?;
    }
    writer.write_all(b"\n")?;
    writer.write_all(frame.get_y_plane())?;
    writer.write_all(frame.get_u_plane())?;
    writer.write_all(frame.get_v_plane())?;
    Ok(())
}

fn write_buffered_frame(writer: &mut impl Write, frame: BufferedFrame) -> std::io::Result<()> {
    frame.write_to(writer)
}

impl Broker<'_> {
    /// Main encoding loop. set_thread_affinity may be ignored if the value is
    /// invalid.
    #[tracing::instrument(skip(self))]
    #[allow(clippy::needless_pass_by_value)]
    pub fn encoding_loop(
        self,
        tx: Sender<()>,
        set_thread_affinity: Option<usize>,
        total_chunks: u32,
    ) -> anyhow::Result<()> {
        if self.project.args.decoupled_encoding_enabled() {
            return self.encoding_loop_decoupled(&tx, total_chunks);
        }

        if !self.chunk_queue.is_empty() {
            let (sender, receiver) = crossbeam_channel::bounded(self.chunk_queue.len());

            for chunk in &self.chunk_queue {
                sender.send(chunk.clone())?;
            }
            drop(sender);

            crossbeam_utils::thread::scope(|s| {
                let terminations_requested = Arc::new(AtomicU8::new(0));
                let terminations_requested_clone = Arc::clone(&terminations_requested);
                let fast_interrupt = self.project.args.fast_interrupt;
                ctrlc::set_handler(move || {
                    let count = terminations_requested_clone.fetch_add(1, Ordering::SeqCst) + 1;
                    let force_shutdown = fast_interrupt || count > 1;
                    if force_shutdown {
                        if count == 1 {
                            error!("Fast shutdown requested. Terminating active workers...");
                        } else {
                            error!("Shutting down all workers...");
                        }
                        terminate_active_child_processes();
                    } else {
                        error!("Shutting down. Waiting for current workers to finish...");
                        error!(
                            "Waiting for current workers to finish. Press Ctrl+C again to terminate \
                             active workers immediately."
                        );
                    }
                })
                .expect("should set ctrlc handler");

                let consumers: Vec<_> = (0..self.project.args.workers)
                    .map(|idx| (receiver.clone(), &self, idx, Arc::clone(&terminations_requested)))
                    .map(|(rx, queue, worker_id, terminations_requested)| {
                        let tx = tx.clone();
                        s.spawn(move |_| {
                            cfg_if! {
                                if #[cfg(any(target_os = "linux", target_os = "windows"))] {
                                    if let Some(threads) = set_thread_affinity {
                                        if threads == 0 {
                                            warn!("Ignoring set_thread_affinity: Requested 0 threads");
                                        } else {
                                            match available_parallelism() {
                                                Ok(parallelism) => {
                                                    let available_threads = parallelism.get();
                                                    let mut cpu_set = SmallVec::<[usize; 16]>::new();
                                                    let start_thread = (threads * worker_id) % available_threads;
                                                    cpu_set.extend((start_thread..start_thread + threads).map(|t| t % available_threads));
                                                    if let Err(e) = affinity::set_thread_affinity(&cpu_set) {
                                                        warn!("Failed to set thread affinity for worker {worker_id}: {e}");
                                                    }
                                                },
                                                Err(e) => {
                                                    warn!("Failed to get thread count: {e}. Thread affinity will not be set");
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            while let Ok(mut chunk) = rx.recv() {
                                if terminations_requested.load(Ordering::SeqCst) == 0
                                    && let Err(e) = queue.encode_chunk(
                                        &mut chunk,
                                        worker_id,
                                        &terminations_requested,
                                        total_chunks,
                                    )
                                {
                                    error!("[chunk {index}] {e}", index = chunk.index);
                                    tx.send(()).expect("should send successfully");
                                    return Err(());
                                }
                            }
                            Ok(())
                        })
                    })
                    .collect();
                for consumer in consumers {
                    consumer.join().expect("consumer should join successfully").ok();
                }

                if terminations_requested.load(Ordering::SeqCst) > 0 {
                    tx.send(()).expect("should send successfully");
                }
            })
            .expect("thread should spawn successfully");

            finish_progress_bar();
        }

        Ok(())
    }

    fn encoding_loop_decoupled(self, tx: &Sender<()>, total_chunks: u32) -> anyhow::Result<()> {
        if self.chunk_queue.is_empty() {
            return Ok(());
        }

        let raw_spool_dir = self
            .project
            .args
            .raw_spool_dir
            .clone()
            .unwrap_or_else(|| Path::new(&self.project.args.temp).join("raw-spool"));
        if self.project.args.use_disk_cache {
            fs::create_dir_all(&raw_spool_dir).with_context(|| {
                format!(
                    "failed to create raw spool directory {}",
                    raw_spool_dir.display()
                )
            })?;
        }

        let padding = printable_base10_digits(self.chunk_queue.len() - 1) as usize;
        let budget = RawSpoolBudget::new(
            self.project.args.raw_spool_limit,
            self.project.args.raw_spool_min_free_ram,
            self.project.args.use_disk_cache,
        );

        let (source_tx, source_rx) = crossbeam_channel::unbounded::<ChunkAttempt>();
        let (encode_tx, encode_rx) = crossbeam_channel::unbounded::<EncodeJob>();
        let (result_tx, result_rx) = crossbeam_channel::unbounded::<PipelineResult>();

        for chunk in &self.chunk_queue {
            source_tx.send(ChunkAttempt {
                chunk:   chunk.clone(),
                attempt: 1,
            })?;
        }
        let retry_tx = source_tx.clone();
        drop(source_tx);

        crossbeam_utils::thread::scope(|s| {
            let terminations_requested = Arc::new(AtomicU8::new(0));
            let terminations_requested_clone = Arc::clone(&terminations_requested);
            let budget_for_ctrlc = budget.clone();
            let fast_interrupt = self.project.args.fast_interrupt;
            ctrlc::set_handler(move || {
                let count = terminations_requested_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let force_shutdown = fast_interrupt || count > 1;
                budget_for_ctrlc.shutdown();
                if force_shutdown {
                    if count == 1 {
                        error!("Fast shutdown requested. Terminating active workers...");
                    } else {
                        error!("Shutting down all workers...");
                    }
                    terminate_active_child_processes();
                } else {
                    error!("Shutting down. Waiting for current workers to finish...");
                    error!(
                        "Waiting for current workers to finish. Press Ctrl+C again to terminate \
                         active workers immediately."
                    );
                }
            })
            .expect("should set ctrlc handler");

            let mut handles = Vec::new();
            for worker_id in 0..self.project.args.source_workers {
                let source_rx = source_rx.clone();
                let encode_tx = encode_tx.clone();
                let result_tx = result_tx.clone();
                let budget = budget.clone();
                let raw_spool_dir = raw_spool_dir.clone();
                let terminations_requested = Arc::clone(&terminations_requested);
                let queue = &self;
                handles.push(s.spawn(move |_| {
                    while terminations_requested.load(Ordering::SeqCst) == 0 {
                        let Ok(attempt) = source_rx.recv() else {
                            break;
                        };
                        update_mp_chunk(worker_id, attempt.chunk.index, padding);
                        update_mp_msg(worker_id, "Source".to_string());

                        if let Err((chunk, attempt, error)) = queue.produce_buffered_chunk(
                            attempt,
                            worker_id,
                            &budget,
                            &raw_spool_dir,
                            &encode_tx,
                            &terminations_requested,
                            padding,
                        ) {
                            result_tx
                                .send(PipelineResult::Failed {
                                    chunk,
                                    attempt,
                                    error,
                                    frames: 0,
                                })
                                .expect("should send successfully");
                            break;
                        }
                    }
                }));
            }
            drop(encode_tx);

            for encoder_id in 0..self.project.args.encoder_workers {
                let encode_rx = encode_rx.clone();
                let result_tx = result_tx.clone();
                let terminations_requested = Arc::clone(&terminations_requested);
                let queue = &self;
                let worker_id = self.project.args.source_workers + encoder_id;
                handles.push(s.spawn(move |_| {
                    while terminations_requested.load(Ordering::SeqCst) == 0 {
                        let Ok(job) = encode_rx.recv() else {
                            break;
                        };
                        let result = queue.encode_buffered_job(job, worker_id, padding);
                        result_tx.send(result).expect("should send successfully");
                    }
                }));
            }
            drop(result_tx);

            let mut completed_chunks = 0_u32;
            let mut failed = false;
            while completed_chunks < total_chunks {
                match result_rx.recv() {
                    Ok(PipelineResult::Done(chunk)) => {
                        completed_chunks += 1;
                        if let Err(error) = self.mark_chunk_done(&chunk, total_chunks) {
                            error!("[chunk {index}] {error}", index = chunk.index);
                            failed = true;
                            break;
                        }
                    },
                    Ok(PipelineResult::Failed {
                        chunk,
                        attempt,
                        error,
                        frames,
                    }) => {
                        dec_bar(frames);
                        if attempt < self.project.args.max_tries
                            && terminations_requested.load(Ordering::SeqCst) == 0
                        {
                            warn!(
                                "Encoder failed (on chunk {index}):\n{error}",
                                index = chunk.index
                            );
                            if let Err(error) = retry_tx.send(ChunkAttempt {
                                chunk,
                                attempt: attempt + 1,
                            }) {
                                error!("failed to requeue chunk: {error}");
                                failed = true;
                                break;
                            }
                        } else {
                            error!("[chunk {index}] {error}", index = chunk.index);
                            failed = true;
                            break;
                        }
                    },
                    Err(_) => {
                        if completed_chunks < total_chunks {
                            error!("source/encoder pipeline stopped before all chunks completed");
                            failed = true;
                        }
                        break;
                    },
                }
            }

            if failed || terminations_requested.load(Ordering::SeqCst) > 0 {
                terminations_requested.store(1, Ordering::SeqCst);
                budget.shutdown();
                terminate_active_child_processes();
                tx.send(()).expect("should send successfully");
            }
            drop(retry_tx);

            for handle in handles {
                handle.join().expect("worker should join successfully");
            }
        })
        .expect("thread should spawn successfully");

        finish_progress_bar();
        Ok(())
    }

    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    fn produce_buffered_chunk(
        &self,
        attempt: ChunkAttempt,
        worker_id: usize,
        budget: &RawSpoolBudget,
        raw_spool_dir: &Path,
        encode_tx: &crossbeam_channel::Sender<EncodeJob>,
        terminations_requested: &Arc<AtomicU8>,
        padding: usize,
    ) -> Result<(), (Chunk, usize, anyhow::Error)> {
        let attempt_number = attempt.attempt;
        let chunk = attempt.chunk;
        let (frame_tx, frame_rx) = crossbeam_channel::unbounded();

        let mut pipeline = self
            .spawn_source_pipeline(&chunk)
            .map_err(|error| (chunk.clone(), attempt_number, error))?;
        let source_pipe_stderr = Arc::clone(&pipeline.source_pipe_stderr);
        let ffmpeg_pipe_stderr = pipeline.ffmpeg_pipe_stderr.clone();

        let stdout = pipeline.stdout.take().expect("source pipeline should have stdout");
        let mut decoder = match y4m::Decoder::new(stdout) {
            Ok(decoder) => decoder,
            Err(error) => {
                let _ = pipeline.wait();
                return Err((
                    chunk,
                    attempt_number,
                    anyhow::anyhow!("failed to read Y4M header: {error}"),
                ));
            },
        };

        let mut y4m_header = Vec::with_capacity(decoder.get_raw_params().len() + 16);
        y4m_header.extend_from_slice(b"YUV4MPEG2 ");
        y4m_header.extend_from_slice(decoder.get_raw_params());
        y4m_header.push(b'\n');

        encode_tx
            .send(EncodeJob {
                chunk: chunk.clone(),
                attempt: attempt_number,
                y4m_header,
                frames: frame_rx,
                source_pipe_stderr: Arc::clone(&source_pipe_stderr),
                ffmpeg_pipe_stderr,
            })
            .map_err(|error| {
                (
                    chunk.clone(),
                    attempt_number,
                    anyhow::anyhow!("failed to queue chunk for encoder: {error}"),
                )
            })?;

        let frame_payload_size = y4m_frame_payload_size(
            decoder.get_width(),
            decoder.get_height(),
            decoder.get_colorspace(),
        )
        .saturating_add(FRAME_RECORD_RESERVE_OVERHEAD);
        let mut frame_index = 0_usize;
        let mut source_pipeline = Some(pipeline);

        let source_result = loop {
            if terminations_requested.load(Ordering::SeqCst) > 0 {
                break Err(anyhow::anyhow!("termination requested"));
            }

            let reservation = match budget.reserve(frame_payload_size) {
                Ok(reservation) => reservation,
                Err(error) => break Err(error),
            };

            let frame = match decoder.read_frame() {
                Ok(frame) => frame,
                Err(y4m::Error::EOF) => {
                    budget.release(reservation);
                    break Ok(());
                },
                Err(error) => {
                    budget.release(reservation);
                    break Err(anyhow::anyhow!("failed to read Y4M frame: {error}"));
                },
            };

            let buffered = match self.buffer_frame(
                &frame,
                reservation,
                &chunk,
                attempt_number,
                frame_index,
                raw_spool_dir,
                budget,
            ) {
                Ok(buffered) => buffered,
                Err(error) => {
                    budget.release(reservation);
                    break Err(error);
                },
            };

            if let Err(error) = frame_tx.send(FrameMessage::Frame(buffered)) {
                let FrameMessage::Frame(_buffered) = error.into_inner() else {
                    unreachable!()
                };
                break Err(anyhow::anyhow!("encoder stopped before source finished"));
            }

            frame_index += 1;
            update_mp_chunk(worker_id, chunk.index, padding);
        };

        drop(decoder);
        let wait_result = source_pipeline.take().expect("source pipeline should exist").wait();
        let source_result = match (source_result, wait_result) {
            (Ok(()), result) => result,
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(wait_error)) => Err(anyhow::anyhow!("{error}; {wait_error}")),
        };

        let end_result = source_result.map_err(|error| {
            let source = source_pipe_stderr.lock().expect("mutex should acquire lock");
            if source.is_empty() {
                error.to_string()
            } else {
                format!("{error}\nsource pipe stderr:\n{source}")
            }
        });
        let _ = frame_tx.send(FrameMessage::End(end_result));

        Ok(())
    }

    fn buffer_frame(
        &self,
        frame: &y4m::Frame<'_>,
        reservation: RawFrameReservation,
        chunk: &Chunk,
        attempt: usize,
        frame_index: usize,
        raw_spool_dir: &Path,
        budget: &RawSpoolBudget,
    ) -> anyhow::Result<BufferedFrame> {
        let storage = match reservation.kind {
            RawFrameStorageKind::Memory => {
                let mut bytes = Vec::with_capacity(reservation.bytes as usize);
                write_y4m_frame_record(&mut bytes, frame)?;
                BufferedFrameStorage::Memory(bytes)
            },
            RawFrameStorageKind::Disk => {
                let path = raw_spool_dir.join(format!(
                    "{chunk:05}_{attempt}_{frame:08}.y4mframe",
                    chunk = chunk.index,
                    frame = frame_index
                ));
                let mut file = File::create(&path).with_context(|| {
                    format!("failed to create raw frame spill file {}", path.display())
                })?;
                write_y4m_frame_record(&mut file, frame)?;
                BufferedFrameStorage::Disk(path)
            },
        };

        Ok(BufferedFrame {
            storage:     Some(storage),
            reservation: Some(reservation),
            budget:      budget.clone(),
        })
    }

    fn spawn_source_pipeline(&self, chunk: &Chunk) -> anyhow::Result<SourcePipeline> {
        let mut use_vs_resize_converter = false;
        let (mut source_pipe, source_guard) = if let [source, args @ ..] = &*chunk.source_cmd {
            let mut command = Command::new(source);

            for arg in chunk.input.as_vspipe_args_vec()? {
                command.args(["-a", &arg]);
            }

            command.args(args);
            if self.project.args.ffmpeg_filter_args.is_empty() {
                match &self.project.args.input_pix_format {
                    InputPixelFormat::FFmpeg {
                        format,
                    } => {
                        if self.project.args.output_pix_format.format != *format
                            && self.project.args.pix_format_converter
                                == PixelFormatConverter::VsResize
                            && self.project.args.input.is_video()
                        {
                            command.env(
                                "AV1AN_PIXEL_FORMAT",
                                self.project
                                    .args
                                    .output_pix_format
                                    .format
                                    .to_vapoursynth_string()?,
                            );
                            use_vs_resize_converter = true;
                        }
                    },
                    InputPixelFormat::VapourSynth {
                        bit_depth,
                    } => {
                        if self.project.args.output_pix_format.bit_depth != *bit_depth
                            && self.project.args.pix_format_converter
                                == PixelFormatConverter::VsResize
                            && self.project.args.input.is_video()
                        {
                            command.env(
                                "AV1AN_PIXEL_FORMAT",
                                self.project
                                    .args
                                    .output_pix_format
                                    .format
                                    .to_vapoursynth_string()?,
                            );
                            use_vs_resize_converter = true;
                        }
                    },
                }
            }

            spawn_tracked(command.stdout(Stdio::piped()).stderr(Stdio::piped()))?
        } else {
            unreachable!()
        };

        let source_stdout = source_pipe.stdout.take().expect("source_pipe should have stdout");
        let source_stderr = source_pipe.stderr.take().expect("source_pipe should have stderr");
        let source_pipe_stderr = Arc::new(Mutex::new(String::with_capacity(128)));
        let mut stderr_threads =
            vec![spawn_stderr_reader(source_stderr, Arc::clone(&source_pipe_stderr))];

        let mut create_ffmpeg_pipe = |pipe_from: std::process::ChildStdout| {
            let ffmpeg_pipe = compose_ffmpeg_pipe(
                self.project.args.ffmpeg_filter_args.as_slice(),
                self.project.args.output_pix_format.format,
            );

            let (mut ffmpeg_pipe, ffmpeg_guard) = if let [ffmpeg, args @ ..] = &*ffmpeg_pipe {
                let mut command = Command::new(ffmpeg);
                spawn_tracked(
                    command
                        .args(args)
                        .stdin(pipe_from)
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped()),
                )?
            } else {
                unreachable!()
            };

            let ffmpeg_stdout = ffmpeg_pipe.stdout.take().expect("ffmpeg_pipe should have stdout");
            let ffmpeg_stderr = ffmpeg_pipe.stderr.take().expect("ffmpeg_pipe should have stderr");
            let ffmpeg_pipe_stderr = Arc::new(Mutex::new(String::with_capacity(128)));
            stderr_threads.push(spawn_stderr_reader(
                ffmpeg_stderr,
                Arc::clone(&ffmpeg_pipe_stderr),
            ));

            Ok::<_, anyhow::Error>((
                Box::new(ffmpeg_stdout) as Box<dyn Read + Send>,
                Some(ffmpeg_pipe),
                Some(ffmpeg_guard),
                Some(ffmpeg_pipe_stderr),
            ))
        };

        let (stdout, ffmpeg_pipe, ffmpeg_guard, ffmpeg_pipe_stderr) =
            if self.project.args.ffmpeg_filter_args.is_empty() {
                match &self.project.args.input_pix_format {
                    InputPixelFormat::FFmpeg {
                        format,
                    } => {
                        if use_vs_resize_converter
                            || self.project.args.output_pix_format.format == *format
                        {
                            (
                                Box::new(source_stdout) as Box<dyn Read + Send>,
                                None,
                                None,
                                None,
                            )
                        } else {
                            create_ffmpeg_pipe(source_stdout)?
                        }
                    },
                    InputPixelFormat::VapourSynth {
                        bit_depth,
                    } => {
                        if use_vs_resize_converter
                            || self.project.args.output_pix_format.bit_depth == *bit_depth
                        {
                            (
                                Box::new(source_stdout) as Box<dyn Read + Send>,
                                None,
                                None,
                                None,
                            )
                        } else {
                            create_ffmpeg_pipe(source_stdout)?
                        }
                    },
                }
            } else {
                create_ffmpeg_pipe(source_stdout)?
            };

        Ok(SourcePipeline {
            stdout: Some(stdout),
            source_pipe,
            source_guard,
            ffmpeg_pipe,
            ffmpeg_guard,
            source_pipe_stderr,
            ffmpeg_pipe_stderr,
            stderr_threads,
        })
    }

    fn encode_buffered_job(
        &self,
        job: EncodeJob,
        worker_id: usize,
        padding: usize,
    ) -> PipelineResult {
        update_mp_chunk(worker_id, job.chunk.index, padding);
        update_mp_msg(worker_id, "Encoder".to_string());

        let attempt = job.attempt;
        match self.try_encode_buffered_job(job, worker_id) {
            Ok(chunk) => PipelineResult::Done(chunk),
            Err((chunk, error, frames)) => PipelineResult::Failed {
                chunk,
                attempt,
                error,
                frames,
            },
        }
    }

    #[allow(clippy::result_large_err)]
    fn try_encode_buffered_job(
        &self,
        job: EncodeJob,
        worker_id: usize,
    ) -> Result<Chunk, (Chunk, anyhow::Error, u64)> {
        let chunk = job.chunk.clone();
        let mut enc_cmd = chunk.encoder.compose_1_1_pass(
            chunk.video_params.clone(),
            chunk.output(),
            self.project.args.encoder_path.as_deref(),
        );
        if let Some(per_shot_target_quality_cq) = chunk.tq_cq {
            enc_cmd = chunk.encoder.man_command(enc_cmd, per_shot_target_quality_cq);
        }

        let (mut enc_pipe, _enc_guard) = if let [encoder, args @ ..] = &*enc_cmd {
            let mut command = Command::new(encoder);
            spawn_tracked(
                command
                    .args(args)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped()),
            )
            .map_err(|error| (chunk.clone(), anyhow::anyhow!(error), 0))?
        } else {
            unreachable!()
        };

        let mut enc_stdin = enc_pipe.stdin.take().expect("enc_pipe should have stdin");
        let mut enc_stderr =
            BufReader::new(enc_pipe.stderr.take().expect("enc_pipe should have stderr"));
        let frames = job.frames;
        let y4m_header = job.y4m_header;

        let writer = thread::spawn(move || -> std::io::Result<()> {
            enc_stdin.write_all(&y4m_header)?;
            while let Ok(message) = frames.recv() {
                match message {
                    FrameMessage::Frame(frame) => {
                        write_buffered_frame(&mut enc_stdin, frame)?;
                    },
                    FrameMessage::End(result) => {
                        if let Err(error) = result {
                            return Err(std::io::Error::other(error));
                        }
                        break;
                    },
                }
            }
            enc_stdin.flush()?;
            Ok(())
        });

        let mut frame = 0_u64;
        let mut buf = Vec::with_capacity(128);
        let mut enc_stderr_text = String::with_capacity(128);
        while let Ok(read) = enc_stderr.read_until(b'\r', &mut buf) {
            if read == 0 {
                break;
            }

            if let Ok(line) = simdutf8::basic::from_utf8_mut(&mut buf) {
                if self.project.args.verbosity == Verbosity::Verbose && !line.contains('\n') {
                    update_mp_msg(worker_id, line.trim().to_string());
                }
                enc_stderr_text.push_str(line);
                enc_stderr_text.push('\n');

                if let Some(new) = chunk.encoder.parse_encoded_frames(line)
                    && new > frame
                {
                    if self.project.args.verbosity == Verbosity::Normal {
                        inc_bar(new - frame);
                    } else if self.project.args.verbosity == Verbosity::Verbose {
                        inc_mp_bar(new - frame);
                    } else if self.project.args.progress_jsonl.is_some() {
                        inc_jsonl_progress(new - frame);
                    }
                    frame = new;
                }
            }

            buf.clear();
        }

        let enc_output = enc_pipe.wait_with_output().expect("enc_pipe should finish");
        let writer_result = writer.join().expect("writer thread should join");

        if let Err(error) = writer_result {
            return Err((
                chunk,
                anyhow::anyhow!("failed to write buffered frames to encoder stdin: {error}"),
                frame,
            ));
        }

        let source_pipe_stderr =
            job.source_pipe_stderr.lock().expect("mutex should acquire lock").clone();
        let ffmpeg_pipe_stderr = job
            .ffmpeg_pipe_stderr
            .map(|stderr| stderr.lock().expect("mutex should acquire lock").clone());

        if !enc_output.status.success() {
            return Err((
                chunk,
                EncoderCrash {
                    exit_status:        enc_output.status,
                    source_pipe_stderr: source_pipe_stderr.into(),
                    ffmpeg_pipe_stderr: ffmpeg_pipe_stderr.map(Into::into),
                    stderr:             enc_stderr_text.into(),
                    stdout:             enc_output.stdout.into(),
                }
                .into(),
                frame,
            ));
        }

        if !fs::exists(chunk.output()).map_err(|error| (chunk.clone(), error.into(), frame))?
            || fs::metadata(chunk.output())
                .map_err(|error| (chunk.clone(), error.into(), frame))?
                .len()
                == 0
        {
            return Err((
                chunk.clone(),
                anyhow::anyhow!(
                    "ERROR: Output chunk file {} could not be created. Possible permissions or \
                     disk space issue?",
                    chunk.output()
                ),
                frame,
            ));
        }

        let encoded_frames = get_num_frames(chunk.output().as_ref());
        let err_str = match encoded_frames {
            Ok(encoded_frames)
                if !chunk.ignore_frame_mismatch && encoded_frames != chunk.frames() =>
            {
                Some(format!(
                    "FRAME MISMATCH: chunk {index}: {encoded_frames}/{expected} (actual/expected \
                     frames)",
                    index = chunk.index,
                    expected = chunk.frames()
                ))
            },
            Err(error) => Some(format!(
                "FAILED TO COUNT FRAMES: chunk {index}: {error}",
                index = chunk.index
            )),
            _ => None,
        };

        if let Some(err_str) = err_str {
            return Err((
                chunk,
                EncoderCrash {
                    exit_status:        enc_output.status,
                    source_pipe_stderr: source_pipe_stderr.into(),
                    ffmpeg_pipe_stderr: ffmpeg_pipe_stderr.map(Into::into),
                    stderr:             enc_stderr_text.into(),
                    stdout:             err_str.into(),
                }
                .into(),
                frame,
            ));
        }

        Ok(chunk)
    }

    fn mark_chunk_done(&self, chunk: &Chunk, total_chunks: u32) -> anyhow::Result<()> {
        let progress_file = Path::new(&self.project.args.temp).join("done.json");
        get_done().done.insert(chunk.name(), DoneChunk {
            frames:     chunk.frames(),
            size_bytes: Path::new(&chunk.output())
                .metadata()
                .expect("Unable to get size of finished chunk")
                .len(),
        });

        let mut progress_file = File::create(progress_file)?;
        progress_file.write_all(serde_json::to_string(get_done())?.as_bytes())?;

        update_progress_bar_estimates(
            chunk.frame_rate,
            self.project.frames,
            self.project.args.verbosity,
            (get_done().done.len() as u32, total_chunks),
        );

        Ok(())
    }

    #[tracing::instrument(skip(self, chunk, terminations_requested), fields(chunk_index = format!("{:>05}", chunk.index)))]
    fn encode_chunk(
        &self,
        chunk: &mut Chunk,
        worker_id: usize,
        terminations_requested: &Arc<AtomicU8>,
        total_chunks: u32,
    ) -> anyhow::Result<()> {
        let st_time = Instant::now();

        // we display the index, so we need to subtract 1 to get the max index
        let padding = printable_base10_digits(self.chunk_queue.len() - 1) as usize;
        update_mp_chunk(worker_id, chunk.index, padding);

        if let Some((min, max)) = chunk.target_quality.target {
            update_mp_msg(
                worker_id,
                format!(
                    "Targeting {metric} Quality: {min}-{max}",
                    metric = chunk.target_quality.metric,
                    min = min,
                    max = max
                ),
            );
            for r#try in 1..=self.project.args.max_tries {
                let res = chunk.target_quality.per_shot_target_quality(
                    chunk,
                    Some(worker_id),
                    self.project.args.vapoursynth_plugins,
                );
                match res {
                    Ok(cq) => {
                        chunk.tq_cq = Some(cq);
                        break;
                    },
                    Err(e) => {
                        if terminations_requested.load(Ordering::SeqCst) > 0 {
                            bail!(
                                "Termination requested during Target Quality. Skipping chunk {}",
                                chunk.index
                            );
                        }
                        if r#try >= self.project.args.max_tries {
                            bail!(
                                "Target Quality failed after {} tries on chunk {}:\n{}",
                                r#try,
                                chunk.index,
                                e
                            );
                        }
                    },
                }
            }

            if chunk.target_quality.params_copied
                && chunk.target_quality.probing_rate == 1
                && self.project.args.ffmpeg_filter_args.is_empty()
                && chunk.proxy.is_none()
                && let Some(optimal_q) = chunk.tq_cq
            {
                let extension = match self.project.args.encoder {
                    crate::encoder::Encoder::x264 => "264",
                    crate::encoder::Encoder::x265 => "hevc",
                    _ => "ivf",
                };
                let probe_file =
                    std::path::Path::new(&self.project.args.temp).join("split").join({
                        let q_str = crate::encoder::format_q(optimal_q);
                        format!("v_{:05}_{}.{}", chunk.index, q_str, extension)
                    });

                if probe_file.exists() {
                    let encode_dir = std::path::Path::new(&self.project.args.temp).join("encode");
                    std::fs::create_dir_all(&encode_dir)?;
                    let output_file =
                        encode_dir.join(format!("{index:05}.{extension}", index = chunk.index));
                    std::fs::copy(&probe_file, &output_file)?;

                    inc_mp_bar(chunk.frames() as u64);

                    let progress_file = Path::new(&self.project.args.temp).join("done.json");
                    get_done().done.insert(chunk.name(), DoneChunk {
                        frames:     chunk.frames(),
                        size_bytes: output_file.metadata()?.len(),
                    });

                    let mut progress_file = File::create(progress_file)?;
                    progress_file.write_all(serde_json::to_string(get_done())?.as_bytes())?;

                    update_progress_bar_estimates(
                        chunk.frame_rate,
                        self.project.frames,
                        self.project.args.verbosity,
                        (get_done().done.len() as u32, total_chunks),
                    );

                    return Ok(());
                }
            }
        }

        if terminations_requested.load(Ordering::SeqCst) > 0 {
            bail!(
                "Termination requested after Target Quality. Skipping chunk {}",
                chunk.index
            );
        }

        // space padding at the beginning to align with "finished chunk"
        debug!(
            " started chunk {index:05}: {frames} frames",
            index = chunk.index,
            frames = chunk.frames()
        );

        let passes = chunk.passes;
        for current_pass in 1..=passes {
            for r#try in 1..=self.project.args.max_tries {
                let res = self.project.create_pipes(chunk, current_pass, worker_id, padding);
                if let Err((e, frames)) = res {
                    dec_bar(frames);

                    if terminations_requested.load(Ordering::SeqCst) > 0 {
                        bail!(
                            "Termination requested after process interruption. Skipping chunk {}",
                            chunk.index
                        );
                    }

                    if r#try == self.project.args.max_tries {
                        bail!(
                            "[chunk {index}] encoder failed {tries} times, shutting down worker: \
                             {e}",
                            index = chunk.index,
                            tries = self.project.args.max_tries
                        );
                    }
                    // avoids double-print of the error message as both a WARN and ERROR,
                    // since `Broker::encoding_loop` will print the error message as well
                    warn!(
                        "Encoder failed (on chunk {index}):\n{e}",
                        index = chunk.index
                    );
                } else {
                    break;
                }
            }
        }

        let enc_time = st_time.elapsed();
        let fps = chunk.frames() as f64 / enc_time.as_secs_f64();

        let progress_file = Path::new(&self.project.args.temp).join("done.json");
        get_done().done.insert(chunk.name(), DoneChunk {
            frames:     chunk.frames(),
            size_bytes: Path::new(&chunk.output())
                .metadata()
                .expect("Unable to get size of finished chunk")
                .len(),
        });

        let mut progress_file = File::create(progress_file)?;
        progress_file.write_all(serde_json::to_string(get_done())?.as_bytes())?;

        update_progress_bar_estimates(
            chunk.frame_rate,
            self.project.frames,
            self.project.args.verbosity,
            (get_done().done.len() as u32, total_chunks),
        );

        debug!(
            "finished chunk {index:05}: {frames} frames, {fps:.2} fps, took {enc_time:.2?}",
            index = chunk.index,
            frames = chunk.frames()
        );

        Ok(())
    }
}
