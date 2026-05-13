use accumulator::{Accumulator, Config, panic_payload_to_string};
use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;
use rand::RngExt;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
enum ExampleError {
    #[error("key {key} is out of range (max is {max})")]
    KeyOutOfRange { key: u8, max: usize },
}

#[derive(Default, Clone, Debug)]
struct KahanState {
    sum: f64,
    compensation: f64,
    count: u64,
}

impl KahanState {
    fn add(&mut self, x: f64) {
        let y = x - self.compensation;
        let t = self.sum + y;
        self.compensation = (t - self.sum) - y;
        self.sum = t;
        self.count += 1;
    }

    fn merge(a: &KahanState, b: &KahanState) -> KahanState {
        let mut out = KahanState::default();
        out.add(a.sum - a.compensation);
        out.add(b.sum - b.compensation);
        out.count = a.count + b.count;
        out
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.sum - self.compensation) / self.count as f64
        }
    }
}

const NUM_KEYS: usize = 192;
const NUM_VALUES: usize = 10_000_000_00;
const DEFAULT_MAX_BATCH_SIZE: usize = 65_536;
const DEFAULT_BUFFER_POOL_MULTIPLIER: usize = 2;

type Sample = (u8, f32);
type Msg = Vec<Sample>;
type BufferSender = accumulator::kanal::Sender<Msg>;
type BufferReceiver = accumulator::kanal::Receiver<Msg>;

fn main() -> Result<()> {
    let max_batch_size = configured_max_batch_size();
    let (samples, generation_time) = generate_samples();

    println!("===========================================================");
    println!("Measurement baselines");
    println!("===========================================================\n");
    println!("max batch size: {max_batch_size}\n");
    println!(
        "pre-generated samples:
  values generated   : {}
  sample storage     : {:.2} GiB
  generation time    : {generation_time:?} ({:.2} M values/s)
",
        samples.len(),
        sample_storage_gib(samples.len()),
        million_msgs_per_sec(samples.len() as u64, generation_time)
    );
    run_measurement_baselines(max_batch_size, samples.as_slice());

    println!("\n===========================================================");
    println!("Example 1: timing sweep (1 / 2 / 4 / 8 consumers, timing only)");
    println!("===========================================================\n");
    run_timing_all(max_batch_size, Arc::clone(&samples)).context("run_timing_all failed")?;

    if std::env::var_os("ACCUMULATOR_TIMING_ONLY").is_some() {
        return Ok(());
    }

    println!("\n===========================================================");
    println!("Example 2: single consumer");
    println!("===========================================================\n");
    run_single(max_batch_size, Arc::clone(&samples)).context("run_single failed")?;

    println!("\n===========================================================");
    println!("Example 3: double consumer");
    println!("===========================================================\n");
    run_double(max_batch_size, Arc::clone(&samples)).context("run_double failed")?;

    println!("\n===========================================================");
    println!("Example 4: four consumers");
    println!("===========================================================\n");
    run_four(max_batch_size, Arc::clone(&samples)).context("run_four failed")?;

    println!("\n===========================================================");
    println!("Example 5: eight consumers");
    println!("===========================================================\n");
    run_eight(max_batch_size, Arc::clone(&samples)).context("run_eight failed")?;

    Ok(())
}

struct WorkerState {
    keys: Vec<KahanState>,
    started_at: Option<Instant>,
    batches: u64,
}

impl WorkerState {
    fn new(num_keys: usize) -> Self {
        WorkerState {
            keys: vec![KahanState::default(); num_keys],
            started_at: None,
            batches: 0,
        }
    }
}

struct WorkerOutput {
    keys: Vec<KahanState>,
    wall_time: Duration,
    batches: u64,
}

#[derive(Default)]
struct ProducerStats {
    elapsed: Duration,
    buffer_wait: Duration,
    buffer_fill: Duration,
    channel_send: Duration,
}

struct TimingRun {
    results: Vec<WorkerOutput>,
    producer: ProducerStats,
    producer_join: Duration,
    drain: Duration,
    total: Duration,
    max_batch_size: usize,
    buffer_pool_size: usize,
}

fn join_thread<T>(handle: JoinHandle<T>, name: &str) -> Result<T> {
    handle.join().map_err(|payload| {
        anyhow!(
            "thread `{name}` panicked: {}",
            panic_payload_to_string(payload)
        )
    })
}

fn configured_max_batch_size() -> usize {
    std::env::var("ACCUMULATOR_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_BATCH_SIZE)
}

fn configured_buffer_pool_size(num_workers: usize) -> usize {
    std::env::var("ACCUMULATOR_BUFFER_POOL_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| (num_workers * DEFAULT_BUFFER_POOL_MULTIPLIER).max(1))
}

fn channel_batch_count(value_count: u64, max_batch_size: usize) -> u64 {
    value_count.div_ceil(max_batch_size as u64)
}

fn sample_storage_gib(value_count: usize) -> f64 {
    let bytes = value_count * std::mem::size_of::<Sample>();
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn add_sample(
    keys: &mut [KahanState],
    (key, value): Sample,
) -> std::result::Result<(), ExampleError> {
    if (key as usize) >= NUM_KEYS {
        return Err(ExampleError::KeyOutOfRange { key, max: NUM_KEYS });
    }
    keys[key as usize].add(value as f64);
    Ok(())
}

fn random_sample(rng: &mut impl RngExt) -> Sample {
    let key: u8 = rng.random_range(0..NUM_KEYS as u8);
    let value: f32 = rng.random::<f32>();
    (key, value)
}

fn generate_samples() -> (Arc<Vec<Sample>>, Duration) {
    let mut rng = rand::rng();
    let mut samples = Vec::with_capacity(NUM_VALUES);
    let started = Instant::now();

    for _ in 0..NUM_VALUES {
        samples.push(random_sample(&mut rng));
    }

    (Arc::new(samples), started.elapsed())
}

fn create_buffer_pool(
    max_batch_size: usize,
    buffer_pool_size: usize,
) -> Result<(BufferSender, BufferReceiver)> {
    let (sender, receiver) = accumulator::kanal::unbounded::<Msg>();
    for _ in 0..buffer_pool_size {
        sender
            .send(Vec::with_capacity(max_batch_size))
            .context("failed to seed buffer pool")?;
    }
    Ok((sender, receiver))
}

fn return_buffer(sender: &BufferSender, mut buffer: Msg) {
    buffer.clear();
    let _ = sender.send(buffer);
}

fn send_sample_batches(
    sender: accumulator::kanal::Sender<Msg>,
    samples: Arc<Vec<Sample>>,
    buffer_receiver: BufferReceiver,
    max_batch_size: usize,
) -> Result<ProducerStats> {
    let send_started = Instant::now();
    let mut stats = ProducerStats::default();

    for chunk in samples.chunks(max_batch_size) {
        let wait_started = Instant::now();
        let mut batch = buffer_receiver
            .recv()
            .context("buffer pool closed while sending batches")?;
        stats.buffer_wait += wait_started.elapsed();

        let fill_started = Instant::now();
        batch.clear();
        batch.extend_from_slice(chunk);
        stats.buffer_fill += fill_started.elapsed();

        let send_one_started = Instant::now();
        sender.send(batch).context("channel send failed")?;
        stats.channel_send += send_one_started.elapsed();
    }

    thread::yield_now();
    stats.elapsed = send_started.elapsed();
    Ok(stats)
}

fn timed_example_accumulator(
    num_workers: usize,
    label: &str,
    buffer_return: BufferSender,
) -> Result<Accumulator<Msg, WorkerOutput, ExampleError>> {
    Accumulator::new(
        Config {
            num_workers,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(WorkerState::new(NUM_KEYS)) },
        move |state: &mut WorkerState, batch: Msg| -> std::result::Result<(), ExampleError> {
            if state.started_at.is_none() {
                state.started_at = Some(Instant::now());
            }
            state.batches += 1;
            let mut result = Ok(());
            for &sample in &batch {
                if let Err(error) = add_sample(&mut state.keys, sample) {
                    result = Err(error);
                    break;
                }
            }
            return_buffer(&buffer_return, batch);
            result
        },
        |state: WorkerState| -> std::result::Result<WorkerOutput, ExampleError> {
            let start = state.started_at.unwrap_or_else(Instant::now);
            Ok(WorkerOutput {
                keys: state.keys,
                wall_time: start.elapsed(),
                batches: state.batches,
            })
        },
    )
    .context(format!("failed to build accumulator ({label})"))
}

type Storage = Vec<KahanState>;

fn clean_example_accumulator(
    num_workers: usize,
    label: &str,
    buffer_return: BufferSender,
) -> Result<Accumulator<Msg, Storage, ExampleError>> {
    Accumulator::new(
        Config {
            num_workers,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(vec![KahanState::default(); NUM_KEYS]) },
        move |s: &mut Storage, batch: Msg| -> std::result::Result<(), ExampleError> {
            let mut result = Ok(());
            for &sample in &batch {
                if let Err(error) = add_sample(s, sample) {
                    result = Err(error);
                    break;
                }
            }
            return_buffer(&buffer_return, batch);
            result
        },
        |s: Storage| -> std::result::Result<_, ExampleError> { Ok(s) },
    )
    .context(format!("failed to build clean accumulator ({label})"))
}

fn merge_all_keys(results: &[Vec<KahanState>]) -> Vec<KahanState> {
    assert!(!results.is_empty());
    (0..NUM_KEYS)
        .map(|k| {
            results
                .iter()
                .skip(1)
                .fold(results[0][k].clone(), |acc, w| {
                    KahanState::merge(&acc, &w[k])
                })
        })
        .collect()
}

fn run_measurement_baselines(max_batch_size: usize, samples: &[Sample]) {
    let mut keys = vec![KahanState::default(); NUM_KEYS];
    let started = Instant::now();

    for &sample in samples {
        add_sample(&mut keys, sample).expect("generated key should be in range");
    }

    let elapsed = started.elapsed();
    let checksum: f64 = keys.iter().map(KahanState::mean).sum();
    println!(
        "pre-generated direct baseline:
  values processed   : {}
  elapsed            : {elapsed:?} ({:.2} M values/s)
  checksum           : {checksum:.12}
",
        samples.len(),
        million_msgs_per_sec(samples.len() as u64, elapsed)
    );

    let mut keys = vec![KahanState::default(); NUM_KEYS];
    let started = Instant::now();

    for chunk in samples.chunks(max_batch_size) {
        let batch = chunk.to_vec();
        for sample in batch {
            add_sample(&mut keys, sample).expect("generated key should be in range");
        }
    }

    let elapsed = started.elapsed();
    let checksum: f64 = keys.iter().map(KahanState::mean).sum();
    println!(
        "pre-generated batched baseline:
  values processed   : {}
  channel batches    : {}
  elapsed            : {elapsed:?} ({:.2} M values/s)
  checksum           : {checksum:.12}
",
        samples.len(),
        channel_batch_count(samples.len() as u64, max_batch_size),
        million_msgs_per_sec(samples.len() as u64, elapsed)
    );

    let mut keys = vec![KahanState::default(); NUM_KEYS];
    let mut batch = Vec::with_capacity(max_batch_size);
    let started = Instant::now();

    for chunk in samples.chunks(max_batch_size) {
        batch.clear();
        batch.extend_from_slice(chunk);
        for &sample in &batch {
            add_sample(&mut keys, sample).expect("generated key should be in range");
        }
    }

    let elapsed = started.elapsed();
    let checksum: f64 = keys.iter().map(KahanState::mean).sum();
    println!(
        "pre-generated reused-buffer baseline:
  values processed   : {}
  channel batches    : {}
  elapsed            : {elapsed:?} ({:.2} M values/s)
  checksum           : {checksum:.12}
",
        samples.len(),
        channel_batch_count(samples.len() as u64, max_batch_size),
        million_msgs_per_sec(samples.len() as u64, elapsed)
    );
}

fn run_timing_all(max_batch_size: usize, samples: Arc<Vec<Sample>>) -> Result<()> {
    for n in [1usize, 2, 4, 8] {
        let label = if n == 1 {
            String::from("1 consumer")
        } else {
            format!("{n} consumers")
        };

        let total_started = Instant::now();
        let buffer_pool_size = configured_buffer_pool_size(n);
        let (buffer_return, buffer_receiver) =
            create_buffer_pool(max_batch_size, buffer_pool_size)?;
        let acc = timed_example_accumulator(n, &label, buffer_return)?;

        let sender = acc.sender();
        let producer_name = format!("producer-{n}");
        let samples = Arc::clone(&samples);
        let producer = thread::Builder::new()
            .name(producer_name.clone())
            .spawn(move || -> Result<ProducerStats> {
                send_sample_batches(sender, samples, buffer_receiver, max_batch_size)
            })
            .with_context(|| format!("spawn `{}`", producer_name.clone()))?;

        let producer_join_started = Instant::now();
        let producer = join_thread(producer, &producer_name)??;
        let producer_join = producer_join_started.elapsed();

        let drain_started = Instant::now();
        let results = acc.join().context("accumulator join failed")?;
        let drain = drain_started.elapsed();

        let timing = TimingRun {
            results,
            producer,
            producer_join,
            drain,
            total: total_started.elapsed(),
            max_batch_size,
            buffer_pool_size,
        };
        print_timing_summary(&label, &timing);
        println!();
    }
    Ok(())
}

fn run_single(max_batch_size: usize, samples: Arc<Vec<Sample>>) -> Result<()> {
    const N: usize = 1;

    let buffer_pool_size = configured_buffer_pool_size(N);
    let (buffer_return, buffer_receiver) = create_buffer_pool(max_batch_size, buffer_pool_size)?;
    let acc = clean_example_accumulator(N, "single", buffer_return)?;
    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        send_sample_batches(sender, samples, buffer_receiver, max_batch_size)?;
        Ok(())
    });

    join_thread(producer, "producer")??;
    let results = acc.join().context("accumulator join failed")?;
    let combined = merge_all_keys(&results);

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, 1 consumer");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn run_double(max_batch_size: usize, samples: Arc<Vec<Sample>>) -> Result<()> {
    const N: usize = 2;

    let buffer_pool_size = configured_buffer_pool_size(N);
    let (buffer_return, buffer_receiver) = create_buffer_pool(max_batch_size, buffer_pool_size)?;
    let acc = clean_example_accumulator(N, "double", buffer_return)?;

    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        send_sample_batches(sender, samples, buffer_receiver, max_batch_size)?;
        Ok(())
    });

    join_thread(producer, "producer")??;
    let results = acc.join().context("accumulator join failed")?;

    let combined = merge_all_keys(&results);

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, {N} consumers");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn run_four(max_batch_size: usize, samples: Arc<Vec<Sample>>) -> Result<()> {
    const N: usize = 4;

    let buffer_pool_size = configured_buffer_pool_size(N);
    let (buffer_return, buffer_receiver) = create_buffer_pool(max_batch_size, buffer_pool_size)?;
    let acc = clean_example_accumulator(N, "four", buffer_return)?;

    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        send_sample_batches(sender, samples, buffer_receiver, max_batch_size)?;
        Ok(())
    });

    join_thread(producer, "producer")??;
    let results = acc.join().context("accumulator join failed")?;

    let combined = merge_all_keys(&results);

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, {N} consumers");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn run_eight(max_batch_size: usize, samples: Arc<Vec<Sample>>) -> Result<()> {
    const N: usize = 8;

    let buffer_pool_size = configured_buffer_pool_size(N);
    let (buffer_return, buffer_receiver) = create_buffer_pool(max_batch_size, buffer_pool_size)?;
    let acc = clean_example_accumulator(N, "eight", buffer_return)?;

    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        send_sample_batches(sender, samples, buffer_receiver, max_batch_size)?;
        Ok(())
    });

    join_thread(producer, "producer")??;
    let results = acc.join().context("accumulator join failed")?;

    let combined = merge_all_keys(&results);

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, {N} consumers");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn worker_message_count(worker: &WorkerOutput) -> u64 {
    worker.keys.iter().map(|k| k.count).sum()
}

fn million_msgs_per_sec(count: u64, duration: Duration) -> f64 {
    if duration.is_zero() {
        0.0
    } else {
        count as f64 / duration.as_secs_f64() / 1_000_000.0
    }
}

fn duration_per_msg(duration: Duration, count: u64) -> Duration {
    if count == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(duration.as_secs_f64() / count as f64)
    }
}

fn print_timing_summary(label: &str, timing: &TimingRun) {
    let results = &timing.results;
    let worker_wall_sum: Duration = results.iter().map(|w| w.wall_time).sum();
    let min_wall: Duration = results
        .iter()
        .map(|w| w.wall_time)
        .min()
        .unwrap_or_default();
    let max_wall: Duration = results
        .iter()
        .map(|w| w.wall_time)
        .max()
        .unwrap_or_default();
    let worker_counts: Vec<u64> = results.iter().map(worker_message_count).collect();
    let total_count: u64 = worker_counts.iter().sum();
    let batch_count = channel_batch_count(total_count, timing.max_batch_size);
    let worker_batches: Vec<u64> = results.iter().map(|w| w.batches).collect();
    let min_count = worker_counts.iter().copied().min().unwrap_or_default();
    let max_count = worker_counts.iter().copied().max().unwrap_or_default();
    let count_spread = max_count.saturating_sub(min_count);

    println!(
        "{label} timing summary:
  values processed   : {total_count}
  channel batches    : {batch_count} (max batch size: {})
  buffer pool size   : {}
  total elapsed      : {:?} ({:.2} M values/s)
  producer send      : {:?} ({:.2} M values/s)
  producer wait      : {:?}
  producer fill      : {:?}
  channel send       : {:?}
  producer joined    : {:?}
  post-feed drain    : {:?} ({:.2} M values/s)
  worker wall sum    : {:?}
  worker wall range  : {:?} .. {:?}
  worker avg / value : {:?}
  end-to-end / value : {:?}
  worker values      : {:?} (min={min_count}, max={max_count}, spread={count_spread})
  worker batches     : {:?}
",
        timing.max_batch_size,
        timing.buffer_pool_size,
        timing.total,
        million_msgs_per_sec(total_count, timing.total),
        timing.producer.elapsed,
        million_msgs_per_sec(total_count, timing.producer.elapsed),
        timing.producer.buffer_wait,
        timing.producer.buffer_fill,
        timing.producer.channel_send,
        timing.producer_join,
        timing.drain,
        million_msgs_per_sec(total_count, timing.drain),
        worker_wall_sum,
        min_wall,
        max_wall,
        duration_per_msg(worker_wall_sum, total_count),
        duration_per_msg(timing.total, total_count),
        worker_counts,
        worker_batches,
    );
}
