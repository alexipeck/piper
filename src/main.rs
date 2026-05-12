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

const NUM_KEYS: usize = 8;
const NUM_VALUES: usize = 10_000_000_0;

type Msg = (u8, f32);

fn main() -> Result<()> {
    println!("===========================================================");
    println!("Example 1: timing sweep (1 / 2 / 4 / 8 consumers, timing only)");
    println!("===========================================================\n");
    run_timing_all().context("run_timing_all failed")?;

    println!("\n===========================================================");
    println!("Example 2: single consumer");
    println!("===========================================================\n");
    run_single().context("run_single failed")?;

    println!("\n===========================================================");
    println!("Example 3: double consumer");
    println!("===========================================================\n");
    run_double().context("run_double failed")?;

    println!("\n===========================================================");
    println!("Example 4: four consumers");
    println!("===========================================================\n");
    run_four().context("run_four failed")?;

    println!("\n===========================================================");
    println!("Example 5: eight consumers");
    println!("===========================================================\n");
    run_eight().context("run_eight failed")?;

    Ok(())
}

struct WorkerState {
    keys: Vec<KahanState>,
    started_at: Option<Instant>,
    handle_time: Duration,
}

impl WorkerState {
    fn new(num_keys: usize) -> Self {
        WorkerState {
            keys: vec![KahanState::default(); num_keys],
            started_at: None,
            handle_time: Duration::ZERO,
        }
    }
}

struct WorkerOutput {
    keys: Vec<KahanState>,
    handle_time: Duration,
    wall_time: Duration,
}

fn join_thread<T>(handle: JoinHandle<T>, name: &str) -> Result<T> {
    handle.join().map_err(|payload| {
        anyhow!(
            "thread `{name}` panicked: {}",
            panic_payload_to_string(payload)
        )
    })
}

fn timed_example_accumulator(
    num_workers: usize,
    label: &str,
) -> Result<Accumulator<Msg, WorkerOutput, ExampleError>> {
    Accumulator::new(
        Config {
            num_workers,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(WorkerState::new(NUM_KEYS)) },
        |state: &mut WorkerState, (key, value): Msg| -> std::result::Result<(), ExampleError> {
            if (key as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange { key, max: NUM_KEYS });
            }
            let t0 = Instant::now();
            state.started_at.get_or_insert(t0);
            state.keys[key as usize].add(value as f64);
            state.handle_time += t0.elapsed();
            Ok(())
        },
        |state: WorkerState| -> std::result::Result<WorkerOutput, ExampleError> {
            let start = state.started_at.unwrap_or_else(Instant::now);
            Ok(WorkerOutput {
                keys: state.keys,
                handle_time: state.handle_time,
                wall_time: start.elapsed(),
            })
        },
    )
    .context(format!("failed to build accumulator ({label})"))
}

type Storage = Vec<KahanState>;

fn clean_example_accumulator(
    num_workers: usize,
    label: &str,
) -> Result<Accumulator<Msg, Storage, ExampleError>> {
    Accumulator::new(
        Config {
            num_workers,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(vec![KahanState::default(); NUM_KEYS]) },
        |s: &mut Storage, (k, v): Msg| -> std::result::Result<(), ExampleError> {
            if (k as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange {
                    key: k,
                    max: NUM_KEYS,
                });
            }
            s[k as usize].add(v as f64);
            Ok(())
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

fn run_timing_all() -> Result<()> {
    for n in [1usize, 2, 4, 8] {
        let label = if n == 1 {
            String::from("1 consumer")
        } else {
            format!("{n} consumers")
        };

        let acc = timed_example_accumulator(n, &label)?;

        let sender = acc.sender();
        let producer_name = format!("producer-{n}");
        let producer = thread::Builder::new()
            .name(producer_name.clone())
            .spawn(move || -> Result<()> {
                let mut rng = rand::rng();
                for _ in 0..NUM_VALUES {
                    let key: u8 = rng.random_range(0..NUM_KEYS as u8);
                    let value: f32 = rng.random::<f32>();
                    sender.send((key, value)).context("channel send failed")?;
                }
                thread::yield_now();
                Ok(())
            })
            .with_context(|| format!("spawn `{}`", producer_name.clone()))?;

        join_thread(producer, &producer_name)??;

        let joiner_name = format!("acc-joiner-{n}");
        let acc_join = thread::Builder::new()
            .name(joiner_name.clone())
            .spawn(move || -> Result<(Vec<WorkerOutput>, Duration)> {
                let t0 = Instant::now();
                let r = acc.join().context("accumulator join failed")?;
                Ok((r, t0.elapsed()))
            })
            .with_context(|| format!("spawn `{}`", joiner_name.clone()))?;

        let (results, drain) = join_thread(acc_join, &joiner_name)??;
        print_timing_summary(&label, &results, drain);
        println!();
    }
    Ok(())
}

fn run_single() -> Result<()> {
    const N: usize = 1;

    let acc = clean_example_accumulator(N, "single")?;
    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        let mut rng = rand::rng();
        for _ in 0..NUM_VALUES {
            let key: u8 = rng.random_range(0..NUM_KEYS as u8);
            let value: f32 = rng.random::<f32>();
            sender.send((key, value)).context("channel send failed")?;
        }
        thread::yield_now();
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

fn run_double() -> Result<()> {
    const N: usize = 2;

    let acc = clean_example_accumulator(N, "double")?;

    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        let mut rng = rand::rng();
        for _ in 0..NUM_VALUES {
            let key: u8 = rng.random_range(0..NUM_KEYS as u8);
            let value: f32 = rng.random::<f32>();
            sender.send((key, value)).context("channel send failed")?;
        }
        thread::yield_now();
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

fn run_four() -> Result<()> {
    const N: usize = 4;

    let acc = clean_example_accumulator(N, "four")?;

    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        let mut rng = rand::rng();
        for _ in 0..NUM_VALUES {
            let key: u8 = rng.random_range(0..NUM_KEYS as u8);
            let value: f32 = rng.random::<f32>();
            sender.send((key, value)).context("channel send failed")?;
        }
        thread::yield_now();
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

fn run_eight() -> Result<()> {
    const N: usize = 8;

    let acc = clean_example_accumulator(N, "eight")?;

    let sender = acc.sender();
    let producer = thread::spawn(move || -> Result<()> {
        let mut rng = rand::rng();
        for _ in 0..NUM_VALUES {
            let key: u8 = rng.random_range(0..NUM_KEYS as u8);
            let value: f32 = rng.random::<f32>();
            sender.send((key, value)).context("channel send failed")?;
        }
        thread::yield_now();
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

fn print_timing_summary(label: &str, results: &[WorkerOutput], drain: Duration) {
    let total_handle: Duration = results.iter().map(|w| w.handle_time).sum();
    let max_wall: Duration = results
        .iter()
        .map(|w| w.wall_time)
        .max()
        .unwrap_or_default();
    let total_count: u64 = results
        .iter()
        .map(|w| w.keys.iter().map(|k| k.count).sum::<u64>())
        .sum();
    let per_msg = if total_count > 0 {
        total_handle / total_count as u32
    } else {
        Duration::ZERO
    };
    println!(
        "{label} timing summary:
  messages processed : {total_count}
  total handle_time  : {total_handle:?} (sum across workers)
  max wall_time      : {max_wall:?} (slowest worker, first message -> end of finalize)
  avg per message    : {per_msg:?}
  post-feed drain    : {drain:?} (time from producer.join() return to all workers finalized)
"
    );
}
