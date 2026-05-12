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
const NUM_VALUES: usize = 10_000_000;

type Msg = (u8, f32);

fn main() -> Result<()> {
    println!("===========================================================");
    println!("Example 1: 1-worker and 2-worker accumulators with timing");
    println!("===========================================================\n");
    run_timing().context("run_timing failed")?;

    println!("\n===========================================================");
    println!("Example 2: single consumer");
    println!("===========================================================\n");
    run_single().context("run_single failed")?;

    println!("\n===========================================================");
    println!("Example 3: double consumer");
    println!("===========================================================\n");
    run_double().context("run_double failed")?;

    println!("\n===========================================================");
    println!("Example 4: eight consumers");
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

fn run_timing() -> Result<()> {
    let cancel = Arc::new(RwLock::new(false));
    let poll_interval = Duration::from_millis(100);

    let acc1: Accumulator<Msg, WorkerOutput, ExampleError> = Accumulator::new(
        Config {
            num_workers: 1,
            poll_interval,
            cancel: Arc::clone(&cancel),
        },
        || -> std::result::Result<_, ExampleError> { Ok(WorkerState::new(NUM_KEYS)) },
        |state: &mut WorkerState, (key, value): Msg| -> std::result::Result<(), ExampleError> {
            if (key as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange {
                    key,
                    max: NUM_KEYS,
                });
            }
            let t0 = Instant::now();
            state.started_at.get_or_insert(t0);
            state.keys[key as usize].add(value as f64);
            state.handle_time += t0.elapsed();
            Ok(())
        },
        |state: WorkerState| -> std::result::Result<_, ExampleError> {
            let start = state.started_at.unwrap_or_else(Instant::now);
            Ok(WorkerOutput {
                keys: state.keys,
                handle_time: state.handle_time,
                wall_time: start.elapsed(),
            })
        },
    )
    .context("failed to build acc1")?;

    let acc2: Accumulator<Msg, WorkerOutput, ExampleError> = Accumulator::new(
        Config {
            num_workers: 2,
            poll_interval,
            cancel: Arc::clone(&cancel),
        },
        || -> std::result::Result<_, ExampleError> { Ok(WorkerState::new(NUM_KEYS)) },
        |state: &mut WorkerState, (key, value): Msg| -> std::result::Result<(), ExampleError> {
            if (key as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange {
                    key,
                    max: NUM_KEYS,
                });
            }
            let t0 = Instant::now();
            state.started_at.get_or_insert(t0);
            state.keys[key as usize].add(value as f64);
            state.handle_time += t0.elapsed();
            Ok(())
        },
        |state: WorkerState| -> std::result::Result<_, ExampleError> {
            let start = state.started_at.unwrap_or_else(Instant::now);
            Ok(WorkerOutput {
                keys: state.keys,
                handle_time: state.handle_time,
                wall_time: start.elapsed(),
            })
        },
    )
    .context("failed to build acc2")?;

    let s1 = acc1.sender();
    let s2 = acc2.sender();

    let producer = thread::spawn(move || -> Result<()> {
        let mut rng = rand::rng();
        for _ in 0..NUM_VALUES {
            let key: u8 = rng.random_range(0..NUM_KEYS as u8);
            let value: f32 = rng.random::<f32>();
            s1.send((key, value)).context("acc1 channel send failed")?;
            s2.send((key, value)).context("acc2 channel send failed")?;
        }
        thread::yield_now();
        Ok(())
    });

    join_thread(producer, "producer")??;

    let acc1_handle = thread::spawn(move || -> Result<(Vec<WorkerOutput>, Duration)> {
        let t0 = Instant::now();
        let r = acc1.join().context("acc1 join failed")?;
        Ok((r, t0.elapsed()))
    });
    let acc2_handle = thread::spawn(move || -> Result<(Vec<WorkerOutput>, Duration)> {
        let t0 = Instant::now();
        let r = acc2.join().context("acc2 join failed")?;
        Ok((r, t0.elapsed()))
    });

    let (results1, drain1) = join_thread(acc1_handle, "acc1-joiner")??;
    let (results2, drain2) = join_thread(acc2_handle, "acc2-joiner")??;

    let combined1: Vec<KahanState> = results1[0].keys.clone();
    let combined2: Vec<KahanState> = (0..NUM_KEYS)
        .map(|k| KahanState::merge(&results2[0].keys[k], &results2[1].keys[k]))
        .collect();

    println!(
        "Kahan compensated summation over {NUM_VALUES} random f32 values across {NUM_KEYS} keys\n"
    );

    print_per_thread("1-worker accumulator", &results1);
    print_combined("1-worker combined", &combined1);
    print_timing_summary("1-worker", &results1, drain1);

    print_per_thread("2-worker accumulator", &results2);
    print_combined("2-worker combined", &combined2);
    print_timing_summary("2-worker", &results2, drain2);

    println!("comparison of combined means:");
    println!(
        "  {:<5} {:>24} {:>24} {:>14}",
        "key", "mean (1 worker)", "mean (2 workers)", "abs diff"
    );
    for k in 0..NUM_KEYS {
        let m1 = combined1[k].mean();
        let m2 = combined2[k].mean();
        println!(
            "  {:<5} {:>24.17} {:>24.17} {:>14.3e}",
            k,
            m1,
            m2,
            (m1 - m2).abs()
        );
    }

    Ok(())
}

fn run_single() -> Result<()> {
    type Storage = Vec<KahanState>;

    let acc: Accumulator<Msg, Storage, ExampleError> = Accumulator::new(
        Config {
            num_workers: 1,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(vec![KahanState::default(); NUM_KEYS]) },
        |s: &mut Storage, (k, v): Msg| -> std::result::Result<(), ExampleError> {
            if (k as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange { key: k, max: NUM_KEYS });
            }
            s[k as usize].add(v as f64);
            Ok(())
        },
        |s: Storage| -> std::result::Result<_, ExampleError> { Ok(s) },
    )
    .context("failed to build accumulator")?;

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
    let combined = &results[0];

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, 1 consumer");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn run_double() -> Result<()> {
    const NUM_WORKERS: usize = 2;
    type Storage = Vec<KahanState>;

    let acc: Accumulator<Msg, Storage, ExampleError> = Accumulator::new(
        Config {
            num_workers: NUM_WORKERS,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(vec![KahanState::default(); NUM_KEYS]) },
        |s: &mut Storage, (k, v): Msg| -> std::result::Result<(), ExampleError> {
            if (k as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange { key: k, max: NUM_KEYS });
            }
            s[k as usize].add(v as f64);
            Ok(())
        },
        |s: Storage| -> std::result::Result<_, ExampleError> { Ok(s) },
    )
    .context("failed to build accumulator")?;

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

    let combined: Vec<KahanState> = (0..NUM_KEYS)
        .map(|k| KahanState::merge(&results[0][k], &results[1][k]))
        .collect();

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, {NUM_WORKERS} consumers");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn run_eight() -> Result<()> {
    const NUM_WORKERS: usize = 8;
    type Storage = Vec<KahanState>;

    let acc: Accumulator<Msg, Storage, ExampleError> = Accumulator::new(
        Config {
            num_workers: NUM_WORKERS,
            poll_interval: Duration::from_millis(100),
            cancel: Arc::new(RwLock::new(false)),
        },
        || -> std::result::Result<_, ExampleError> { Ok(vec![KahanState::default(); NUM_KEYS]) },
        |s: &mut Storage, (k, v): Msg| -> std::result::Result<(), ExampleError> {
            if (k as usize) >= NUM_KEYS {
                return Err(ExampleError::KeyOutOfRange { key: k, max: NUM_KEYS });
            }
            s[k as usize].add(v as f64);
            Ok(())
        },
        |s: Storage| -> std::result::Result<_, ExampleError> { Ok(s) },
    )
    .context("failed to build accumulator")?;

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

    let combined: Vec<KahanState> = (0..NUM_KEYS)
        .map(|k| {
            results
                .iter()
                .skip(1)
                .fold(results[0][k].clone(), |acc, w| {
                    KahanState::merge(&acc, &w[k])
                })
        })
        .collect();

    println!("{NUM_VALUES} f32 values across {NUM_KEYS} keys, {NUM_WORKERS} consumers");
    for (k, st) in combined.iter().enumerate() {
        println!("  key {k}: count={:>8}, mean={:.17}", st.count, st.mean());
    }
    Ok(())
}

fn print_per_thread(label: &str, results: &[WorkerOutput]) {
    println!("{label} per-thread state:");
    for (i, w) in results.iter().enumerate() {
        println!(
            "  worker {i}: handle_time={:?}, wall_time={:?}",
            w.handle_time, w.wall_time
        );
        print_state_table(&w.keys, "    ");
    }
    println!();
}

fn print_combined(label: &str, combined: &[KahanState]) {
    println!("{label} state:");
    print_state_table(combined, "  ");
    println!();
}

fn print_state_table(state: &[KahanState], indent: &str) {
    println!(
        "{indent}{:<5} {:>10} {:>24} {:>14} {:>24}",
        "key", "count", "sum", "compensation", "mean"
    );
    for (k, st) in state.iter().enumerate() {
        println!(
            "{indent}{:<5} {:>10} {:>24.17} {:>14.3e} {:>24.17}",
            k,
            st.count,
            st.sum,
            st.compensation,
            st.mean()
        );
    }
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
