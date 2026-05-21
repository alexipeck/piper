use piper::{
    BufferLease, CsvTelemetryConfig, PiperConfig, Stage, StageContext, StageExt, anchor, pipeline,
};
use std::io::{self, Write};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const BATCH_COUNT: usize = 15_000;
const BATCH_SIZE: usize = 2_048;
const COMPUTE_ROUNDS: usize = 12_288;
const TELEMETRY_INTERVAL: Duration = Duration::from_millis(100);
const MANAGER_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
const CSV_TELEMETRY_INTERVAL: Duration = MANAGER_SAMPLE_INTERVAL;

type Batch = Vec<u64>;
type BatchLease = BufferLease<Vec<u64>>;

#[derive(Debug, Error)]
enum ExampleError {}

struct Prepare;

impl Stage for Prepare {
    type Input = Batch;
    type Output = BatchLease;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        let mut output = ctx.acquire_output();
        output.extend(
            input
                .into_iter()
                .map(|value| value.wrapping_mul(31).rotate_left(7) ^ 0x9e37_79b9_7f4a_7c15),
        );
        ctx.emit(output);
        Ok(())
    }
}

struct Normalize;

impl Stage for Normalize {
    type Input = BatchLease;
    type Output = BatchLease;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        let mut output = ctx.acquire_output();
        output.extend(input.iter().map(|value| {
            value
                .wrapping_add(0xa076_1d64_78bd_642f)
                .rotate_right((value & 31) as u32)
        }));
        ctx.emit(output);
        Ok(())
    }
}

struct Compute;

impl Stage for Compute {
    type Input = BatchLease;
    type Output = BatchLease;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        let mut output = ctx.acquire_output();
        for &value in input.iter() {
            let mut acc = value;
            for round in 0..COMPUTE_ROUNDS {
                acc = acc
                    .wrapping_mul(0x9e37_79b1_85eb_ca87)
                    .rotate_left(((acc ^ round as u64) & 31) as u32)
                    ^ (round as u64).wrapping_mul(0xc2b2_ae3d_27d4_eb4f);
            }
            output.push(acc);
        }
        ctx.emit(output);
        Ok(())
    }
}

struct Emit;

impl Stage for Emit {
    type Input = BatchLease;
    type Output = BatchLease;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        ctx.emit(input);
        Ok(())
    }
}

pipeline! {
    pub struct ScalingPipeline {
        type Input = Batch;
        type Output = BatchLease;
        type Error = ExampleError;

        config = config();

        stages = [
            Prepare.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            Normalize.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            anchor(Compute)
                .max_threads(max_parallelism())
                .initial_threads(half_max_parallelism())
                .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            Emit,
        ];
    }
}

fn max_parallelism() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1)
}

fn half_max_parallelism() -> usize {
    max_parallelism().div_ceil(2).max(1)
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: MANAGER_SAMPLE_INTERVAL,
        poll_interval: Duration::from_millis(5),
        global_worker_cap: None,
        csv_telemetry: Some(
            CsvTelemetryConfig::new(format!(
                "piper_{}.csv",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ))
            .interval(CSV_TELEMETRY_INTERVAL),
        ),
    }
}

fn main() -> piper::Result<(), ExampleError> {
    let piper = ScalingPipeline::start()?;
    let sender = piper.sender();
    let receiver = piper.receiver();

    let producer = thread::spawn(move || {
        for batch_index in 0..BATCH_COUNT {
            let start = (batch_index * BATCH_SIZE) as u64;
            let batch: Vec<_> = (0..BATCH_SIZE)
                .map(|offset| start + offset as u64)
                .collect();
            sender.send(batch).expect("pipeline input is open");
        }
    });

    let mut received_batches = 0usize;
    let mut producer = Some(producer);
    let mut producer_joined = false;
    let mut next_telemetry = Instant::now();

    while received_batches < BATCH_COUNT {
        if !producer_joined && producer.as_ref().is_some_and(|handle| handle.is_finished()) {
            producer
                .take()
                .expect("producer exists")
                .join()
                .expect("producer thread should not panic");
            producer_joined = true;
        }

        match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(batch) => {
                received_batches += 1;
                drop(batch);
            }
            Err(piper::RecvOutputError::Timeout) => {}
            Err(piper::RecvOutputError::Closed) => break,
            Err(piper::RecvOutputError::TypeMismatch) => {
                panic!("pipeline output type mismatch");
            }
        }

        if next_telemetry.elapsed() >= TELEMETRY_INTERVAL {
            print_progress(received_batches);
            next_telemetry = Instant::now();
        }
    }

    print_progress(received_batches);
    println!();

    if let Some(producer) = producer {
        producer.join().expect("producer thread should not panic");
    }

    piper.shutdown();
    piper.join()?;
    Ok(())
}

fn print_progress(received_batches: usize) {
    let progress = (received_batches as f64 / BATCH_COUNT as f64) * 100.0;
    print!("\r{:>6.2}%", progress);
    io::stdout().flush().expect("flush progress output");
}
