use piper::{
    BufferLease, PiperConfig, PiperSnapshot, Stage, StageContext, StageExt, WaterState, pipeline,
};
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;

const BATCH_COUNT: usize = 1_500;
const BATCH_SIZE: usize = 2_048;
const COMPUTE_ROUNDS: usize = 6_144;
const EMIT_ROUNDS: usize = 12_288;
const TELEMETRY_INTERVAL: Duration = Duration::from_millis(250);

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
    type Output = Batch;
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
        let mut output = Vec::with_capacity(input.len());
        for &value in input.iter() {
            let mut acc = value;
            for round in 0..EMIT_ROUNDS {
                acc = acc
                    .wrapping_add(0xa076_1d64_78bd_642f ^ round as u64)
                    .rotate_right(((acc >> 3) & 31) as u32)
                    .wrapping_mul(0xe703_7ed1_a0b4_28db);
            }
            output.push(acc);
        }
        ctx.emit(output);
        Ok(())
    }
}

pipeline! {
    pub struct ScalingPipeline {
        type Input = Batch;
        type Output = Batch;
        type Error = ExampleError;

        config = PiperConfig {
            sample_interval: Duration::from_millis(10),
            poll_interval: Duration::from_millis(5),
            scale_cooldown: Duration::from_millis(20),
            add_dwell: Duration::from_millis(40),
            remove_dwell: Duration::from_millis(500),
            low_water: 2,
            high_water: 24,
            compute_stage: 2,
            compute_threads: 2,
        };

        stages = [
            Prepare.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            Normalize.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            Compute.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            Emit,
        ];
    }
}

fn main() -> piper::Result<(), ExampleError> {
    let piper = ScalingPipeline::start()?;
    let sender = piper.sender();
    let receiver = piper.receiver();
    let started = Instant::now();

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
    let mut received_items = 0usize;
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
                received_items += batch.len();
                drop(batch);
            }
            Err(piper::RecvOutputError::Timeout) => {}
            Err(piper::RecvOutputError::Closed) => break,
            Err(piper::RecvOutputError::TypeMismatch) => {
                panic!("pipeline output type mismatch");
            }
        }

        if next_telemetry.elapsed() >= TELEMETRY_INTERVAL {
            print_telemetry(started.elapsed(), &piper.get_telemetry(), received_batches);
            next_telemetry = Instant::now();
        }
    }

    if let Some(producer) = producer {
        producer.join().expect("producer thread should not panic");
    }

    piper.shutdown();
    piper.join()?;
    let elapsed = started.elapsed();
    println!("completed {received_batches} batches / {received_items} items in {elapsed:?}");
    Ok(())
}

fn print_telemetry(elapsed: Duration, telemetry: &PiperSnapshot, received_batches: usize) {
    let stages = telemetry
        .stages
        .iter()
        .map(|stage| format!("{}:{}", stage.name, stage.active_threads))
        .collect::<Vec<_>>()
        .join(", ");
    let links = telemetry
        .links
        .iter()
        .map(|link| format!("L{}:{}:{}", link.index, link.len, water_label(link.state)))
        .collect::<Vec<_>>()
        .join(", ");

    println!(
        "[{:>6.2}s] received={received_batches}/{BATCH_COUNT} parked={} pending={} stages=[{stages}] links=[{links}]",
        elapsed.as_secs_f64(),
        telemetry.parked_threads,
        telemetry.pending_scale_operation,
    );
}

fn water_label(state: WaterState) -> &'static str {
    match state {
        WaterState::Starved => "starved",
        WaterState::BelowLowWater => "low",
        WaterState::Nominal => "ok",
        WaterState::AboveHighWater => "high",
    }
}
