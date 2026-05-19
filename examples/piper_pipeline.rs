use piper::{BufferLease, PiperConfig, StageContext, inline_stage_no_cleanup, pipeline};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum ExampleError {}

pipeline! {
    pub struct ExamplePipeline {
        type Input = u32;
        type Output = String;
        type Error = ExampleError;

        config = PiperConfig {
            sample_interval: Duration::from_millis(10),
            poll_interval: Duration::from_millis(10),
            scale_cooldown: Duration::from_millis(10),
            add_dwell: Duration::from_millis(10),
            remove_dwell: Duration::from_millis(50),
            low_water: 1,
            high_water: 4,
            compute_stage: 1,
            compute_threads: 2,
        };

        stages = [
            inline_stage_no_cleanup(
                "batch",
                || -> std::result::Result<(), ExampleError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<BufferLease<Vec<u32>>, ExampleError>| -> std::result::Result<(), ExampleError> {
                    let mut batch = ctx.acquire_output();
                    batch.push(input);
                    ctx.emit(batch);
                    Ok(())
                },
            ).with_reusable_output(|| Vec::<u32>::with_capacity(16)),
            inline_stage_no_cleanup(
                "format",
                || -> std::result::Result<(), ExampleError> { Ok(()) },
                |_state: &mut (), input: BufferLease<Vec<u32>>, ctx: &mut StageContext<String, ExampleError>| -> std::result::Result<(), ExampleError> {
                    for value in input.iter() {
                        ctx.emit(format!("value={value}"));
                    }
                    Ok(())
                },
            ),
        ];
    }
}

fn main() -> piper::Result<(), ExampleError> {
    let piper = ExamplePipeline::start()?;
    let sender = piper.sender();
    let receiver = piper.receiver();

    for value in 0..8 {
        sender.send(value).expect("pipeline input is open");
    }

    piper.shutdown();

    for _ in 0..8 {
        println!("{}", receiver.recv().expect("pipeline output is open"));
    }

    let snapshot = piper.snapshot();
    println!(
        "stages={}, parked={}",
        snapshot.stages.len(),
        snapshot.parked_threads
    );

    piper.join()
}
