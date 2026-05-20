use piper::{PiperConfig, Stage, StageContext, anchor, inline_stage, pipeline, stage};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;

static INLINE_CLEANUPS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Error)]
enum ExampleError {}

struct AddOne;

impl Stage for AddOne {
    type Input = u32;
    type Output = u32;
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
        ctx.emit(input + 1);
        Ok(())
    }
}

struct Square;

impl Stage for Square {
    type Input = u32;
    type Output = u32;
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
        ctx.emit(input * input);
        Ok(())
    }
}

struct FormatValue;

impl Stage for FormatValue {
    type Input = u32;
    type Output = String;
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
        ctx.emit(format!("value={input}"));
        Ok(())
    }
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(5),
        poll_interval: Duration::from_millis(5),
        scale_cooldown: Duration::from_millis(10),
        add_dwell: Duration::from_millis(20),
        remove_dwell: Duration::from_millis(100),
        low_water: 1,
        high_water: 8,
        csv_telemetry: None,
    }
}

pipeline! {
    pub struct DirectStructPipeline {
        type Input = u32;
        type Output = String;
        type Error = ExampleError;

        config = config();
        stages = [AddOne, anchor(Square).max_threads(1), FormatValue];
    }
}

pipeline! {
    pub struct NamedStagePipeline {
        type Input = u32;
        type Output = String;
        type Error = ExampleError;

        config = config();
        stages = [
            stage("add", AddOne),
            anchor(stage("square", Square)).max_threads(1),
            stage("format", FormatValue),
        ];
    }
}

pipeline! {
    pub struct InlineBuilderPipeline {
        type Input = u32;
        type Output = String;
        type Error = ExampleError;

        config = config();
        stages = [
            inline_stage(
                "add",
                || -> std::result::Result<(), ExampleError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<u32, ExampleError>| {
                    ctx.emit(input + 1);
                    Ok(())
                },
            ),
            anchor(inline_stage(
                "square",
                || -> std::result::Result<(), ExampleError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<u32, ExampleError>| {
                    ctx.emit(input * input);
                    Ok(())
                },
            )).max_threads(1),
            inline_stage(
                "format",
                || -> std::result::Result<(), ExampleError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<String, ExampleError>| {
                    ctx.emit(format!("value={input}"));
                    Ok(())
                },
            ).with_cleanup(|_state| {
                INLINE_CLEANUPS.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
        ];
    }
}

fn run_pipeline(
    label: &str,
    piper: piper::Piper<u32, String, ExampleError>,
) -> piper::Result<(), ExampleError> {
    let sender = piper.sender();
    let receiver = piper.receiver();

    for value in 0..4 {
        sender.send(value).expect("pipeline input is open");
    }
    piper.shutdown();

    let mut output = Vec::new();
    for _ in 0..4 {
        output.push(receiver.recv().expect("pipeline output is open"));
    }
    output.sort();
    println!("{label}: {}", output.join(", "));

    piper.join()
}

fn main() -> piper::Result<(), ExampleError> {
    run_pipeline("direct structs", DirectStructPipeline::start()?)?;
    run_pipeline("stage helper", NamedStagePipeline::start()?)?;
    run_pipeline("inline builder", InlineBuilderPipeline::start()?)?;
    println!(
        "inline cleanup calls: {}",
        INLINE_CLEANUPS.load(Ordering::Relaxed)
    );
    Ok(())
}
