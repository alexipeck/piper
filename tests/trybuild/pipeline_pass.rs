use piper::{PiperConfig, Stage, StageContext, anchor, inline_stage, pipeline, stage};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum MacroError {}

struct Widen;

impl Stage for Widen {
    type Input = u8;
    type Output = u16;
    type Error = MacroError;
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
        ctx.emit(input as u16);
        Ok(())
    }
}

struct Keep;

impl Stage for Keep {
    type Input = u16;
    type Output = u16;
    type Error = MacroError;
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

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        global_worker_cap: Some(4),
        csv_telemetry: None,
    }
}

pipeline! {
    pub struct DirectPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        stages = [anchor(Widen).max_threads(1), Keep];
    }
}

pipeline! {
    pub struct NamedPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        stages = [anchor(stage("widen", Widen)).max_threads(1), stage("keep", Keep)];
    }
}

pipeline! {
    pub struct InlinePipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        stages = [
            anchor(inline_stage(
                "widen",
                || -> std::result::Result<(), MacroError> { Ok(()) },
                |_state: &mut (), input: u8, ctx: &mut StageContext<u16, MacroError>| {
                    ctx.emit(input as u16);
                    Ok(())
                },
            )).max_threads(1),
        ];
    }
}

pipeline! {
    pub struct GraphPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        stages = {
            widen = anchor(Widen).max_threads(1),
            left = stage("left", Keep),
            right = anchor(stage("right", Keep)).fixed_threads(1),
            out = stage("out", Keep),
        };
        graph = {
            input -> widen;
            widen -> [left, right];
            [left, right] -> out;
            out -> output;
        };
    }
}

pipeline! {
    pub struct ExternalPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        stages = {
            external = external_node(u8, u16),
        };
        graph = {
            input -> external;
            external -> output;
        };
    }
}

fn external_run_shape(run: ExternalPipelineRun) {
    let _sender = run.sender();
    let _receiver = run.receiver();
    let _external = run.external.clone();
    run.shutdown();
    run.abort();
    let _telemetry = run.get_telemetry();
    let _join = ExternalPipelineRun::join;
}

fn main() {
    let _ = DirectPipeline::start;
    let _ = NamedPipeline::start;
    let _ = InlinePipeline::start;
    let _ = GraphPipeline::start;
    let _ = ExternalPipeline::start;
    let _ = external_run_shape;
}
