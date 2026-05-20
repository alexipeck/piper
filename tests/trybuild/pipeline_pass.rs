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
        scale_cooldown: Duration::from_millis(1),
        add_dwell: Duration::from_millis(1),
        remove_dwell: Duration::from_millis(1),
        low_water: 1,
        high_water: 8,
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

fn main() {
    let _ = DirectPipeline::start;
    let _ = NamedPipeline::start;
    let _ = InlinePipeline::start;
}
