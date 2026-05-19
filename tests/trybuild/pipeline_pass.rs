use piper::{PiperConfig, StageContext, inline_stage_no_cleanup, pipeline};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum MacroError {}

pipeline! {
    pub struct MacroPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = PiperConfig {
            sample_interval: Duration::from_millis(1),
            poll_interval: Duration::from_millis(1),
            scale_cooldown: Duration::from_millis(1),
            add_dwell: Duration::from_millis(1),
            remove_dwell: Duration::from_millis(1),
            low_water: 1,
            high_water: 8,
            compute_stage: 0,
            compute_threads: 1,
        };

        stages = [
            inline_stage_no_cleanup(
                "widen",
                || -> std::result::Result<(), MacroError> { Ok(()) },
                |_state: &mut (), input: u8, ctx: &mut StageContext<u16, MacroError>| {
                    ctx.emit(input as u16);
                    Ok(())
                },
            ),
        ];
    }
}

fn main() {
    let _ = MacroPipeline::start;
}
