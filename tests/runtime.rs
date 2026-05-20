use piper::{
    IntoStageSpec, Piper, PiperConfig, PiperError, Stage, StageContext, inline_stage, stage,
};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum TestError {
    #[error("test error")]
    Test,
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        scale_cooldown: Duration::from_millis(1),
        add_dwell: Duration::from_millis(2),
        remove_dwell: Duration::from_millis(5),
        low_water: 1,
        high_water: 2,
        compute_stage: 1,
        compute_threads: 1,
    }
}

struct Double;

impl Stage for Double {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
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
        ctx.emit(input * 2);
        Ok(())
    }
}

struct FormatValue;

impl Stage for FormatValue {
    type Input = u32;
    type Output = String;
    type Error = TestError;
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

struct Pass;

impl Stage for Pass {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
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

#[test]
fn associated_type_stages_stream_outputs_and_default_cleanup_is_optional() {
    let piper = Piper::<u32, String, TestError>::start(
        config(),
        vec![stage("double", Double), stage("format", FormatValue)],
    )
    .unwrap();

    let sender = piper.sender();
    let receiver = piper.receiver();
    for value in 1..=3 {
        sender.send(value).unwrap();
    }

    piper.shutdown();

    let mut outputs = Vec::new();
    for _ in 0..3 {
        outputs.push(receiver.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    piper.join().unwrap();
    outputs.sort();
    assert_eq!(outputs, ["value=2", "value=4", "value=6"]);
}

#[test]
fn get_telemetry_reports_operational_state() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        vec![stage("left", Pass), stage("right", Pass)],
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();

    assert_eq!(telemetry.links.len(), 3);
    assert_eq!(telemetry.stages.len(), 2);
    assert_eq!(telemetry.stages[0].active_threads, 1);
    assert_eq!(telemetry.stages[1].active_threads, 1);
    assert!(telemetry.parked_threads >= 2);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn abort_skips_inline_builder_cleanup() {
    let cleaned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleaned_for_stage = std::sync::Arc::clone(&cleaned);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            compute_stage: 0,
            ..config()
        },
        vec![
            inline_stage(
                "cleanup",
                || -> std::result::Result<(), TestError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<u32, TestError>| {
                    ctx.emit(input);
                    Ok(())
                },
            )
            .with_cleanup(move |_state| {
                cleaned_for_stage.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            })
            .into_stage_spec(),
        ],
    )
    .unwrap();

    piper.abort();
    piper.join().unwrap();

    assert!(!cleaned.load(std::sync::atomic::Ordering::Acquire));
}

#[test]
fn user_process_failure_fails_pipeline() {
    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            compute_stage: 0,
            ..config()
        },
        vec![stage("fail", Fail)],
    )
    .unwrap();

    piper.sender().send(1).unwrap();
    let error = piper.join().expect_err("process error should fail join");
    assert!(matches!(error, PiperError::UserProcess { .. }));
}

struct Fail;

impl Stage for Fail {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        _input: Self::Input,
        _ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        Err(TestError::Test)
    }
}
