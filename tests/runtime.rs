use piper::{
    CsvTelemetryConfig, IntoStageSpec, Piper, PiperConfig, PiperError, Stage, StageContext, anchor,
    inline_stage, stage,
};
use std::fs;
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
        csv_telemetry: None,
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
        vec![
            anchor(stage("double", Double)).max_threads(1),
            stage("format", FormatValue),
        ],
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
        vec![
            stage("left", Pass),
            anchor(stage("right", Pass)).max_threads(1),
        ],
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();

    assert_eq!(telemetry.links.len(), 3);
    assert_eq!(telemetry.stages.len(), 2);
    assert_eq!(telemetry.stages[0].active_threads, 1);
    assert_eq!(telemetry.stages[1].active_threads, 1);
    assert_eq!(telemetry.anchor.stage_index, 1);
    assert_eq!(telemetry.anchor.max_threads, 1);
    assert!(telemetry.stages.iter().any(|stage| stage.is_anchor));
    assert!(telemetry.parked_threads >= 2);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn abort_skips_inline_builder_cleanup() {
    let cleaned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleaned_for_stage = std::sync::Arc::clone(&cleaned);

    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        vec![
            anchor(
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
                }),
            )
            .max_threads(1)
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
        config(),
        vec![anchor(stage("fail", Fail)).max_threads(1)],
    )
    .unwrap();

    piper.sender().send(1).unwrap();
    let error = piper.join().expect_err("process error should fail join");
    assert!(matches!(error, PiperError::UserProcess { .. }));
}

#[test]
fn piper_requires_exactly_one_anchor() {
    let no_anchor = Piper::<u32, u32, TestError>::start(config(), vec![stage("pass", Pass)]);
    let Err(no_anchor) = no_anchor else {
        panic!("missing anchor should fail");
    };
    assert!(matches!(
        no_anchor,
        PiperError::InvalidAnchorCount { count: 0 }
    ));

    let two_anchors = Piper::<u32, u32, TestError>::start(
        config(),
        vec![
            anchor(stage("left", Pass)).max_threads(1),
            anchor(stage("right", Pass)).max_threads(1),
        ],
    );
    let Err(two_anchors) = two_anchors else {
        panic!("multiple anchors should fail");
    };
    assert!(matches!(
        two_anchors,
        PiperError::InvalidAnchorCount { count: 2 }
    ));
}

#[test]
fn csv_telemetry_writes_wide_rows_and_existing_path_fails() {
    let path = std::env::temp_dir().join(format!(
        "piper_test_{}_{}.csv",
        std::process::id(),
        std::thread::current().name().unwrap_or("runtime")
    ));
    let _ = fs::remove_file(&path);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(CsvTelemetryConfig::new(&path).interval(Duration::from_millis(5))),
            ..config()
        },
        vec![anchor(stage("pass", Pass)).max_threads(1)],
    )
    .unwrap();
    piper.sender().send(1).unwrap();
    assert_eq!(
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        1
    );
    piper.shutdown();
    piper.join().unwrap();

    let csv = fs::read_to_string(&path).unwrap();
    assert!(csv.starts_with("elapsed_ms,shutdown_requested"));
    assert!(csv.contains("stage0_busy_ratio"));
    assert!(csv.lines().count() >= 2);

    let existing = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(CsvTelemetryConfig::new(&path)),
            ..config()
        },
        vec![anchor(stage("pass", Pass)).max_threads(1)],
    );
    let Err(existing) = existing else {
        panic!("existing CSV path should fail");
    };
    assert!(matches!(existing, PiperError::Telemetry { .. }));
    let _ = fs::remove_file(&path);
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
