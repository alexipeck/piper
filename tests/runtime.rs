use piper::{
    CsvTelemetryConfig, IntoStageSpec, PipelineGraph, PipelineGraphBuilder, Piper, PiperConfig,
    PiperError, Stage, StageContext, anchor, inline_stage, stage,
};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
        global_worker_cap: Some(8),
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

struct CountPass {
    count: Arc<AtomicUsize>,
}

impl Stage for CountPass {
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
        self.count.fetch_add(1, Ordering::Relaxed);
        ctx.emit(input);
        Ok(())
    }
}

fn one_stage_graph<S>(stage_like: S) -> PipelineGraph<u32, u32, TestError>
where
    S: IntoStageSpec<TestError, Input = u32, Output = u32>,
{
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.add_stage(input, stage_like);
    builder.finish(output)
}

#[test]
fn associated_type_stages_stream_outputs_and_default_cleanup_is_optional() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let doubled = builder.add_stage(input, anchor(stage("double", Double)).max_threads(1));
    let output = builder.add_stage(doubled, stage("format", FormatValue));
    let piper = Piper::<u32, String, TestError>::start(config(), builder.finish(output)).unwrap();

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
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let left = builder.add_stage(input, stage("left", Pass));
    let output = builder.add_stage(left, anchor(stage("right", Pass)).max_threads(1));
    let piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();

    assert_eq!(telemetry.links.len(), 3);
    assert_eq!(telemetry.stages.len(), 2);
    assert_eq!(telemetry.stages[0].active_threads, 1);
    assert_eq!(telemetry.stages[1].active_threads, 1);
    assert_eq!(telemetry.anchors.len(), 1);
    assert_eq!(telemetry.anchors[0].stage_index, 1);
    assert_eq!(telemetry.anchors[0].max_threads, 1);
    assert_eq!(telemetry.global_worker_cap, 8);
    assert_eq!(telemetry.total_active_workers, 2);
    assert!(telemetry.stages.iter().any(|stage| stage.is_anchor));
    assert!(telemetry.parked_threads >= 2);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn fixed_anchor_does_not_reserve_parked_worker() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(anchor(stage("fixed", Pass)).fixed_threads(1)),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();

    assert_eq!(telemetry.stages.len(), 1);
    assert_eq!(telemetry.stages[0].active_threads, 1);
    assert!(telemetry.stages[0].is_fixed_anchor);
    assert_eq!(telemetry.parked_threads, 0);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn abort_skips_inline_builder_cleanup() {
    let cleaned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleaned_for_stage = std::sync::Arc::clone(&cleaned);

    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(
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
            .max_threads(1),
        ),
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
        one_stage_graph(anchor(stage("fail", Fail)).max_threads(1)),
    )
    .unwrap();

    piper.sender().send(1).unwrap();
    let error = piper.join().expect_err("process error should fail join");
    assert!(matches!(error, PiperError::UserProcess { .. }));
}

#[test]
fn fork_join_graph_work_shares_and_merges_outputs() {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let fork = builder.add_stage(input, stage("prepare", Pass));
    let merged = builder.link();
    builder.add_stage_to(
        fork,
        stage(
            "left",
            CountPass {
                count: Arc::clone(&left),
            },
        ),
        merged,
    );
    builder.add_stage_to(
        fork,
        anchor(stage(
            "right",
            CountPass {
                count: Arc::clone(&right),
            },
        ))
        .fixed_threads(1),
        merged,
    );
    let piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(merged)).unwrap();

    for value in 0..100 {
        piper.sender().send(value).unwrap();
    }
    piper.shutdown();
    for _ in 0..100 {
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
    }

    assert_eq!(
        left.load(Ordering::Relaxed) + right.load(Ordering::Relaxed),
        100
    );
    assert_eq!(piper.get_telemetry().anchors.len(), 1);
    assert_eq!(piper.get_telemetry().anchors[0].fixed_threads, Some(1));
    piper.join().unwrap();
}

#[test]
fn piper_allows_zero_or_multiple_anchors() {
    let no_anchor =
        Piper::<u32, u32, TestError>::start(config(), one_stage_graph(stage("pass", Pass)))
            .unwrap();
    no_anchor.abort();
    no_anchor.join().unwrap();

    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let left = builder.add_stage(input, anchor(stage("left", Pass)).max_threads(1));
    let output = builder.add_stage(left, anchor(stage("right", Pass)).max_threads(1));
    let two_anchors =
        Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();
    assert_eq!(two_anchors.get_telemetry().anchors.len(), 2);
    two_anchors.abort();
    two_anchors.join().unwrap();
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
        one_stage_graph(anchor(stage("pass", Pass)).max_threads(1)),
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
    assert!(csv.contains("link0_trend"));
    assert!(csv.contains("stage0_service_time_ms"));
    assert!(csv.contains("output_backpressure"));
    assert!(csv.contains("anchor_count"));
    assert!(csv.lines().count() >= 2);

    let existing = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(CsvTelemetryConfig::new(&path)),
            ..config()
        },
        one_stage_graph(anchor(stage("pass", Pass)).max_threads(1)),
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
