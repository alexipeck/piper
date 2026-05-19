use piper::{Piper, PiperConfig, PiperError, StageContext, inline_stage_no_cleanup};
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

#[test]
fn piper_streams_outputs_and_drains_on_shutdown() {
    let piper = Piper::<u32, String, TestError>::start(
        config(),
        vec![
            inline_stage_no_cleanup(
                "double",
                || -> std::result::Result<(), TestError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<u32, TestError>| {
                    ctx.emit(input * 2);
                    Ok(())
                },
            ),
            inline_stage_no_cleanup(
                "format",
                || -> std::result::Result<(), TestError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<String, TestError>| {
                    ctx.emit(format!("value={input}"));
                    Ok(())
                },
            ),
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
fn snapshot_reports_operational_state() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        vec![
            inline_stage_no_cleanup(
                "left",
                || -> std::result::Result<(), TestError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<u32, TestError>| {
                    ctx.emit(input);
                    Ok(())
                },
            ),
            inline_stage_no_cleanup(
                "right",
                || -> std::result::Result<(), TestError> { Ok(()) },
                |_state: &mut (), input: u32, ctx: &mut StageContext<u32, TestError>| {
                    ctx.emit(input);
                    Ok(())
                },
            ),
        ],
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let snapshot = piper.snapshot();

    assert_eq!(snapshot.links.len(), 3);
    assert_eq!(snapshot.stages.len(), 2);
    assert_eq!(snapshot.stages[0].active_threads, 1);
    assert_eq!(snapshot.stages[1].active_threads, 1);
    assert!(snapshot.parked_threads >= 2);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn abort_skips_cleanup() {
    let cleaned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleaned_for_stage = std::sync::Arc::clone(&cleaned);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            compute_stage: 0,
            ..config()
        },
        vec![piper::inline_stage(
            "cleanup",
            || -> std::result::Result<(), TestError> { Ok(()) },
            |_state: &mut (), input: u32, ctx: &mut StageContext<u32, TestError>| {
                ctx.emit(input);
                Ok(())
            },
            move |_state| {
                cleaned_for_stage.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            },
        )],
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
        vec![inline_stage_no_cleanup(
            "fail",
            || -> std::result::Result<(), TestError> { Ok(()) },
            |_state: &mut (), _input: u32, _ctx: &mut StageContext<u32, TestError>| {
                Err(TestError::Test)
            },
        )],
    )
    .unwrap();

    piper.sender().send(1).unwrap();
    let error = piper.join().expect_err("process error should fail join");
    assert!(matches!(error, PiperError::UserProcess { .. }));
}
