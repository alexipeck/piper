use parking_lot::RwLock;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use thiserror::Error;

pub use kanal;
pub use parking_lot;
pub use piper_macros::pipeline;

type Message = Box<dyn Any + Send>;
type DynAcquire = Arc<dyn Any + Send + Sync>;
type AcquireFn<Out> = dyn Fn() -> Out + Send + Sync + 'static;

pub type Result<T, E = String> = std::result::Result<T, PiperError<E>>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PiperError<E: Debug + Display = String> {
    #[error("worker count must be greater than 0")]
    ZeroWorkers,

    #[error("Piper requires at least one stage")]
    NoStages,

    #[error("Piper graph is invalid: {message}")]
    InvalidGraph { message: String },

    #[error("anchor thread count must be greater than 0")]
    InvalidAnchorThreads,

    #[error("failed to spawn worker thread `{worker}`")]
    SpawnFailed {
        worker: String,
        #[source]
        source: std::io::Error,
    },

    #[error("worker thread `{worker}` panicked: {message}")]
    WorkerPanicked { worker: String, message: String },

    #[error("init closure failed in worker `{worker}`: {error}")]
    UserInit { worker: String, error: E },

    #[error("process closure failed in worker `{worker}`: {error}")]
    UserProcess { worker: String, error: E },

    #[error("cleanup closure failed in worker `{worker}`: {error}")]
    UserCleanup { worker: String, error: E },

    #[error("finalize closure failed in worker `{worker}`: {error}")]
    UserFinalize { worker: String, error: E },

    #[error("internal Piper failure in worker `{worker}`: {message}")]
    Internal { worker: String, message: String },

    #[error("Piper telemetry failure: {message}")]
    Telemetry { message: String },
}

#[derive(Clone)]
pub struct PipeConfig {
    pub num_workers: usize,
    pub poll_interval: Duration,
    pub cancel: Arc<AtomicBool>,
}

pub struct Pipe<Msg, Output, UserErr = String>
where
    UserErr: Debug + Display + Send + 'static,
{
    sender: kanal::Sender<Msg>,
    workers: Vec<(
        String,
        JoinHandle<std::result::Result<Output, PiperError<UserErr>>>,
    )>,
}

impl<Msg, Output, UserErr> Pipe<Msg, Output, UserErr>
where
    Msg: Send + 'static,
    Output: Send + 'static,
    UserErr: Debug + Display + Send + 'static,
{
    pub fn new<Storage, Init, Process, Finalize>(
        config: PipeConfig,
        init: Init,
        process: Process,
        finalize: Finalize,
    ) -> Result<Self, UserErr>
    where
        Storage: Send + 'static,
        Init: Fn() -> std::result::Result<Storage, UserErr> + Send + Sync + 'static,
        Process: Fn(&mut Storage, Msg) -> std::result::Result<(), UserErr> + Send + Sync + 'static,
        Finalize: Fn(Storage) -> std::result::Result<Output, UserErr> + Send + Sync + 'static,
    {
        if config.num_workers == 0 {
            return Err(PiperError::ZeroWorkers);
        }

        let (sender, receiver) = kanal::unbounded::<Msg>();
        let init = Arc::new(init);
        let process = Arc::new(process);
        let finalize = Arc::new(finalize);
        let mut workers = Vec::with_capacity(config.num_workers);

        for worker_index in 0..config.num_workers {
            let name = format!("pipe-worker-{worker_index}");
            let receiver = receiver.clone();
            let cancel = Arc::clone(&config.cancel);
            let poll_interval = config.poll_interval;
            let init = Arc::clone(&init);
            let process = Arc::clone(&process);
            let finalize = Arc::clone(&finalize);
            let thread_name = name.clone();

            let worker = thread::Builder::new()
                .name(name.clone())
                .spawn(
                    move || -> std::result::Result<Output, PiperError<UserErr>> {
                        let mut storage = init().map_err(|error| PiperError::UserInit {
                            worker: thread_name.clone(),
                            error,
                        })?;
                        loop {
                            if cancel.load(Ordering::Acquire) {
                                break;
                            }
                            match receiver.recv_timeout(poll_interval) {
                                Ok(msg) => process(&mut storage, msg).map_err(|error| {
                                    PiperError::UserProcess {
                                        worker: thread_name.clone(),
                                        error,
                                    }
                                })?,
                                Err(kanal::ReceiveErrorTimeout::Timeout) => continue,
                                Err(kanal::ReceiveErrorTimeout::Closed)
                                | Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
                            }
                        }
                        finalize(storage).map_err(|error| PiperError::UserFinalize {
                            worker: thread_name.clone(),
                            error,
                        })
                    },
                )
                .map_err(|source| PiperError::SpawnFailed {
                    worker: name.clone(),
                    source,
                })?;

            workers.push((name, worker));
        }

        drop(receiver);

        Ok(Pipe { sender, workers })
    }

    pub fn sender(&self) -> kanal::Sender<Msg> {
        self.sender.clone()
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn join(self) -> Result<Vec<Output>, UserErr> {
        drop(self.sender);
        let mut results = Vec::with_capacity(self.workers.len());
        for (name, worker) in self.workers {
            let inner_result = worker
                .join()
                .map_err(|payload| PiperError::WorkerPanicked {
                    worker: name.clone(),
                    message: panic_payload_to_string(payload),
                })?;
            let output = inner_result?;
            results.push(output);
        }
        Ok(results)
    }
}

#[derive(Clone, Debug)]
pub struct PiperConfig {
    pub sample_interval: Duration,
    pub poll_interval: Duration,
    pub global_worker_cap: Option<usize>,
    pub csv_telemetry: Option<CsvTelemetryConfig>,
}

impl Default for PiperConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_millis(10),
            poll_interval: Duration::from_millis(10),
            global_worker_cap: None,
            csv_telemetry: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CsvTelemetryConfig {
    pub path: PathBuf,
    pub interval: Duration,
}

impl CsvTelemetryConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            interval: Duration::from_millis(250),
        }
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum QueueTrend {
    Starved,
    FastDraining,
    Draining,
    Stable,
    Growing,
    FastGrowing,
    Runaway,
}

impl QueueTrend {
    pub fn code(self) -> u8 {
        self as u8
    }

    pub fn is_draining(self) -> bool {
        matches!(
            self,
            QueueTrend::Starved | QueueTrend::FastDraining | QueueTrend::Draining
        )
    }

    pub fn is_growing(self) -> bool {
        matches!(
            self,
            QueueTrend::Growing | QueueTrend::FastGrowing | QueueTrend::Runaway
        )
    }
}

#[derive(Clone, Debug)]
pub struct LinkSnapshot {
    pub index: usize,
    pub len: usize,
    pub trend: QueueTrend,
    pub arrival_rate: f64,
    pub drain_rate: f64,
    pub net_rate: f64,
    pub smoothed_len: f64,
}

#[derive(Clone, Debug)]
pub struct StageSnapshot {
    pub index: usize,
    pub name: String,
    pub input_link: usize,
    pub output_link: usize,
    pub active_threads: usize,
    pub processed_count: u64,
    pub busy_ratio: f64,
    pub service_time: Duration,
    pub per_worker_throughput: f64,
    pub desired_workers: usize,
    pub scaling_state: StageScalingState,
    pub is_anchor: bool,
    pub is_fixed_anchor: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageScalingState {
    Eligible,
    Settling,
    Probing,
    BackingOff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnchorProbeState {
    WarmingUp,
    Eligible,
    Probing,
    BackingOff,
    AtMax,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnchorProbeOutcome {
    None,
    Kept,
    Reverted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnchorProbeReason {
    None,
    InputUnderfed,
    OutputBackpressure,
    BudgetPressure,
    SupportUnstable,
    Idle,
}

#[derive(Clone, Debug)]
pub struct AnchorSnapshot {
    pub stage_index: usize,
    pub stage_name: String,
    pub active_threads: usize,
    pub max_threads: usize,
    pub fixed_threads: Option<usize>,
    pub probe_state: AnchorProbeState,
    pub last_probe_outcome: AnchorProbeOutcome,
    pub last_probe_reason: AnchorProbeReason,
}

#[derive(Clone, Debug)]
pub struct PiperSnapshot {
    pub links: Vec<LinkSnapshot>,
    pub stages: Vec<StageSnapshot>,
    pub anchors: Vec<AnchorSnapshot>,
    pub parked_threads: usize,
    pub total_active_workers: usize,
    pub global_worker_cap: usize,
    pub budget_pressure: bool,
    pub output_backpressure: bool,
    pub shutdown_requested: bool,
    pub abort_requested: bool,
    pub pending_scale_operation: bool,
}

#[derive(Debug, Error)]
#[error("Piper input channel is closed")]
pub struct SendInputError;

#[derive(Debug, Error)]
pub enum RecvOutputError {
    #[error("Piper output channel is closed")]
    Closed,
    #[error("Piper output channel timed out")]
    Timeout,
    #[error("Piper output type mismatch")]
    TypeMismatch,
}

#[derive(Debug, Error)]
pub enum TryRecvOutputError {
    #[error("Piper output channel is closed")]
    Closed,
    #[error("Piper output channel is empty")]
    Empty,
    #[error("Piper output type mismatch")]
    TypeMismatch,
}

pub struct PiperSender<In> {
    inner: kanal::Sender<Message>,
    stats: Arc<LinkStats>,
    shutdown: Arc<AtomicBool>,
    _marker: PhantomData<fn(In)>,
}

impl<In> Clone for PiperSender<In> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stats: Arc::clone(&self.stats),
            shutdown: Arc::clone(&self.shutdown),
            _marker: PhantomData,
        }
    }
}

impl<In> PiperSender<In>
where
    In: Send + 'static,
{
    pub fn send(&self, input: In) -> std::result::Result<(), SendInputError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(SendInputError);
        }
        self.inner
            .send(Box::new(input))
            .map_err(|_| SendInputError)?;
        self.stats.arrivals.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.shutdown.load(Ordering::Acquire) || self.inner.is_closed()
    }
}

pub struct PiperReceiver<Out> {
    inner: kanal::Receiver<Message>,
    stats: Arc<LinkStats>,
    _marker: PhantomData<fn() -> Out>,
}

impl<Out> Clone for PiperReceiver<Out> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stats: Arc::clone(&self.stats),
            _marker: PhantomData,
        }
    }
}

impl<Out> PiperReceiver<Out>
where
    Out: Send + 'static,
{
    pub fn recv(&self) -> std::result::Result<Out, RecvOutputError> {
        let output = self.inner.recv().map_err(|_| RecvOutputError::Closed)?;
        self.stats.drains.fetch_add(1, Ordering::Relaxed);
        output
            .downcast::<Out>()
            .map(|value| *value)
            .map_err(|_| RecvOutputError::TypeMismatch)
    }

    pub fn recv_timeout(&self, duration: Duration) -> std::result::Result<Out, RecvOutputError> {
        let output = self
            .inner
            .recv_timeout(duration)
            .map_err(|error| match error {
                kanal::ReceiveErrorTimeout::Timeout => RecvOutputError::Timeout,
                kanal::ReceiveErrorTimeout::Closed | kanal::ReceiveErrorTimeout::SendClosed => {
                    RecvOutputError::Closed
                }
            })?;
        self.stats.drains.fetch_add(1, Ordering::Relaxed);
        output
            .downcast::<Out>()
            .map(|value| *value)
            .map_err(|_| RecvOutputError::TypeMismatch)
    }

    pub fn try_recv(&self) -> std::result::Result<Out, TryRecvOutputError> {
        let output = match self.inner.try_recv() {
            Ok(Some(output)) => output,
            Ok(None) => return Err(TryRecvOutputError::Empty),
            Err(kanal::ReceiveError::Closed) | Err(kanal::ReceiveError::SendClosed) => {
                return Err(TryRecvOutputError::Closed);
            }
        };
        self.stats.drains.fetch_add(1, Ordering::Relaxed);
        output
            .downcast::<Out>()
            .map(|value| *value)
            .map_err(|_| TryRecvOutputError::TypeMismatch)
    }
}

pub trait Recycle {
    fn recycle(&mut self);
}

impl<T> Recycle for Vec<T> {
    fn recycle(&mut self) {
        self.clear();
    }
}

#[derive(Clone)]
struct LeaseRuntime {
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: kanal::Sender<InternalFailure>,
}

pub struct BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    value: Option<T>,
    recycle_sender: kanal::Sender<T>,
    runtime: LeaseRuntime,
}

impl<T> BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn new(value: T, recycle_sender: kanal::Sender<T>, runtime: LeaseRuntime) -> Self {
        Self {
            value: Some(value),
            recycle_sender,
            runtime,
        }
    }

    pub fn into_inner(mut self) -> T {
        self.value
            .take()
            .expect("BufferLease value was already taken")
    }
}

impl<T> std::ops::Deref for BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
            .as_ref()
            .expect("BufferLease value was already taken")
    }
}

impl<T> std::ops::DerefMut for BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
            .as_mut()
            .expect("BufferLease value was already taken")
    }
}

impl<T> Drop for BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn drop(&mut self) {
        let Some(mut value) = self.value.take() else {
            return;
        };
        value.recycle();
        if self.recycle_sender.send(value).is_err()
            && !self.runtime.shutdown.load(Ordering::Acquire)
            && !self.runtime.abort.load(Ordering::Acquire)
        {
            self.runtime.abort.store(true, Ordering::Release);
            let _ = self.internal_failure("recycle channel closed while returning BufferLease");
        }
    }
}

impl<T> BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn internal_failure(&self, message: impl Into<String>) -> std::result::Result<(), ()> {
        self.runtime
            .internal_failure
            .send(InternalFailure::internal(message))
            .map_err(|_| ())
    }
}

pub struct StageContext<Out, E = String>
where
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    output: kanal::Sender<Message>,
    output_stats: Arc<LinkStats>,
    output_acquire: Option<Arc<AcquireFn<Out>>>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: kanal::Sender<InternalFailure>,
    _marker: PhantomData<fn(E)>,
}

impl<Out, E> StageContext<Out, E>
where
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn emit(&mut self, output: Out) {
        match self.output.send(Box::new(output)) {
            Ok(()) => {
                self.output_stats.arrivals.fetch_add(1, Ordering::Relaxed);
            }
            Err(_)
                if !self.shutdown.load(Ordering::Acquire)
                    && !self.abort.load(Ordering::Acquire) =>
            {
                self.abort.store(true, Ordering::Release);
                let _ = self.internal_failure.send(InternalFailure::internal(
                    "stage output channel closed unexpectedly",
                ));
            }
            Err(_) => {}
        }
    }

    pub fn acquire_output(&self) -> Out {
        let acquire = self.output_acquire.as_ref().unwrap_or_else(|| {
            panic!("ctx.acquire_output() was called for a stage without a reusable output factory")
        });
        acquire()
    }

    pub fn is_aborting(&self) -> bool {
        self.abort.load(Ordering::Acquire)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

pub trait Stage: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;
    type Error: Debug + Display + Send + 'static;
    type State: Send + 'static;

    fn init(&self) -> std::result::Result<Self::State, Self::Error>;

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error>;

    fn cleanup(&self, _state: Self::State) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

trait DynStage<E>: Send + Sync
where
    E: Debug + Display + Send + 'static,
{
    fn init_box(&self) -> std::result::Result<Box<dyn Any + Send>, StageFailure<E>>;

    fn process_box(
        &self,
        state: &mut dyn Any,
        input: Message,
        ctx: RuntimeStageContext,
    ) -> std::result::Result<(), StageFailure<E>>;

    fn cleanup_box(&self, state: Box<dyn Any + Send>) -> std::result::Result<(), StageFailure<E>>;
}

struct RuntimeStageContext {
    output: kanal::Sender<Message>,
    output_stats: Arc<LinkStats>,
    output_acquire: Option<DynAcquire>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: kanal::Sender<InternalFailure>,
}

struct StageAdapter<S>
where
    S: Stage,
{
    stage: S,
}

impl<S> DynStage<S::Error> for StageAdapter<S>
where
    S: Stage,
{
    fn init_box(&self) -> std::result::Result<Box<dyn Any + Send>, StageFailure<S::Error>> {
        self.stage
            .init()
            .map(|state| Box::new(state) as Box<dyn Any + Send>)
            .map_err(StageFailure::Init)
    }

    fn process_box(
        &self,
        state: &mut dyn Any,
        input: Message,
        ctx: RuntimeStageContext,
    ) -> std::result::Result<(), StageFailure<S::Error>> {
        let state = state
            .downcast_mut::<S::State>()
            .ok_or_else(|| StageFailure::Internal("stage state type mismatch".to_string()))?;
        let input = input
            .downcast::<S::Input>()
            .map(|input| *input)
            .map_err(|_| StageFailure::Internal("stage input type mismatch".to_string()))?;
        let output_acquire = match ctx.output_acquire {
            Some(acquire) => Some(
                Arc::downcast::<Arc<AcquireFn<S::Output>>>(acquire)
                    .map_err(|_| {
                        StageFailure::Internal("stage output factory type mismatch".to_string())
                    })?
                    .as_ref()
                    .clone(),
            ),
            None => None,
        };
        let mut ctx = StageContext {
            output: ctx.output,
            output_stats: ctx.output_stats,
            output_acquire,
            shutdown: ctx.shutdown,
            abort: ctx.abort,
            internal_failure: ctx.internal_failure,
            _marker: PhantomData,
        };
        self.stage
            .process(state, input, &mut ctx)
            .map_err(StageFailure::Process)
    }

    fn cleanup_box(
        &self,
        state: Box<dyn Any + Send>,
    ) -> std::result::Result<(), StageFailure<S::Error>> {
        let state = state
            .downcast::<S::State>()
            .map(|state| *state)
            .map_err(|_| StageFailure::Internal("stage cleanup state type mismatch".to_string()))?;
        self.stage.cleanup(state).map_err(StageFailure::Cleanup)
    }
}

struct InlineStage<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut StageContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    init: Init,
    process: Process,
    cleanup: Cleanup,
    _marker: PhantomData<fn(State, In, Out, E)>,
}

impl<Init, Process, Cleanup, State, In, Out, E> Stage
    for InlineStage<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut StageContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type Input = In;
    type Output = Out;
    type Error = E;
    type State = State;

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        (self.init)()
    }

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut StageContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        (self.process)(state, input, ctx)
    }

    fn cleanup(&self, state: Self::State) -> std::result::Result<(), Self::Error> {
        (self.cleanup)(state)
    }
}

pub struct StageSpec<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Arc<dyn DynStage<E>>,
    output_acquire_builder: Option<Arc<dyn OutputAcquireBuilder + Send + Sync>>,
    anchor: Option<AnchorHints>,
    _marker: PhantomData<fn(In) -> Out>,
}

impl<In, Out, E> StageSpec<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn with_reusable_output<T, Factory>(mut self, factory: Factory) -> Self
    where
        Out: BufferLeaseOutput<T>,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        self.output_acquire_builder = Some(Arc::new(RecycleAcquireBuilder {
            factory: Arc::new(factory),
            _marker: PhantomData::<fn() -> T>,
        }));
        self
    }

    pub fn max_threads(mut self, max_threads: usize) -> Self {
        self.anchor
            .get_or_insert_with(AnchorHints::default)
            .max_threads = Some(max_threads);
        self
    }

    pub fn initial_threads(mut self, initial_threads: usize) -> Self {
        self.anchor
            .get_or_insert_with(AnchorHints::default)
            .initial_threads = Some(initial_threads);
        self
    }

    pub fn fixed_threads(mut self, fixed_threads: usize) -> Self {
        let anchor = self.anchor.get_or_insert_with(AnchorHints::default);
        anchor.fixed_threads = Some(fixed_threads);
        anchor.initial_threads = Some(fixed_threads);
        anchor.max_threads = Some(fixed_threads);
        self
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AnchorHints {
    max_threads: Option<usize>,
    initial_threads: Option<usize>,
    fixed_threads: Option<usize>,
}

pub trait BufferLeaseOutput<T> {}

impl<T> BufferLeaseOutput<T> for BufferLease<T> where T: Recycle + Send + 'static {}

pub trait IntoStageSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn into_stage_spec(self) -> StageSpec<Self::Input, Self::Output, E>;
}

impl<In, Out, E> IntoStageSpec<E> for StageSpec<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type Input = In;
    type Output = Out;

    fn into_stage_spec(self) -> StageSpec<In, Out, E> {
        self
    }
}

impl<S> IntoStageSpec<S::Error> for S
where
    S: Stage,
{
    type Input = S::Input;
    type Output = S::Output;

    fn into_stage_spec(self) -> StageSpec<S::Input, S::Output, S::Error> {
        stage(default_stage_name::<S>(), self)
    }
}

pub trait StageExt: Stage + Sized {
    fn with_reusable_output<T, Factory>(
        self,
        factory: Factory,
    ) -> StageSpec<Self::Input, Self::Output, Self::Error>
    where
        Self::Output: BufferLeaseOutput<T>,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        stage(default_stage_name::<Self>(), self).with_reusable_output(factory)
    }
}

impl<S> StageExt for S where S: Stage {}

pub fn stage<S>(name: impl Into<String>, stage_impl: S) -> StageSpec<S::Input, S::Output, S::Error>
where
    S: Stage,
{
    StageSpec {
        name: name.into(),
        stage: Arc::new(StageAdapter { stage: stage_impl }),
        output_acquire_builder: None,
        anchor: None,
        _marker: PhantomData,
    }
}

pub fn anchor<S, E>(stage_like: S) -> StageSpec<S::Input, S::Output, E>
where
    S: IntoStageSpec<E>,
    E: Debug + Display + Send + 'static,
{
    let mut spec = stage_like.into_stage_spec();
    spec.anchor = Some(spec.anchor.unwrap_or_default());
    spec
}

fn default_stage_name<S>() -> String {
    std::any::type_name::<S>()
        .rsplit("::")
        .next()
        .unwrap_or("stage")
        .to_string()
}

pub struct InlineStageBuilder<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut StageContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    name: String,
    init: Init,
    process: Process,
    cleanup: Cleanup,
    _marker: PhantomData<fn(State, In, Out, E)>,
}

impl<Init, Process, Cleanup, State, In, Out, E>
    InlineStageBuilder<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut StageContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn with_cleanup<NextCleanup>(
        self,
        cleanup: NextCleanup,
    ) -> InlineStageBuilder<Init, Process, NextCleanup, State, In, Out, E>
    where
        NextCleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    {
        InlineStageBuilder {
            name: self.name,
            init: self.init,
            process: self.process,
            cleanup,
            _marker: PhantomData,
        }
    }

    pub fn with_reusable_output<T, Factory>(self, factory: Factory) -> StageSpec<In, Out, E>
    where
        Out: BufferLeaseOutput<T>,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        self.into_stage_spec().with_reusable_output(factory)
    }
}

impl<Init, Process, Cleanup, State, In, Out, E> IntoStageSpec<E>
    for InlineStageBuilder<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut StageContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type Input = In;
    type Output = Out;

    fn into_stage_spec(self) -> StageSpec<In, Out, E> {
        stage(
            self.name,
            InlineStage {
                init: self.init,
                process: self.process,
                cleanup: self.cleanup,
                _marker: PhantomData::<fn(State, In, Out, E)>,
            },
        )
    }
}

fn default_inline_cleanup<State, E>(_state: State) -> std::result::Result<(), E> {
    Ok(())
}

pub fn inline_stage<Init, Process, State, In, Out, E>(
    name: impl Into<String>,
    init: Init,
    process: Process,
) -> InlineStageBuilder<Init, Process, fn(State) -> std::result::Result<(), E>, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut StageContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    InlineStageBuilder {
        name: name.into(),
        init,
        process,
        cleanup: default_inline_cleanup::<State, E>,
        _marker: PhantomData,
    }
}

trait OutputAcquireBuilder {
    fn build(&self, runtime: LeaseRuntime) -> DynAcquire;
}

struct RecycleAcquireBuilder<T>
where
    T: Recycle + Send + 'static,
{
    factory: Arc<dyn Fn() -> T + Send + Sync>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> OutputAcquireBuilder for RecycleAcquireBuilder<T>
where
    T: Recycle + Send + 'static,
{
    fn build(&self, runtime: LeaseRuntime) -> DynAcquire {
        let (recycle_sender, recycle_receiver) = kanal::unbounded::<T>();
        let factory = Arc::clone(&self.factory);
        let acquire: Arc<AcquireFn<BufferLease<T>>> = Arc::new(move || {
            let value = match recycle_receiver.try_recv() {
                Ok(Some(value)) => value,
                Ok(None) | Err(_) => factory(),
            };
            BufferLease::new(value, recycle_sender.clone(), runtime.clone())
        });
        Arc::new(acquire)
    }
}

#[derive(Debug)]
enum StageFailure<E> {
    Init(E),
    Process(E),
    Cleanup(E),
    Internal(String),
}

#[derive(Clone, Debug)]
enum InternalFailure {
    Internal { message: String },
    Telemetry { message: String },
}

impl InternalFailure {
    fn internal(message: impl Into<String>) -> Self {
        InternalFailure::Internal {
            message: message.into(),
        }
    }

    fn telemetry(message: impl Into<String>) -> Self {
        InternalFailure::Telemetry {
            message: message.into(),
        }
    }
}

struct Link {
    sender: Option<kanal::Sender<Message>>,
    receiver: kanal::Receiver<Message>,
    stats: Arc<LinkStats>,
}

#[derive(Default)]
struct LinkStats {
    arrivals: AtomicU64,
    drains: AtomicU64,
}

pub struct GraphLink<T>
where
    T: Send + 'static,
{
    index: usize,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for GraphLink<T>
where
    T: Send + 'static,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for GraphLink<T> where T: Send + 'static {}

impl<T> GraphLink<T>
where
    T: Send + 'static,
{
    pub fn index(self) -> usize {
        self.index
    }
}

pub struct PipelineGraph<In, Out, E = String>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    input_link: usize,
    output_link: usize,
    link_count: usize,
    stages: Vec<GraphStageSpec<E>>,
    _marker: PhantomData<fn(In) -> Out>,
}

pub struct PipelineGraphBuilder<In, E = String>
where
    In: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    link_count: usize,
    stages: Vec<GraphStageSpec<E>>,
    _marker: PhantomData<fn(In)>,
}

impl<In, E> PipelineGraphBuilder<In, E>
where
    In: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            link_count: 1,
            stages: Vec::new(),
            _marker: PhantomData,
        }
    }

    pub fn input(&self) -> GraphLink<In> {
        GraphLink {
            index: 0,
            _marker: PhantomData,
        }
    }

    pub fn link<T>(&mut self) -> GraphLink<T>
    where
        T: Send + 'static,
    {
        let index = self.link_count;
        self.link_count += 1;
        GraphLink {
            index,
            _marker: PhantomData,
        }
    }

    pub fn add_stage<S>(
        &mut self,
        input: GraphLink<S::Input>,
        stage_like: S,
    ) -> GraphLink<S::Output>
    where
        S: IntoStageSpec<E>,
    {
        let output = self.link();
        self.add_stage_to(input, stage_like, output);
        output
    }

    pub fn add_stage_to<S>(
        &mut self,
        input: GraphLink<S::Input>,
        stage_like: S,
        output: GraphLink<S::Output>,
    ) where
        S: IntoStageSpec<E>,
    {
        let stage = stage_like.into_stage_spec();
        self.stages.push(GraphStageSpec {
            name: stage.name,
            stage: stage.stage,
            output_acquire_builder: stage.output_acquire_builder,
            anchor: stage.anchor,
            input_link: input.index,
            output_link: output.index,
        });
    }

    pub fn finish<Out>(self, output: GraphLink<Out>) -> PipelineGraph<In, Out, E>
    where
        Out: Send + 'static,
    {
        PipelineGraph {
            input_link: 0,
            output_link: output.index,
            link_count: self.link_count.max(output.index + 1),
            stages: self.stages,
            _marker: PhantomData,
        }
    }
}

impl<In, E> Default for PipelineGraphBuilder<In, E>
where
    In: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

struct GraphStageSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Arc<dyn DynStage<E>>,
    output_acquire_builder: Option<Arc<dyn OutputAcquireBuilder + Send + Sync>>,
    anchor: Option<AnchorHints>,
    input_link: usize,
    output_link: usize,
}

#[derive(Clone)]
struct RuntimeStage<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Arc<dyn DynStage<E>>,
    output_acquire: Option<DynAcquire>,
    anchor: Option<ResolvedAnchor>,
    input_link: usize,
    output_link: usize,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedAnchor {
    max_threads: usize,
    initial_threads: usize,
    fixed_threads: Option<usize>,
}

enum WorkerCommand<E>
where
    E: Debug + Display + Send + 'static,
{
    Run(WorkerAssignment<E>),
    Stop,
}

struct WorkerAssignment<E>
where
    E: Debug + Display + Send + 'static,
{
    stage_index: usize,
    stage: Arc<dyn DynStage<E>>,
    input: kanal::Receiver<Message>,
    input_stats: Arc<LinkStats>,
    output: kanal::Sender<Message>,
    output_stats: Arc<LinkStats>,
    output_acquire: Option<DynAcquire>,
    is_input_stage: bool,
    retire: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    poll_interval: Duration,
    internal_failure: kanal::Sender<InternalFailure>,
    stats: Arc<WorkerStats>,
}

enum WorkerEvent<E>
where
    E: Debug + Display + Send + 'static,
{
    Started {
        worker_id: usize,
    },
    Parked {
        worker_id: usize,
        stage_index: usize,
    },
    Failed {
        worker_id: usize,
        stage_index: usize,
        worker: String,
        failure: StageFailure<E>,
    },
    Stopped,
}

struct WorkerSlot<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    command: kanal::Sender<WorkerCommand<E>>,
    handle: Option<JoinHandle<()>>,
    active_stage: Option<usize>,
    retire: Option<Arc<AtomicBool>>,
    stats: Arc<WorkerStats>,
}

#[derive(Default)]
struct WorkerStats {
    process_nanos: AtomicU64,
    wait_nanos: AtomicU64,
    processed_items: AtomicU64,
}

impl WorkerStats {
    fn reset(&self) {
        self.process_nanos.store(0, Ordering::Relaxed);
        self.wait_nanos.store(0, Ordering::Relaxed);
        self.processed_items.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScaleDirection {
    Add,
    Remove,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScaleReason {
    Support,
    AnchorProbe,
    AnchorRevert,
    BudgetPressure,
    Idle,
}

struct PendingScale {
    worker_id: usize,
    stage_index: usize,
    direction: ScaleDirection,
    reason: ScaleReason,
}

struct StageControl {
    processed_count: u64,
    busy_ratio: f64,
    service_time_ewma: f64,
    per_worker_throughput: f64,
    desired_workers: usize,
    last_sample_processed: u64,
    scaling_state: StageScalingState,
    settling: bool,
    settle_samples: u32,
    settle_observed_work: bool,
    idle_samples: u32,
    last_operation: Option<(ScaleDirection, Instant)>,
    anchor: Option<AnchorControl>,
}

struct AnchorControl {
    max_threads: usize,
    fixed_threads: Option<usize>,
    warmup_complete: bool,
    probe: Option<AnchorProbe>,
    cooldown_samples: u32,
    last_probe_outcome: AnchorProbeOutcome,
    last_probe_reason: AnchorProbeReason,
}

struct AnchorProbe {
    worker_id: usize,
    samples: u32,
    observed_work: bool,
}

#[derive(Default)]
struct StageSample {
    process_nanos: u64,
    wait_nanos: u64,
    processed_items: u64,
}

#[derive(Clone, Debug)]
struct LinkControl {
    last_arrivals: u64,
    last_drains: u64,
    len: usize,
    previous_len: usize,
    smoothed_len: f64,
    arrival_rate: f64,
    drain_rate: f64,
    net_rate: f64,
    trend: QueueTrend,
}

impl Default for LinkControl {
    fn default() -> Self {
        Self {
            last_arrivals: 0,
            last_drains: 0,
            len: 0,
            previous_len: 0,
            smoothed_len: 0.0,
            arrival_rate: 0.0,
            drain_rate: 0.0,
            net_rate: 0.0,
            trend: QueueTrend::Starved,
        }
    }
}

fn resolve_anchor_hints<E>(hints: AnchorHints) -> Result<ResolvedAnchor, E>
where
    E: Debug + Display + Send + 'static,
{
    let available = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1);
    let max_threads = hints.max_threads.unwrap_or(available);
    if max_threads == 0 {
        return Err(PiperError::InvalidAnchorThreads);
    }
    let initial_threads = hints
        .initial_threads
        .unwrap_or_else(|| max_threads.div_ceil(2).max(1));
    if initial_threads == 0 {
        return Err(PiperError::InvalidAnchorThreads);
    }
    if hints.fixed_threads.is_some_and(|threads| threads == 0) {
        return Err(PiperError::InvalidAnchorThreads);
    }

    Ok(ResolvedAnchor {
        max_threads,
        initial_threads: initial_threads.min(max_threads),
        fixed_threads: hints.fixed_threads,
    })
}

fn build_stage_controls<E>(stages: &[RuntimeStage<E>]) -> Vec<StageControl>
where
    E: Debug + Display + Send + 'static,
{
    stages
        .iter()
        .map(|stage| StageControl {
            processed_count: 0,
            busy_ratio: 0.0,
            service_time_ewma: 0.0,
            per_worker_throughput: 0.0,
            desired_workers: 1,
            last_sample_processed: 0,
            scaling_state: StageScalingState::Eligible,
            settling: false,
            settle_samples: 0,
            settle_observed_work: false,
            idle_samples: 0,
            last_operation: None,
            anchor: stage.anchor.map(|anchor| AnchorControl {
                max_threads: anchor.max_threads,
                fixed_threads: anchor.fixed_threads,
                warmup_complete: false,
                probe: None,
                cooldown_samples: 0,
                last_probe_outcome: AnchorProbeOutcome::None,
                last_probe_reason: AnchorProbeReason::None,
            }),
        })
        .collect()
}

fn resolve_global_worker_cap(configured: Option<usize>, stage_count: usize) -> usize {
    let available = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1);
    configured
        .unwrap_or_else(|| available.saturating_mul(2).max(stage_count))
        .max(stage_count)
}

fn validate_graph_is_acyclic<E>(stages: &[GraphStageSpec<E>]) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    let mut adjacency = vec![Vec::<usize>::new(); stages.len()];
    for (producer_index, producer) in stages.iter().enumerate() {
        for (consumer_index, consumer) in stages.iter().enumerate() {
            if producer.output_link == consumer.input_link {
                adjacency[producer_index].push(consumer_index);
            }
        }
    }

    fn visit(
        node: usize,
        adjacency: &[Vec<usize>],
        temporary: &mut [bool],
        permanent: &mut [bool],
    ) -> bool {
        if permanent[node] {
            return false;
        }
        if temporary[node] {
            return true;
        }
        temporary[node] = true;
        for child in &adjacency[node] {
            if visit(*child, adjacency, temporary, permanent) {
                return true;
            }
        }
        temporary[node] = false;
        permanent[node] = true;
        false
    }

    let mut temporary = vec![false; stages.len()];
    let mut permanent = vec![false; stages.len()];
    for index in 0..stages.len() {
        if visit(index, &adjacency, &mut temporary, &mut permanent) {
            return Err(PiperError::InvalidGraph {
                message: "graph cycles are not supported".to_string(),
            });
        }
    }
    Ok(())
}

pub struct Piper<In, Out, E = String>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    sender: PiperSender<In>,
    receiver: PiperReceiver<Out>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    supervisor: Option<JoinHandle<Result<(), E>>>,
}

impl<In, Out, E> Piper<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn start(config: PiperConfig, graph: PipelineGraph<In, Out, E>) -> Result<Self, E> {
        if graph.stages.is_empty() {
            return Err(PiperError::NoStages);
        }

        let PipelineGraph {
            input_link,
            output_link,
            link_count,
            stages,
            ..
        } = graph;

        if input_link >= link_count || output_link >= link_count {
            return Err(PiperError::InvalidGraph {
                message: "input or output link is outside the graph".to_string(),
            });
        }

        for stage in &stages {
            if stage.input_link >= link_count || stage.output_link >= link_count {
                return Err(PiperError::InvalidGraph {
                    message: format!("stage `{}` references a missing link", stage.name),
                });
            }
        }
        validate_graph_is_acyclic(&stages)?;

        let anchor_count = stages.iter().filter(|stage| stage.anchor.is_some()).count();
        if anchor_count == 0 && config.global_worker_cap == Some(0) {
            return Err(PiperError::InvalidGraph {
                message: "unanchored graphs require a finite non-zero worker cap".to_string(),
            });
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let abort = Arc::new(AtomicBool::new(false));
        let (internal_failure_sender, internal_failure_receiver) = kanal::unbounded();
        let lease_runtime = LeaseRuntime {
            shutdown: Arc::clone(&shutdown),
            abort: Arc::clone(&abort),
            internal_failure: internal_failure_sender.clone(),
        };

        let mut links = Vec::with_capacity(link_count);
        for _ in 0..link_count {
            let (sender, receiver) = kanal::unbounded::<Message>();
            links.push(Link {
                sender: Some(sender),
                receiver,
                stats: Arc::new(LinkStats::default()),
            });
        }

        let input_sender = links[input_link]
            .sender
            .as_ref()
            .expect("input sender exists")
            .clone();
        let output_receiver = links[output_link].receiver.clone();
        let input_stats = Arc::clone(&links[input_link].stats);
        let output_stats = Arc::clone(&links[output_link].stats);

        let runtime_stages: Vec<_> = stages
            .into_iter()
            .map(|stage| {
                let anchor = stage.anchor.map(resolve_anchor_hints).transpose()?;
                Ok(RuntimeStage {
                    name: stage.name,
                    stage: stage.stage,
                    output_acquire: stage
                        .output_acquire_builder
                        .map(|builder| builder.build(lease_runtime.clone())),
                    anchor,
                    input_link: stage.input_link,
                    output_link: stage.output_link,
                })
            })
            .collect::<Result<Vec<_>, E>>()?;

        let global_worker_cap =
            resolve_global_worker_cap(config.global_worker_cap, runtime_stages.len());

        let snapshot = Arc::new(RwLock::new(PiperSnapshot {
            links: (0..link_count)
                .map(|index| LinkSnapshot {
                    index,
                    len: 0,
                    trend: QueueTrend::Starved,
                    arrival_rate: 0.0,
                    drain_rate: 0.0,
                    net_rate: 0.0,
                    smoothed_len: 0.0,
                })
                .collect(),
            stages: runtime_stages
                .iter()
                .enumerate()
                .map(|(index, stage)| StageSnapshot {
                    index,
                    name: stage.name.clone(),
                    input_link: stage.input_link,
                    output_link: stage.output_link,
                    active_threads: 0,
                    processed_count: 0,
                    busy_ratio: 0.0,
                    service_time: Duration::ZERO,
                    per_worker_throughput: 0.0,
                    desired_workers: 1,
                    scaling_state: StageScalingState::Eligible,
                    is_anchor: stage.anchor.is_some(),
                    is_fixed_anchor: stage
                        .anchor
                        .is_some_and(|anchor| anchor.fixed_threads.is_some()),
                })
                .collect(),
            anchors: runtime_stages
                .iter()
                .enumerate()
                .filter_map(|(index, stage)| {
                    stage.anchor.map(|anchor| AnchorSnapshot {
                        stage_index: index,
                        stage_name: stage.name.clone(),
                        active_threads: 0,
                        max_threads: anchor.max_threads,
                        fixed_threads: anchor.fixed_threads,
                        probe_state: AnchorProbeState::WarmingUp,
                        last_probe_outcome: AnchorProbeOutcome::None,
                        last_probe_reason: AnchorProbeReason::None,
                    })
                })
                .collect(),
            parked_threads: 0,
            total_active_workers: 0,
            global_worker_cap,
            budget_pressure: false,
            output_backpressure: false,
            shutdown_requested: false,
            abort_requested: false,
            pending_scale_operation: false,
        }));

        let csv_recorder = start_csv_recorder(
            config.csv_telemetry.clone(),
            Arc::clone(&snapshot),
            internal_failure_sender.clone(),
            Arc::clone(&abort),
        )?;

        let supervisor_snapshot = Arc::clone(&snapshot);
        let supervisor_shutdown = Arc::clone(&shutdown);
        let supervisor_abort = Arc::clone(&abort);
        let supervisor = thread::Builder::new()
            .name("piper-supervisor".to_string())
            .spawn(move || {
                run_supervisor(
                    config,
                    runtime_stages,
                    links,
                    input_link,
                    output_link,
                    supervisor_shutdown,
                    supervisor_abort,
                    supervisor_snapshot,
                    internal_failure_sender,
                    internal_failure_receiver,
                    csv_recorder,
                )
            })
            .map_err(|source| PiperError::SpawnFailed {
                worker: "piper-supervisor".to_string(),
                source,
            })?;

        Ok(Piper {
            sender: PiperSender {
                inner: input_sender,
                stats: input_stats,
                shutdown: Arc::clone(&shutdown),
                _marker: PhantomData,
            },
            receiver: PiperReceiver {
                inner: output_receiver,
                stats: output_stats,
                _marker: PhantomData,
            },
            shutdown,
            abort,
            snapshot,
            supervisor: Some(supervisor),
        })
    }

    pub fn sender(&self) -> PiperSender<In> {
        self.sender.clone()
    }

    pub fn receiver(&self) -> PiperReceiver<Out> {
        self.receiver.clone()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn abort(&self) {
        self.abort.store(true, Ordering::Release);
    }

    pub fn get_telemetry(&self) -> PiperSnapshot {
        self.snapshot.read().clone()
    }

    pub fn join(mut self) -> Result<(), E> {
        self.shutdown();
        if let Some(supervisor) = self.supervisor.take() {
            supervisor
                .join()
                .map_err(|payload| PiperError::WorkerPanicked {
                    worker: "piper-supervisor".to_string(),
                    message: panic_payload_to_string(payload),
                })?
        } else {
            Ok(())
        }
    }
}

struct CsvRecorder {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl CsvRecorder {
    fn stop<E>(self) -> Result<(), E>
    where
        E: Debug + Display + Send + 'static,
    {
        self.stop.store(true, Ordering::Release);
        self.handle
            .join()
            .map_err(|payload| PiperError::WorkerPanicked {
                worker: "piper-csv-telemetry".to_string(),
                message: panic_payload_to_string(payload),
            })
    }
}

fn start_csv_recorder<E>(
    config: Option<CsvTelemetryConfig>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    failure_sender: kanal::Sender<InternalFailure>,
    abort: Arc<AtomicBool>,
) -> Result<Option<CsvRecorder>, E>
where
    E: Debug + Display + Send + 'static,
{
    let Some(config) = config else {
        return Ok(None);
    };

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&config.path)
        .map_err(|source| PiperError::Telemetry {
            message: format!(
                "failed to create CSV telemetry file `{}`: {source}",
                config.path.display()
            ),
        })?;
    let mut writer = BufWriter::new(file);
    let initial_snapshot = snapshot.read().clone();
    writeln!(writer, "{}", csv_header(&initial_snapshot)).map_err(|source| {
        PiperError::Telemetry {
            message: format!(
                "failed to write CSV telemetry header `{}`: {source}",
                config.path.display()
            ),
        }
    })?;
    writer.flush().map_err(|source| PiperError::Telemetry {
        message: format!(
            "failed to flush CSV telemetry header `{}`: {source}",
            config.path.display()
        ),
    })?;

    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let path = config.path.clone();
    let handle = thread::Builder::new()
        .name("piper-csv-telemetry".to_string())
        .spawn(move || {
            let started = Instant::now();
            loop {
                thread::sleep(config.interval);
                let stopping = thread_stop.load(Ordering::Acquire) || abort.load(Ordering::Acquire);
                let snapshot = snapshot.read().clone();
                if let Err(source) = writeln!(writer, "{}", csv_row(started.elapsed(), &snapshot))
                    .and_then(|_| writer.flush())
                {
                    abort.store(true, Ordering::Release);
                    let _ = failure_sender.send(InternalFailure::telemetry(format!(
                        "failed to write CSV telemetry file `{}`: {source}",
                        path.display()
                    )));
                    break;
                }
                if stopping {
                    break;
                }
            }
        })
        .map_err(|source| PiperError::SpawnFailed {
            worker: "piper-csv-telemetry".to_string(),
            source,
        })?;

    Ok(Some(CsvRecorder { stop, handle }))
}

fn csv_header(snapshot: &PiperSnapshot) -> String {
    let mut fields = vec![
        "elapsed_ms".to_string(),
        "shutdown_requested".to_string(),
        "abort_requested".to_string(),
        "pending_scale_operation".to_string(),
        "parked_threads".to_string(),
        "total_active_workers".to_string(),
        "global_worker_cap".to_string(),
        "budget_pressure".to_string(),
        "output_backpressure".to_string(),
        "anchor_count".to_string(),
    ];

    for index in 0..snapshot.anchors.len() {
        fields.push(format!("anchor{index}_index"));
        fields.push(format!("anchor{index}_name"));
        fields.push(format!("anchor{index}_active_threads"));
        fields.push(format!("anchor{index}_max_threads"));
        fields.push(format!("anchor{index}_fixed_threads"));
        fields.push(format!("anchor{index}_probe_state"));
        fields.push(format!("anchor{index}_last_probe_outcome"));
        fields.push(format!("anchor{index}_last_probe_reason"));
    }

    for link in &snapshot.links {
        fields.push(format!("link{}_len", link.index));
        fields.push(format!("link{}_trend", link.index));
        fields.push(format!("link{}_arrival_rate", link.index));
        fields.push(format!("link{}_drain_rate", link.index));
        fields.push(format!("link{}_net_rate", link.index));
        fields.push(format!("link{}_smoothed_len", link.index));
    }

    for stage in &snapshot.stages {
        fields.push(format!("stage{}_name", stage.index));
        fields.push(format!("stage{}_input_link", stage.index));
        fields.push(format!("stage{}_output_link", stage.index));
        fields.push(format!("stage{}_active_threads", stage.index));
        fields.push(format!("stage{}_processed_count", stage.index));
        fields.push(format!("stage{}_busy_ratio", stage.index));
        fields.push(format!("stage{}_service_time_ms", stage.index));
        fields.push(format!("stage{}_per_worker_throughput", stage.index));
        fields.push(format!("stage{}_desired_workers", stage.index));
        fields.push(format!("stage{}_scaling_state", stage.index));
        fields.push(format!("stage{}_is_anchor", stage.index));
        fields.push(format!("stage{}_is_fixed_anchor", stage.index));
    }

    fields.join(",")
}

fn csv_row(elapsed: Duration, snapshot: &PiperSnapshot) -> String {
    let mut fields = vec![
        format!("{:.3}", elapsed.as_secs_f64() * 1000.0),
        snapshot.shutdown_requested.to_string(),
        snapshot.abort_requested.to_string(),
        snapshot.pending_scale_operation.to_string(),
        snapshot.parked_threads.to_string(),
        snapshot.total_active_workers.to_string(),
        snapshot.global_worker_cap.to_string(),
        snapshot.budget_pressure.to_string(),
        snapshot.output_backpressure.to_string(),
        snapshot.anchors.len().to_string(),
    ];

    for anchor in &snapshot.anchors {
        fields.push(anchor.stage_index.to_string());
        fields.push(csv_escape(&anchor.stage_name));
        fields.push(anchor.active_threads.to_string());
        fields.push(anchor.max_threads.to_string());
        fields.push(
            anchor
                .fixed_threads
                .map(|threads| threads.to_string())
                .unwrap_or_default(),
        );
        fields.push(format!("{:?}", anchor.probe_state));
        fields.push(format!("{:?}", anchor.last_probe_outcome));
        fields.push(format!("{:?}", anchor.last_probe_reason));
    }

    for link in &snapshot.links {
        fields.push(link.len.to_string());
        fields.push(link.trend.code().to_string());
        fields.push(format!("{:.6}", link.arrival_rate));
        fields.push(format!("{:.6}", link.drain_rate));
        fields.push(format!("{:.6}", link.net_rate));
        fields.push(format!("{:.6}", link.smoothed_len));
    }

    for stage in &snapshot.stages {
        fields.push(csv_escape(&stage.name));
        fields.push(stage.input_link.to_string());
        fields.push(stage.output_link.to_string());
        fields.push(stage.active_threads.to_string());
        fields.push(stage.processed_count.to_string());
        fields.push(format!("{:.6}", stage.busy_ratio));
        fields.push(format!("{:.3}", stage.service_time.as_secs_f64() * 1000.0));
        fields.push(format!("{:.6}", stage.per_worker_throughput));
        fields.push(stage.desired_workers.to_string());
        fields.push(format!("{:?}", stage.scaling_state));
        fields.push(stage.is_anchor.to_string());
        fields.push(stage.is_fixed_anchor.to_string());
    }

    fields.join(",")
}

fn csv_escape(value: &str) -> String {
    if value
        .chars()
        .any(|ch| matches!(ch, ',' | '"' | '\n' | '\r'))
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn run_supervisor<E>(
    config: PiperConfig,
    stages: Vec<RuntimeStage<E>>,
    mut links: Vec<Link>,
    input_link: usize,
    output_link: usize,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    internal_failure_sender: kanal::Sender<InternalFailure>,
    internal_failure_receiver: kanal::Receiver<InternalFailure>,
    mut csv_recorder: Option<CsvRecorder>,
) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    let (worker_event_sender, worker_event_receiver) = kanal::unbounded::<WorkerEvent<E>>();
    let mut workers = Vec::new();
    let mut active_by_stage = vec![Vec::<usize>::new(); stages.len()];
    let mut parked = Vec::new();
    let mut link_controls = vec![LinkControl::default(); links.len()];
    let mut controls = build_stage_controls(&stages);
    let global_worker_cap = resolve_global_worker_cap(config.global_worker_cap, stages.len());
    let mut pending_scale = None;
    let mut stored_failure = None;
    let mut supervisor_holds_links = true;
    let mut last_sample_at = Instant::now();

    for stage_index in 0..stages.len() {
        let active_count = if let Some(anchor) = stages[stage_index].anchor {
            if let Some(fixed_threads) = anchor.fixed_threads {
                fixed_threads
            } else {
                let support_minimum = stages.len().saturating_sub(1);
                let anchor_budget = global_worker_cap.saturating_sub(support_minimum).max(1);
                anchor
                    .initial_threads
                    .min(anchor.max_threads)
                    .min(anchor_budget)
            }
        } else {
            1
        };
        for _ in 0..active_count {
            let name = format!("piper-worker-{}", workers.len());
            let worker_id = spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
            assign_worker(
                worker_id,
                stage_index,
                &stages,
                &links,
                &config,
                input_link,
                &shutdown,
                &abort,
                &internal_failure_sender,
                &mut workers,
                &mut active_by_stage,
            )?;
        }
    }

    let parked_target = parked_worker_target(&stages);
    for _ in 0..parked_target {
        let name = format!("piper-worker-{}", workers.len());
        let worker_id = spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
        parked.push(worker_id);
    }

    update_snapshot(
        &snapshot,
        &links,
        &stages,
        &active_by_stage,
        parked.len(),
        &link_controls,
        &controls,
        output_link,
        global_worker_cap,
        shutdown.load(Ordering::Acquire),
        abort.load(Ordering::Acquire),
        pending_scale.is_some(),
    );

    loop {
        drain_worker_events(
            &worker_event_receiver,
            &mut workers,
            &mut active_by_stage,
            &mut parked,
            &mut controls,
            &mut pending_scale,
            &abort,
            &mut stored_failure,
        );

        while let Ok(Some(failure)) = internal_failure_receiver.try_recv() {
            stored_failure = Some(match failure {
                InternalFailure::Internal { message } => PiperError::Internal {
                    worker: "piper-supervisor".to_string(),
                    message,
                },
                InternalFailure::Telemetry { message } => PiperError::Telemetry { message },
            });
            abort.store(true, Ordering::Release);
        }

        if shutdown.load(Ordering::Acquire)
            || abort.load(Ordering::Acquire)
            || stored_failure.is_some()
        {
            if supervisor_holds_links {
                for link in &mut links {
                    link.sender.take();
                }
                supervisor_holds_links = false;
            }
        }

        let now = Instant::now();
        let sample_elapsed = now
            .duration_since(last_sample_at)
            .max(Duration::from_micros(1));
        last_sample_at = now;
        sample_links(&links, sample_elapsed, &mut link_controls);
        collect_stage_samples(
            &workers,
            &active_by_stage,
            &link_controls,
            &stages,
            &mut controls,
        );
        update_desired_workers(&link_controls, &active_by_stage, &stages, &mut controls);

        if !shutdown.load(Ordering::Acquire)
            && !abort.load(Ordering::Acquire)
            && stored_failure.is_none()
            && pending_scale.is_none()
        {
            if let Some(operation) = choose_scale_operation(
                &link_controls,
                &active_by_stage,
                &mut controls,
                &stages,
                output_link,
                global_worker_cap,
            ) {
                match operation {
                    ScaleOperation::Add {
                        stage_index,
                        reason,
                    } => {
                        let worker_id = match parked.pop() {
                            Some(worker_id) => worker_id,
                            None => {
                                let name = format!("piper-worker-{}", workers.len());
                                spawn_worker(&mut workers, worker_event_sender.clone(), &name)?
                            }
                        };
                        assign_worker(
                            worker_id,
                            stage_index,
                            &stages,
                            &links,
                            &config,
                            input_link,
                            &shutdown,
                            &abort,
                            &internal_failure_sender,
                            &mut workers,
                            &mut active_by_stage,
                        )?;
                        pending_scale = Some(PendingScale {
                            worker_id,
                            stage_index,
                            direction: ScaleDirection::Add,
                            reason,
                        });

                        while parked.len() < parked_target {
                            let name = format!("piper-worker-{}", workers.len());
                            let worker_id =
                                spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
                            parked.push(worker_id);
                        }
                    }
                    ScaleOperation::Remove {
                        stage_index,
                        worker_id,
                        reason,
                    } => {
                        if let Some(worker_id) =
                            worker_id.or_else(|| active_by_stage[stage_index].first().copied())
                        {
                            if let Some(retire) = workers[worker_id].retire.as_ref() {
                                retire.store(true, Ordering::Release);
                                pending_scale = Some(PendingScale {
                                    worker_id,
                                    stage_index,
                                    direction: ScaleDirection::Remove,
                                    reason,
                                });
                            }
                        }
                    }
                }
            }
        }

        update_snapshot(
            &snapshot,
            &links,
            &stages,
            &active_by_stage,
            parked.len(),
            &link_controls,
            &controls,
            output_link,
            global_worker_cap,
            shutdown.load(Ordering::Acquire),
            abort.load(Ordering::Acquire),
            pending_scale.is_some(),
        );

        let active_count: usize = active_by_stage.iter().map(Vec::len).sum();
        if (shutdown.load(Ordering::Acquire)
            || abort.load(Ordering::Acquire)
            || stored_failure.is_some())
            && active_count == 0
        {
            break;
        }

        thread::sleep(config.sample_interval);
    }

    for worker_id in parked.drain(..) {
        let _ = workers[worker_id].command.send(WorkerCommand::Stop);
    }

    for worker in &mut workers {
        if let Some(handle) = worker.handle.take() {
            handle
                .join()
                .map_err(|payload| PiperError::WorkerPanicked {
                    worker: worker.name.clone(),
                    message: panic_payload_to_string(payload),
                })?;
        }
    }

    update_snapshot(
        &snapshot,
        &links,
        &stages,
        &active_by_stage,
        0,
        &link_controls,
        &controls,
        output_link,
        global_worker_cap,
        shutdown.load(Ordering::Acquire),
        abort.load(Ordering::Acquire),
        false,
    );

    if let Some(recorder) = csv_recorder.take() {
        recorder.stop()?;
    }

    if let Some(failure) = stored_failure {
        Err(failure)
    } else {
        Ok(())
    }
}

fn spawn_worker<E>(
    workers: &mut Vec<WorkerSlot<E>>,
    event_sender: kanal::Sender<WorkerEvent<E>>,
    name: &str,
) -> Result<usize, E>
where
    E: Debug + Display + Send + 'static,
{
    let worker_id = workers.len();
    let (command_sender, command_receiver) = kanal::unbounded::<WorkerCommand<E>>();
    let thread_name = name.to_string();
    let worker_thread_name = thread_name.clone();
    let handle = thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            worker_loop(
                worker_id,
                worker_thread_name,
                command_receiver,
                event_sender,
            )
        })
        .map_err(|source| PiperError::SpawnFailed {
            worker: name.to_string(),
            source,
        })?;
    workers.push(WorkerSlot {
        name: name.to_string(),
        command: command_sender,
        handle: Some(handle),
        active_stage: None,
        retire: None,
        stats: Arc::new(WorkerStats::default()),
    });
    Ok(worker_id)
}

fn parked_worker_target<E>(stages: &[RuntimeStage<E>]) -> usize
where
    E: Debug + Display + Send + 'static,
{
    stages
        .iter()
        .filter(|stage| {
            !stage
                .anchor
                .is_some_and(|anchor| anchor.fixed_threads.is_some())
        })
        .count()
}

#[allow(clippy::too_many_arguments)]
fn assign_worker<E>(
    worker_id: usize,
    stage_index: usize,
    stages: &[RuntimeStage<E>],
    links: &[Link],
    config: &PiperConfig,
    input_link: usize,
    shutdown: &Arc<AtomicBool>,
    abort: &Arc<AtomicBool>,
    internal_failure: &kanal::Sender<InternalFailure>,
    workers: &mut [WorkerSlot<E>],
    active_by_stage: &mut [Vec<usize>],
) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    let retire = Arc::new(AtomicBool::new(false));
    workers[worker_id].stats.reset();
    let stage = &stages[stage_index];
    let output = links[stage.output_link]
        .sender
        .as_ref()
        .ok_or_else(|| PiperError::Internal {
            worker: workers[worker_id].name.clone(),
            message: "cannot assign worker after output sender was dropped".to_string(),
        })?
        .clone();
    let assignment = WorkerAssignment {
        stage_index,
        stage: Arc::clone(&stage.stage),
        input: links[stage.input_link].receiver.clone(),
        input_stats: Arc::clone(&links[stage.input_link].stats),
        output,
        output_stats: Arc::clone(&links[stage.output_link].stats),
        output_acquire: stage.output_acquire.clone(),
        is_input_stage: stage.input_link == input_link,
        retire: Arc::clone(&retire),
        shutdown: Arc::clone(shutdown),
        abort: Arc::clone(abort),
        poll_interval: config.poll_interval,
        internal_failure: internal_failure.clone(),
        stats: Arc::clone(&workers[worker_id].stats),
    };
    workers[worker_id].active_stage = Some(stage_index);
    workers[worker_id].retire = Some(retire);
    active_by_stage[stage_index].push(worker_id);
    workers[worker_id]
        .command
        .send(WorkerCommand::Run(assignment))
        .map_err(|_| PiperError::Internal {
            worker: workers[worker_id].name.clone(),
            message: "worker command channel closed".to_string(),
        })
}

fn worker_loop<E>(
    worker_id: usize,
    worker_name: String,
    command_receiver: kanal::Receiver<WorkerCommand<E>>,
    event_sender: kanal::Sender<WorkerEvent<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    while let Ok(command) = command_receiver.recv() {
        match command {
            WorkerCommand::Run(assignment) => {
                run_assignment(worker_id, &worker_name, assignment, &event_sender);
            }
            WorkerCommand::Stop => break,
        }
    }
    let _ = event_sender.send(WorkerEvent::Stopped);
}

fn run_assignment<E>(
    worker_id: usize,
    worker_name: &str,
    assignment: WorkerAssignment<E>,
    event_sender: &kanal::Sender<WorkerEvent<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    let stage_index = assignment.stage_index;
    let mut state = match assignment.stage.init_box() {
        Ok(state) => state,
        Err(failure) => {
            let _ = event_sender.send(WorkerEvent::Failed {
                worker_id,
                stage_index,
                worker: worker_name.to_string(),
                failure,
            });
            return;
        }
    };

    let _ = event_sender.send(WorkerEvent::Started { worker_id });

    let mut graceful = true;

    loop {
        if assignment.abort.load(Ordering::Acquire) {
            graceful = false;
            break;
        }
        if assignment.retire.load(Ordering::Acquire) {
            break;
        }
        if assignment.shutdown.load(Ordering::Acquire)
            && assignment.is_input_stage
            && assignment.input.is_empty()
        {
            break;
        }

        let wait_started = Instant::now();
        match assignment.input.recv_timeout(assignment.poll_interval) {
            Ok(input) => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                assignment
                    .input_stats
                    .drains
                    .fetch_add(1, Ordering::Relaxed);
                let ctx = RuntimeStageContext {
                    output: assignment.output.clone(),
                    output_stats: Arc::clone(&assignment.output_stats),
                    output_acquire: assignment.output_acquire.clone(),
                    shutdown: Arc::clone(&assignment.shutdown),
                    abort: Arc::clone(&assignment.abort),
                    internal_failure: assignment.internal_failure.clone(),
                };
                let process_started = Instant::now();
                let result = assignment.stage.process_box(state.as_mut(), input, ctx);
                assignment.stats.process_nanos.fetch_add(
                    duration_nanos_u64(process_started.elapsed()),
                    Ordering::Relaxed,
                );
                if let Err(failure) = result {
                    let _ = event_sender.send(WorkerEvent::Failed {
                        worker_id,
                        stage_index,
                        worker: worker_name.to_string(),
                        failure,
                    });
                    return;
                }
                assignment
                    .stats
                    .processed_items
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(kanal::ReceiveErrorTimeout::Timeout) => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                if assignment.input.is_terminated() {
                    break;
                }
            }
            Err(kanal::ReceiveErrorTimeout::Closed)
            | Err(kanal::ReceiveErrorTimeout::SendClosed) => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                break;
            }
        }
    }

    if graceful {
        if let Err(failure) = assignment.stage.cleanup_box(state) {
            let _ = event_sender.send(WorkerEvent::Failed {
                worker_id,
                stage_index,
                worker: worker_name.to_string(),
                failure,
            });
            return;
        }
    }

    let _ = event_sender.send(WorkerEvent::Parked {
        worker_id,
        stage_index,
    });
}

#[allow(clippy::too_many_arguments)]
fn drain_worker_events<E>(
    receiver: &kanal::Receiver<WorkerEvent<E>>,
    workers: &mut [WorkerSlot<E>],
    active_by_stage: &mut [Vec<usize>],
    parked: &mut Vec<usize>,
    controls: &mut [StageControl],
    pending_scale: &mut Option<PendingScale>,
    abort: &Arc<AtomicBool>,
    stored_failure: &mut Option<PiperError<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    while let Ok(Some(event)) = receiver.try_recv() {
        match event {
            WorkerEvent::Started { worker_id, .. } => {
                if pending_scale.as_ref().is_some_and(|pending| {
                    pending.worker_id == worker_id && pending.direction == ScaleDirection::Add
                }) {
                    let pending = pending_scale.take().expect("pending scale exists");
                    record_stage_operation(controls, pending.stage_index, ScaleDirection::Add);
                    match pending.reason {
                        ScaleReason::Support => {
                            controls[pending.stage_index].settling = true;
                            controls[pending.stage_index].settle_samples = 0;
                            controls[pending.stage_index].settle_observed_work = false;
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Settling;
                        }
                        ScaleReason::AnchorProbe => {
                            if let Some(anchor) = controls[pending.stage_index].anchor.as_mut() {
                                anchor.probe = Some(AnchorProbe {
                                    worker_id,
                                    samples: 0,
                                    observed_work: false,
                                });
                            }
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Probing;
                        }
                        ScaleReason::AnchorRevert
                        | ScaleReason::BudgetPressure
                        | ScaleReason::Idle => {
                            controls[pending.stage_index].settling = true;
                            controls[pending.stage_index].settle_samples = 0;
                            controls[pending.stage_index].settle_observed_work = false;
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Settling;
                        }
                    }
                }
            }
            WorkerEvent::Parked {
                worker_id,
                stage_index,
            } => {
                remove_worker_from_stage(active_by_stage, stage_index, worker_id);
                workers[worker_id].active_stage = None;
                workers[worker_id].retire = None;
                parked.push(worker_id);
                if pending_scale.as_ref().is_some_and(|pending| {
                    pending.worker_id == worker_id && pending.direction == ScaleDirection::Remove
                }) {
                    let pending = pending_scale.take().expect("pending scale exists");
                    record_stage_operation(controls, pending.stage_index, ScaleDirection::Remove);
                    if let Some(anchor) = controls[pending.stage_index].anchor.as_mut() {
                        if matches!(
                            pending.reason,
                            ScaleReason::AnchorRevert
                                | ScaleReason::BudgetPressure
                                | ScaleReason::Idle
                        ) {
                            anchor.probe = None;
                            anchor.last_probe_outcome = AnchorProbeOutcome::Reverted;
                            if pending.reason == ScaleReason::BudgetPressure {
                                anchor.last_probe_reason = AnchorProbeReason::BudgetPressure;
                            } else if pending.reason == ScaleReason::Idle {
                                anchor.last_probe_reason = AnchorProbeReason::Idle;
                            }
                            anchor.cooldown_samples = grow_sample_count(anchor.cooldown_samples);
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::BackingOff;
                        } else {
                            controls[pending.stage_index].settling = true;
                            controls[pending.stage_index].settle_samples = 0;
                            controls[pending.stage_index].settle_observed_work = false;
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Settling;
                        }
                    } else {
                        controls[pending.stage_index].settling = true;
                        controls[pending.stage_index].settle_samples = 0;
                        controls[pending.stage_index].settle_observed_work = false;
                        controls[pending.stage_index].scaling_state = StageScalingState::Settling;
                    }
                }
            }
            WorkerEvent::Failed {
                worker_id,
                stage_index,
                worker,
                failure,
            } => {
                remove_worker_from_stage(active_by_stage, stage_index, worker_id);
                workers[worker_id].active_stage = None;
                workers[worker_id].retire = None;
                if !parked.contains(&worker_id) {
                    parked.push(worker_id);
                }
                if pending_scale
                    .as_ref()
                    .is_some_and(|pending| pending.worker_id == worker_id)
                {
                    *pending_scale = None;
                }
                abort.store(true, Ordering::Release);
                *stored_failure = Some(match failure {
                    StageFailure::Init(error) => PiperError::UserInit { worker, error },
                    StageFailure::Process(error) => PiperError::UserProcess { worker, error },
                    StageFailure::Cleanup(error) => PiperError::UserCleanup { worker, error },
                    StageFailure::Internal(message) => PiperError::Internal { worker, message },
                });
            }
            WorkerEvent::Stopped => {}
        }
    }
}

fn remove_worker_from_stage(
    active_by_stage: &mut [Vec<usize>],
    stage_index: usize,
    worker_id: usize,
) {
    if let Some(position) = active_by_stage[stage_index]
        .iter()
        .position(|id| *id == worker_id)
    {
        active_by_stage[stage_index].swap_remove(position);
    }
}

fn record_stage_operation(
    controls: &mut [StageControl],
    stage_index: usize,
    direction: ScaleDirection,
) {
    controls[stage_index].last_operation = Some((direction, Instant::now()));
}

const RATE_EWMA_ALPHA: f64 = 0.35;
const SERVICE_EWMA_ALPHA: f64 = 0.30;
const STABLE_RATE_RATIO: f64 = 0.12;
const FAST_RATE_RATIO: f64 = 0.45;
const RUNAWAY_RATE_RATIO: f64 = 0.85;
const SETTLE_SAMPLES: u32 = 2;
const PROBE_SETTLE_SAMPLES: u32 = 2;
const IDLE_SHRINK_SAMPLES: u32 = 50;
const DEFAULT_BACKLOG_DRAIN_SECS: f64 = 1.0;

fn sample_links(links: &[Link], elapsed: Duration, controls: &mut [LinkControl]) {
    let seconds = elapsed.as_secs_f64().max(0.000_001);
    for (index, link) in links.iter().enumerate() {
        let arrivals = link.stats.arrivals.load(Ordering::Relaxed);
        let drains = link.stats.drains.load(Ordering::Relaxed);
        let delta_arrivals = arrivals.saturating_sub(controls[index].last_arrivals);
        let delta_drains = drains.saturating_sub(controls[index].last_drains);
        let len = link.receiver.len();
        let previous_len = controls[index].len;
        let arrival_rate = delta_arrivals as f64 / seconds;
        let drain_rate = delta_drains as f64 / seconds;
        let net_rate = arrival_rate - drain_rate;

        controls[index].last_arrivals = arrivals;
        controls[index].last_drains = drains;
        controls[index].previous_len = previous_len;
        controls[index].len = len;
        controls[index].arrival_rate =
            ewma(controls[index].arrival_rate, arrival_rate, RATE_EWMA_ALPHA);
        controls[index].drain_rate = ewma(controls[index].drain_rate, drain_rate, RATE_EWMA_ALPHA);
        controls[index].net_rate = ewma(controls[index].net_rate, net_rate, RATE_EWMA_ALPHA);
        controls[index].smoothed_len = ewma(controls[index].smoothed_len, len as f64, 0.25);
        controls[index].trend = classify_queue_trend(
            len,
            previous_len,
            controls[index].arrival_rate,
            controls[index].drain_rate,
            controls[index].net_rate,
        );
    }
}

fn ewma(previous: f64, sample: f64, alpha: f64) -> f64 {
    if previous == 0.0 {
        sample
    } else {
        (previous * (1.0 - alpha)) + (sample * alpha)
    }
}

fn classify_queue_trend(
    len: usize,
    previous_len: usize,
    arrival_rate: f64,
    drain_rate: f64,
    net_rate: f64,
) -> QueueTrend {
    let total_rate = arrival_rate + drain_rate;
    if len == 0 && total_rate < 0.01 {
        return QueueTrend::Starved;
    }
    if len == 0 && previous_len == 0 {
        return QueueTrend::Stable;
    }

    let basis = arrival_rate.max(drain_rate).max(1.0);
    let ratio = net_rate / basis;
    let length_delta = len as isize - previous_len as isize;

    if ratio <= -FAST_RATE_RATIO {
        QueueTrend::FastDraining
    } else if ratio <= -STABLE_RATE_RATIO {
        QueueTrend::Draining
    } else if ratio >= RUNAWAY_RATE_RATIO && length_delta > 0 {
        QueueTrend::Runaway
    } else if ratio >= FAST_RATE_RATIO {
        QueueTrend::FastGrowing
    } else if ratio >= STABLE_RATE_RATIO || length_delta > 2 {
        QueueTrend::Growing
    } else {
        QueueTrend::Stable
    }
}

fn collect_stage_samples<E>(
    workers: &[WorkerSlot<E>],
    active_by_stage: &[Vec<usize>],
    links: &[LinkControl],
    stages: &[RuntimeStage<E>],
    controls: &mut [StageControl],
) where
    E: Debug + Display + Send + 'static,
{
    for (stage_index, worker_ids) in active_by_stage.iter().enumerate() {
        let mut sample = StageSample::default();
        for worker_id in worker_ids {
            let stats = &workers[*worker_id].stats;
            sample.process_nanos += stats.process_nanos.swap(0, Ordering::Relaxed);
            sample.wait_nanos += stats.wait_nanos.swap(0, Ordering::Relaxed);
            sample.processed_items += stats.processed_items.swap(0, Ordering::Relaxed);
        }

        let control = &mut controls[stage_index];
        control.last_sample_processed = sample.processed_items;
        control.processed_count = control
            .processed_count
            .saturating_add(sample.processed_items);

        let total_nanos = sample.process_nanos.saturating_add(sample.wait_nanos);
        if total_nanos > 0 {
            control.busy_ratio = sample.process_nanos as f64 / total_nanos as f64;
        }

        if sample.processed_items > 0 && sample.process_nanos > 0 {
            let service_time = sample.process_nanos as f64 / sample.processed_items as f64;
            control.service_time_ewma =
                ewma(control.service_time_ewma, service_time, SERVICE_EWMA_ALPHA);
            control.per_worker_throughput = 1_000_000_000.0 / control.service_time_ewma.max(1.0);
        }

        let input_link = stages[stage_index].input_link;
        let output_link = stages[stage_index].output_link;

        if sample.processed_items > 0 || links[input_link].trend != QueueTrend::Starved {
            control.idle_samples = 0;
        } else if control.busy_ratio < 0.05 {
            control.idle_samples = control.idle_samples.saturating_add(1);
        }

        if control.settling {
            control.settle_samples = control.settle_samples.saturating_add(1);
            control.settle_observed_work |= sample.processed_items > 0;
            if control.settle_samples >= SETTLE_SAMPLES
                && (control.settle_observed_work || control.idle_samples >= IDLE_SHRINK_SAMPLES)
            {
                control.settling = false;
                control.scaling_state = StageScalingState::Eligible;
            }
        }

        let input_available = links[input_link].trend != QueueTrend::Starved;
        let output_unblocked = !links[output_link].trend.is_growing();
        if let Some(anchor) = control.anchor.as_mut() {
            if let Some(probe) = anchor.probe.as_mut() {
                probe.samples = probe.samples.saturating_add(1);
                probe.observed_work |= sample.processed_items > 0;
            } else if anchor.cooldown_samples > 0 {
                anchor.cooldown_samples -= 1;
                if anchor.cooldown_samples == 0 {
                    control.scaling_state = StageScalingState::Eligible;
                }
            }

            if !anchor.warmup_complete
                && sample.processed_items > 0
                && input_available
                && output_unblocked
            {
                anchor.warmup_complete = true;
                control.scaling_state = StageScalingState::Eligible;
            }
        }
    }
}

fn update_desired_workers<E>(
    links: &[LinkControl],
    active_by_stage: &[Vec<usize>],
    stages: &[RuntimeStage<E>],
    controls: &mut [StageControl],
) where
    E: Debug + Display + Send + 'static,
{
    for stage_index in 0..controls.len() {
        let active = active_by_stage[stage_index].len().max(1);
        let input = &links[stages[stage_index].input_link];
        let throughput = controls[stage_index].per_worker_throughput;
        let desired = if throughput > 0.0 {
            let mut required_rate = input.arrival_rate.max(0.0);
            if input.trend.is_growing() {
                required_rate += input.net_rate.max(0.0);
                required_rate += input.len as f64 / DEFAULT_BACKLOG_DRAIN_SECS;
            }
            (required_rate / throughput).ceil().max(1.0) as usize
        } else if input.trend.is_growing() {
            active.saturating_add(1)
        } else {
            active
        };
        controls[stage_index].desired_workers = desired.max(1);
    }
}

enum ScaleOperation {
    Add {
        stage_index: usize,
        reason: ScaleReason,
    },
    Remove {
        stage_index: usize,
        worker_id: Option<usize>,
        reason: ScaleReason,
    },
}

fn choose_scale_operation<E>(
    links: &[LinkControl],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    stages: &[RuntimeStage<E>],
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if let Some(operation) = choose_support_operation(
        links,
        active_by_stage,
        controls,
        stages,
        output_link,
        global_worker_cap,
    ) {
        return Some(operation);
    }

    let scalable_anchors: Vec<_> = scalable_anchor_indices(controls).collect();
    for anchor_index in scalable_anchors {
        if let Some(operation) = choose_anchor_probe_operation(
            links,
            active_by_stage,
            controls,
            stages,
            anchor_index,
            output_link,
            global_worker_cap,
        ) {
            return Some(operation);
        }
    }

    let scalable_anchors: Vec<_> = scalable_anchor_indices(controls).collect();
    for anchor_index in scalable_anchors {
        if let Some(operation) = choose_anchor_operation(
            links,
            active_by_stage,
            controls,
            stages,
            anchor_index,
            output_link,
            global_worker_cap,
        ) {
            return Some(operation);
        }
    }

    choose_idle_operation(links, active_by_stage, controls, stages)
}

fn choose_anchor_probe_operation<E>(
    links: &[LinkControl],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    stages: &[RuntimeStage<E>],
    anchor_index: usize,
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if support_growth_is_settling(controls, anchor_index) {
        return None;
    }

    let control = &mut controls[anchor_index];
    let anchor = control.anchor.as_mut()?;

    if let Some(probe) = anchor.probe.as_ref() {
        if probe.samples < PROBE_SETTLE_SAMPLES || !probe.observed_work {
            return None;
        }

        let input_link = stages[anchor_index].input_link;
        let stage_output_link = stages[anchor_index].output_link;
        let output_backpressure = links[output_link].trend.is_growing();
        let input_underfed =
            link_underfeeds_stage(&links[input_link], active_by_stage[anchor_index].len());
        let output_unstable = links[stage_output_link].trend.is_growing();
        let budget_pressure = active_worker_count(active_by_stage) >= global_worker_cap
            && active_by_stage[anchor_index].len() > 1;
        let too_idle = control.busy_ratio < 0.45 && !links[input_link].trend.is_growing();

        let revert_reason = if output_backpressure {
            Some(AnchorProbeReason::OutputBackpressure)
        } else if budget_pressure {
            Some(AnchorProbeReason::BudgetPressure)
        } else if input_underfed {
            Some(AnchorProbeReason::InputUnderfed)
        } else if output_unstable {
            Some(AnchorProbeReason::SupportUnstable)
        } else if too_idle {
            Some(AnchorProbeReason::Idle)
        } else {
            None
        };

        if let Some(reason) = revert_reason {
            anchor.last_probe_reason = reason;
            return Some(ScaleOperation::Remove {
                stage_index: anchor_index,
                worker_id: Some(probe.worker_id),
                reason: ScaleReason::AnchorRevert,
            });
        }

        anchor.probe = None;
        anchor.last_probe_outcome = AnchorProbeOutcome::Kept;
        anchor.last_probe_reason = AnchorProbeReason::None;
        anchor.cooldown_samples = 2;
        control.scaling_state = StageScalingState::Eligible;
    }

    None
}

fn support_growth_is_settling(controls: &[StageControl], anchor_index: usize) -> bool {
    controls.iter().enumerate().any(|(stage_index, control)| {
        stage_index != anchor_index
            && control.settling
            && matches!(control.last_operation, Some((ScaleDirection::Add, _)))
    })
}

fn choose_support_operation<E>(
    links: &[LinkControl],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    stages: &[RuntimeStage<E>],
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if links[output_link].trend.is_growing() {
        let scalable_anchors: Vec<_> = scalable_anchor_indices(controls).collect();
        for anchor_index in scalable_anchors {
            if active_by_stage[anchor_index].len() > 1 && stage_can_scale(&controls[anchor_index]) {
                set_anchor_reason(
                    controls,
                    anchor_index,
                    AnchorProbeReason::OutputBackpressure,
                );
                return Some(ScaleOperation::Remove {
                    stage_index: anchor_index,
                    worker_id: None,
                    reason: ScaleReason::AnchorRevert,
                });
            }
        }
        return None;
    }

    for link_index in 0..links.len() {
        if !links[link_index].trend.is_growing() {
            continue;
        }
        for consumer_stage in consumer_stages(stages, link_index) {
            if controls[consumer_stage]
                .anchor
                .as_ref()
                .is_some_and(|anchor| anchor.fixed_threads.is_some())
            {
                continue;
            }
            if controls[consumer_stage].anchor.is_some() {
                continue;
            }
            if active_by_stage[consumer_stage].len() < controls[consumer_stage].desired_workers
                && support_can_add(consumer_stage, controls)
            {
                return add_or_rebalance_for_stage(
                    consumer_stage,
                    active_by_stage,
                    controls,
                    global_worker_cap,
                );
            }
        }
    }

    let anchors: Vec<_> = anchor_indices(controls).collect();
    for anchor_index in anchors {
        let input_link = stages[anchor_index].input_link;
        let anchor_input = &links[input_link];
        if !link_underfeeds_stage(anchor_input, active_by_stage[anchor_index].len())
            || controls[anchor_index].busy_ratio <= 0.60
        {
            continue;
        }
        for stage_index in producer_stages(stages, input_link) {
            if controls[stage_index]
                .anchor
                .as_ref()
                .is_some_and(|anchor| anchor.fixed_threads.is_some())
            {
                continue;
            }
            if links[stages[stage_index].input_link].trend == QueueTrend::Starved {
                continue;
            }
            if support_can_add(stage_index, controls)
                && active_by_stage[stage_index].len()
                    < controls[stage_index]
                        .desired_workers
                        .max(active_by_stage[stage_index].len() + 1)
            {
                return add_or_rebalance_for_stage(
                    stage_index,
                    active_by_stage,
                    controls,
                    global_worker_cap,
                );
            }
        }
        if controls[anchor_index]
            .anchor
            .as_ref()
            .is_some_and(|anchor| anchor.fixed_threads.is_none())
            && active_by_stage[anchor_index].len() > 1
            && stage_can_scale(&controls[anchor_index])
        {
            set_anchor_reason(controls, anchor_index, AnchorProbeReason::InputUnderfed);
            return Some(ScaleOperation::Remove {
                stage_index: anchor_index,
                worker_id: None,
                reason: ScaleReason::AnchorRevert,
            });
        }
    }

    None
}

fn choose_anchor_operation<E>(
    links: &[LinkControl],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    stages: &[RuntimeStage<E>],
    anchor_index: usize,
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    let control = &mut controls[anchor_index];
    let anchor = control.anchor.as_mut()?;
    if anchor.fixed_threads.is_some() {
        return None;
    }

    if anchor.probe.is_some() {
        return None;
    }

    if anchor.cooldown_samples > 0 {
        control.scaling_state = StageScalingState::BackingOff;
        return None;
    }

    let active = active_by_stage[anchor_index].len();
    if active >= anchor.max_threads {
        control.scaling_state = StageScalingState::Eligible;
        return None;
    }

    if !anchor.warmup_complete {
        control.scaling_state = StageScalingState::BackingOff;
        return None;
    }

    let has_budget = active_worker_count(active_by_stage) < global_worker_cap;
    let input_link = stages[anchor_index].input_link;
    let stage_output_link = stages[anchor_index].output_link;
    let input_ready = !link_underfeeds_stage(&links[input_link], active);
    let downstream_ready =
        !links[stage_output_link].trend.is_growing() && !links[output_link].trend.is_growing();
    let enough_busy_signal = control.busy_ratio >= 0.70 || links[input_link].trend.is_growing();

    if has_budget && input_ready && downstream_ready && enough_busy_signal {
        return Some(ScaleOperation::Add {
            stage_index: anchor_index,
            reason: ScaleReason::AnchorProbe,
        });
    }

    control.scaling_state = StageScalingState::Eligible;
    None
}

fn choose_idle_operation<E>(
    links: &[LinkControl],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    stages: &[RuntimeStage<E>],
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    for stage_index in 0..controls.len() {
        if controls[stage_index]
            .anchor
            .as_ref()
            .is_some_and(|anchor| anchor.fixed_threads.is_some())
        {
            continue;
        }
        if active_by_stage[stage_index].len() <= 1 || !stage_can_scale(&controls[stage_index]) {
            continue;
        }
        if controls[stage_index].anchor.is_none()
            && active_by_stage[stage_index].len() > controls[stage_index].desired_workers.max(1)
            && !links[stages[stage_index].input_link].trend.is_growing()
        {
            return Some(ScaleOperation::Remove {
                stage_index,
                worker_id: None,
                reason: ScaleReason::Support,
            });
        }
        if controls[stage_index].idle_samples >= IDLE_SHRINK_SAMPLES {
            if controls[stage_index].anchor.is_some() {
                set_anchor_reason(controls, stage_index, AnchorProbeReason::Idle);
            }
            return Some(ScaleOperation::Remove {
                stage_index,
                worker_id: None,
                reason: ScaleReason::Idle,
            });
        }
    }
    None
}

fn support_can_add(stage_index: usize, controls: &[StageControl]) -> bool {
    stage_can_scale(&controls[stage_index])
}

fn link_underfeeds_stage(link: &LinkControl, active_threads: usize) -> bool {
    link.trend == QueueTrend::Starved
        || (link.trend.is_draining() && link.len <= active_threads.saturating_mul(2).max(1))
}

fn stage_can_scale(control: &StageControl) -> bool {
    !control
        .anchor
        .as_ref()
        .is_some_and(|anchor| anchor.fixed_threads.is_some())
        && !control.settling
        && !matches!(
            control.scaling_state,
            StageScalingState::Settling | StageScalingState::Probing
        )
}

fn add_or_rebalance_for_stage(
    stage_index: usize,
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    global_worker_cap: usize,
) -> Option<ScaleOperation> {
    if active_worker_count(active_by_stage) < global_worker_cap {
        return Some(ScaleOperation::Add {
            stage_index,
            reason: ScaleReason::Support,
        });
    }

    let scalable_anchors: Vec<_> = scalable_anchor_indices(controls).collect();
    for anchor_index in scalable_anchors {
        if stage_index != anchor_index
            && active_by_stage[anchor_index].len() > 1
            && stage_can_scale(&controls[anchor_index])
        {
            set_anchor_reason(controls, anchor_index, AnchorProbeReason::BudgetPressure);
            return Some(ScaleOperation::Remove {
                stage_index: anchor_index,
                worker_id: None,
                reason: ScaleReason::BudgetPressure,
            });
        }
    }

    None
}

fn active_worker_count(active_by_stage: &[Vec<usize>]) -> usize {
    active_by_stage.iter().map(Vec::len).sum()
}

fn anchor_indices(controls: &[StageControl]) -> impl Iterator<Item = usize> + '_ {
    controls
        .iter()
        .enumerate()
        .filter_map(|(index, control)| control.anchor.as_ref().map(|_| index))
}

fn scalable_anchor_indices(controls: &[StageControl]) -> impl Iterator<Item = usize> + '_ {
    controls.iter().enumerate().filter_map(|(index, control)| {
        control
            .anchor
            .as_ref()
            .filter(|anchor| anchor.fixed_threads.is_none())
            .map(|_| index)
    })
}

fn consumer_stages<E>(
    stages: &[RuntimeStage<E>],
    link_index: usize,
) -> impl Iterator<Item = usize> + '_
where
    E: Debug + Display + Send + 'static,
{
    stages
        .iter()
        .enumerate()
        .filter_map(move |(index, stage)| (stage.input_link == link_index).then_some(index))
}

fn producer_stages<E>(
    stages: &[RuntimeStage<E>],
    link_index: usize,
) -> impl Iterator<Item = usize> + '_
where
    E: Debug + Display + Send + 'static,
{
    stages
        .iter()
        .enumerate()
        .filter_map(move |(index, stage)| (stage.output_link == link_index).then_some(index))
}

fn set_anchor_reason(
    controls: &mut [StageControl],
    anchor_index: usize,
    reason: AnchorProbeReason,
) {
    if let Some(anchor) = controls[anchor_index].anchor.as_mut() {
        anchor.last_probe_reason = reason;
    }
}

fn grow_sample_count(current: u32) -> u32 {
    current.saturating_mul(2).max(4).min(64)
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn anchor_probe_state(anchor: &AnchorControl, active_threads: usize) -> AnchorProbeState {
    if !anchor.warmup_complete {
        AnchorProbeState::WarmingUp
    } else if anchor.probe.is_some() {
        AnchorProbeState::Probing
    } else if active_threads >= anchor.max_threads {
        AnchorProbeState::AtMax
    } else if anchor.cooldown_samples > 0 {
        AnchorProbeState::BackingOff
    } else {
        AnchorProbeState::Eligible
    }
}

#[allow(clippy::too_many_arguments)]
fn update_snapshot<E>(
    snapshot: &Arc<RwLock<PiperSnapshot>>,
    links: &[Link],
    stages: &[RuntimeStage<E>],
    active_by_stage: &[Vec<usize>],
    parked_threads: usize,
    link_controls: &[LinkControl],
    controls: &[StageControl],
    output_link: usize,
    global_worker_cap: usize,
    shutdown_requested: bool,
    abort_requested: bool,
    pending_scale_operation: bool,
) where
    E: Debug + Display + Send + 'static,
{
    let mut snapshot = snapshot.write();
    snapshot.links = link_controls
        .iter()
        .enumerate()
        .map(|(index, control)| LinkSnapshot {
            index,
            len: links[index].receiver.len(),
            trend: control.trend,
            arrival_rate: control.arrival_rate,
            drain_rate: control.drain_rate,
            net_rate: control.net_rate,
            smoothed_len: control.smoothed_len,
        })
        .collect();
    snapshot.stages = stages
        .iter()
        .enumerate()
        .map(|(index, stage)| StageSnapshot {
            index,
            name: stage.name.clone(),
            input_link: stage.input_link,
            output_link: stage.output_link,
            active_threads: active_by_stage[index].len(),
            processed_count: controls[index].processed_count,
            busy_ratio: controls[index].busy_ratio,
            service_time: Duration::from_nanos(
                controls[index]
                    .service_time_ewma
                    .max(0.0)
                    .min(u64::MAX as f64) as u64,
            ),
            per_worker_throughput: controls[index].per_worker_throughput,
            desired_workers: controls[index].desired_workers,
            scaling_state: controls[index].scaling_state,
            is_anchor: controls[index].anchor.is_some(),
            is_fixed_anchor: controls[index]
                .anchor
                .as_ref()
                .is_some_and(|anchor| anchor.fixed_threads.is_some()),
        })
        .collect();
    snapshot.anchors = controls
        .iter()
        .enumerate()
        .filter_map(|(index, control)| {
            control.anchor.as_ref().map(|anchor| AnchorSnapshot {
                stage_index: index,
                stage_name: stages[index].name.clone(),
                active_threads: active_by_stage[index].len(),
                max_threads: anchor.max_threads,
                fixed_threads: anchor.fixed_threads,
                probe_state: anchor_probe_state(anchor, active_by_stage[index].len()),
                last_probe_outcome: anchor.last_probe_outcome,
                last_probe_reason: anchor.last_probe_reason,
            })
        })
        .collect();
    snapshot.parked_threads = parked_threads;
    snapshot.total_active_workers = active_worker_count(active_by_stage);
    snapshot.global_worker_cap = global_worker_cap;
    snapshot.budget_pressure = snapshot.total_active_workers >= global_worker_cap;
    snapshot.output_backpressure = link_controls[output_link].trend.is_growing();
    snapshot.shutdown_requested = shutdown_requested;
    snapshot.abort_requested = abort_requested;
    snapshot.pending_scale_operation = pending_scale_operation;
}

pub fn panic_payload_to_string(payload: Box<dyn Any + Send + 'static>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        String::from("<non-string panic payload>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    enum TestError {
        #[error("boom")]
        Boom,
    }

    struct TestStage;

    impl Stage for TestStage {
        type Input = u8;
        type Output = u8;
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

    fn test_config() -> PiperConfig {
        PiperConfig {
            sample_interval: Duration::from_millis(1),
            poll_interval: Duration::from_millis(1),
            global_worker_cap: Some(8),
            csv_telemetry: None,
        }
    }

    fn support_control() -> StageControl {
        StageControl {
            processed_count: 0,
            busy_ratio: 0.0,
            service_time_ewma: 1_000_000.0,
            per_worker_throughput: 1_000.0,
            desired_workers: 1,
            last_sample_processed: 0,
            scaling_state: StageScalingState::Eligible,
            settling: false,
            settle_samples: 0,
            settle_observed_work: false,
            idle_samples: 0,
            last_operation: None,
            anchor: None,
        }
    }

    fn anchor_control(max_threads: usize) -> StageControl {
        StageControl {
            anchor: Some(AnchorControl {
                max_threads,
                fixed_threads: None,
                warmup_complete: true,
                probe: None,
                cooldown_samples: 0,
                last_probe_outcome: AnchorProbeOutcome::None,
                last_probe_reason: AnchorProbeReason::None,
            }),
            busy_ratio: 0.8,
            desired_workers: max_threads,
            ..support_control()
        }
    }

    fn link(trend: QueueTrend) -> LinkControl {
        LinkControl {
            trend,
            len: if trend.is_growing() { 8 } else { 0 },
            previous_len: 4,
            arrival_rate: if trend.is_growing() { 100.0 } else { 10.0 },
            drain_rate: if trend.is_draining() { 100.0 } else { 10.0 },
            net_rate: if trend.is_growing() {
                90.0
            } else if trend.is_draining() {
                -90.0
            } else {
                0.0
            },
            ..LinkControl::default()
        }
    }

    fn linear_runtime_stages(count: usize) -> Vec<RuntimeStage<TestError>> {
        (0..count)
            .map(|index| RuntimeStage {
                name: format!("stage{index}"),
                stage: Arc::new(StageAdapter { stage: TestStage }),
                output_acquire: None,
                anchor: None,
                input_link: index,
                output_link: index + 1,
            })
            .collect()
    }

    #[test]
    fn queue_trend_classification_distinguishes_stable_empty_from_starved() {
        assert_eq!(
            classify_queue_trend(0, 0, 0.0, 0.0, 0.0),
            QueueTrend::Starved
        );
        assert_eq!(
            classify_queue_trend(0, 0, 100.0, 100.0, 0.0),
            QueueTrend::Stable
        );
        assert_eq!(
            classify_queue_trend(20, 10, 100.0, 10.0, 90.0),
            QueueTrend::Runaway
        );
        assert_eq!(
            classify_queue_trend(2, 20, 10.0, 100.0, -90.0),
            QueueTrend::FastDraining
        );
    }

    #[test]
    fn link_sampling_tracks_rates_and_numeric_trends() {
        let (sender, receiver) = kanal::unbounded::<Message>();
        let links = vec![Link {
            sender: Some(sender.clone()),
            receiver,
            stats: Arc::new(LinkStats::default()),
        }];
        let mut controls = vec![LinkControl::default()];

        for _ in 0..10 {
            sender.send(Box::new(1_u8)).unwrap();
            links[0].stats.arrivals.fetch_add(1, Ordering::Relaxed);
        }
        sample_links(&links, Duration::from_secs(1), &mut controls);

        assert_eq!(controls[0].arrival_rate, 10.0);
        assert_eq!(controls[0].drain_rate, 0.0);
        assert!(controls[0].trend.is_growing());
        assert_eq!(QueueTrend::Runaway.code(), 6);
    }

    #[test]
    fn growing_internal_link_adds_consumer() {
        let active = vec![vec![0], vec![1]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Growing),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![anchor_control(1), support_control()];
        controls[1].desired_workers = 2;
        let stages = linear_runtime_stages(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &stages, 2, 4),
            Some(ScaleOperation::Add {
                stage_index: 1,
                reason: ScaleReason::Support
            })
        ));
    }

    #[test]
    fn draining_anchor_input_adds_nearest_upstream_producer() {
        let active = vec![vec![0], vec![1], vec![2, 3]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
            link(QueueTrend::Draining),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![support_control(), support_control(), anchor_control(4)];
        controls[1].desired_workers = 2;
        let stages = linear_runtime_stages(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &stages, 3, 6),
            Some(ScaleOperation::Add {
                stage_index: 1,
                reason: ScaleReason::Support
            })
        ));
    }

    #[test]
    fn unsuppliable_anchor_input_reduces_anchor() {
        let active = vec![vec![0], vec![1], vec![2, 3]];
        let links = vec![
            link(QueueTrend::Starved),
            link(QueueTrend::Starved),
            link(QueueTrend::Draining),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![support_control(), support_control(), anchor_control(4)];
        let stages = linear_runtime_stages(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &stages, 3, 6),
            Some(ScaleOperation::Remove {
                stage_index: 2,
                reason: ScaleReason::AnchorRevert,
                ..
            })
        ));
        assert_eq!(
            controls[2].anchor.as_ref().unwrap().last_probe_reason,
            AnchorProbeReason::InputUnderfed
        );
    }

    #[test]
    fn global_cap_full_reduces_anchor_before_support_growth() {
        let active = vec![vec![0, 1], vec![2]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Growing),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![anchor_control(4), support_control()];
        controls[1].desired_workers = 2;
        let stages = linear_runtime_stages(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &stages, 2, 3),
            Some(ScaleOperation::Remove {
                stage_index: 0,
                reason: ScaleReason::BudgetPressure,
                ..
            })
        ));
    }

    #[test]
    fn output_growth_sets_backpressure_and_blocks_anchor_scaling() {
        let active = vec![vec![0, 1], vec![2]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
            link(QueueTrend::Growing),
        ];
        let mut controls = vec![anchor_control(4), support_control()];
        let stages = linear_runtime_stages(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &stages, 2, 4),
            Some(ScaleOperation::Remove {
                stage_index: 0,
                reason: ScaleReason::AnchorRevert,
                ..
            })
        ));
        assert_eq!(
            controls[0].anchor.as_ref().unwrap().last_probe_reason,
            AnchorProbeReason::OutputBackpressure
        );
    }

    #[test]
    fn support_shrink_settling_does_not_block_anchor_probe_decision() {
        let active = vec![vec![0], vec![1, 2, 3]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![support_control(), anchor_control(4)];
        controls[0].settling = true;
        controls[0].last_operation = Some((ScaleDirection::Remove, Instant::now()));
        controls[1].scaling_state = StageScalingState::Probing;
        controls[1].anchor.as_mut().unwrap().probe = Some(AnchorProbe {
            worker_id: 3,
            samples: PROBE_SETTLE_SAMPLES,
            observed_work: true,
        });
        let stages = linear_runtime_stages(active.len());

        assert!(
            choose_anchor_probe_operation(&links, &active, &mut controls, &stages, 1, 2, 8)
                .is_none()
        );
        let anchor = controls[1].anchor.as_ref().unwrap();
        assert!(anchor.probe.is_none());
        assert_eq!(anchor.last_probe_outcome, AnchorProbeOutcome::Kept);
        assert_eq!(anchor.last_probe_reason, AnchorProbeReason::None);
        assert_eq!(anchor.cooldown_samples, 2);
        assert_eq!(controls[1].scaling_state, StageScalingState::Eligible);
    }

    #[test]
    fn support_growth_settling_still_blocks_anchor_probe_decision() {
        let active = vec![vec![0], vec![1, 2, 3]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![support_control(), anchor_control(4)];
        controls[0].settling = true;
        controls[0].last_operation = Some((ScaleDirection::Add, Instant::now()));
        controls[1].scaling_state = StageScalingState::Probing;
        controls[1].anchor.as_mut().unwrap().probe = Some(AnchorProbe {
            worker_id: 3,
            samples: PROBE_SETTLE_SAMPLES,
            observed_work: true,
        });
        let stages = linear_runtime_stages(active.len());

        assert!(
            choose_anchor_probe_operation(&links, &active, &mut controls, &stages, 1, 2, 8)
                .is_none()
        );
        let anchor = controls[1].anchor.as_ref().unwrap();
        assert!(anchor.probe.is_some());
        assert_eq!(anchor.last_probe_outcome, AnchorProbeOutcome::None);
        assert_eq!(anchor.cooldown_samples, 0);
        assert_eq!(controls[1].scaling_state, StageScalingState::Probing);
    }

    #[test]
    fn delayed_idle_shrink_preserves_short_gaps_then_removes_one() {
        let active = vec![vec![0], vec![1, 2]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Starved),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![anchor_control(1), support_control()];
        controls[1].desired_workers = 2;
        controls[1].idle_samples = IDLE_SHRINK_SAMPLES - 1;
        let stages = linear_runtime_stages(active.len());

        assert!(choose_idle_operation(&links, &active, &mut controls, &stages).is_none());

        controls[1].idle_samples = IDLE_SHRINK_SAMPLES;
        assert!(matches!(
            choose_idle_operation(&links, &active, &mut controls, &stages),
            Some(ScaleOperation::Remove {
                stage_index: 1,
                reason: ScaleReason::Idle,
                ..
            })
        ));
    }

    #[test]
    fn lease_returns_recycled_value_on_drop() {
        let (recycle_sender, recycle_receiver) = kanal::unbounded();
        let (failure_sender, _failure_receiver) = kanal::unbounded();
        let runtime = LeaseRuntime {
            shutdown: Arc::new(AtomicBool::new(false)),
            abort: Arc::new(AtomicBool::new(false)),
            internal_failure: failure_sender,
        };

        {
            let mut lease = BufferLease::new(vec![1, 2, 3], recycle_sender, runtime);
            lease.push(4);
        }

        let recycled = recycle_receiver.recv().unwrap();
        assert!(recycled.is_empty());
        assert!(recycled.capacity() >= 4);
    }

    #[test]
    fn lease_recycle_failure_is_fatal_outside_shutdown() {
        let (recycle_sender, recycle_receiver) = kanal::unbounded();
        drop(recycle_receiver);
        let (failure_sender, failure_receiver) = kanal::unbounded();
        let abort = Arc::new(AtomicBool::new(false));
        let runtime = LeaseRuntime {
            shutdown: Arc::new(AtomicBool::new(false)),
            abort: Arc::clone(&abort),
            internal_failure: failure_sender,
        };

        drop(BufferLease::new(vec![1], recycle_sender, runtime));

        assert!(abort.load(Ordering::Acquire));
        let InternalFailure::Internal { message } = failure_receiver.recv().unwrap() else {
            panic!("expected internal recycle failure");
        };
        assert!(message.contains("recycle"));
    }

    #[test]
    fn cleanup_failure_is_reported_from_join() {
        let config = test_config();
        let mut builder = PipelineGraphBuilder::<u8, TestError>::new();
        let input = builder.input();
        let output = builder.add_stage(
            input,
            anchor(
                inline_stage(
                    "cleanup",
                    || -> std::result::Result<(), TestError> { Ok(()) },
                    |_state: &mut (), input: u8, ctx: &mut StageContext<u8, TestError>| {
                        ctx.emit(input);
                        Ok(())
                    },
                )
                .with_cleanup(|_state| Err(TestError::Boom)),
            )
            .max_threads(1),
        );
        let piper = Piper::<u8, u8, TestError>::start(config, builder.finish(output)).unwrap();

        piper.shutdown();
        let error = piper.join().expect_err("cleanup failure should fail join");
        assert!(matches!(error, PiperError::UserCleanup { .. }));
    }
}
