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

    #[error("Piper requires exactly one anchor stage, found {count}")]
    InvalidAnchorCount { count: usize },

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
    pub scale_cooldown: Duration,
    pub add_dwell: Duration,
    pub remove_dwell: Duration,
    pub low_water: usize,
    pub high_water: usize,
    pub csv_telemetry: Option<CsvTelemetryConfig>,
}

impl Default for PiperConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_millis(10),
            poll_interval: Duration::from_millis(10),
            scale_cooldown: Duration::from_millis(10),
            add_dwell: Duration::from_millis(50),
            remove_dwell: Duration::from_millis(250),
            low_water: 1,
            high_water: 64,
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
pub enum WaterState {
    Starved,
    BelowLowWater,
    Nominal,
    AboveHighWater,
}

impl WaterState {
    pub fn classify(len: usize, low_water: usize, high_water: usize) -> Self {
        if len == 0 {
            WaterState::Starved
        } else if len < low_water {
            WaterState::BelowLowWater
        } else if len > high_water {
            WaterState::AboveHighWater
        } else {
            WaterState::Nominal
        }
    }

    pub fn is_low_pressure(self) -> bool {
        matches!(self, WaterState::Starved | WaterState::BelowLowWater)
    }
}

#[derive(Clone, Debug)]
pub struct LinkSnapshot {
    pub index: usize,
    pub len: usize,
    pub state: WaterState,
}

#[derive(Clone, Debug)]
pub struct StageSnapshot {
    pub index: usize,
    pub name: String,
    pub active_threads: usize,
    pub processed_count: u64,
    pub busy_ratio: f64,
    pub busy_ceiling: f64,
    pub effective_add_dwell: Duration,
    pub effective_remove_dwell: Duration,
    pub scaling_state: StageScalingState,
    pub is_anchor: bool,
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

#[derive(Clone, Debug)]
pub struct AnchorSnapshot {
    pub stage_index: usize,
    pub stage_name: String,
    pub active_threads: usize,
    pub max_threads: usize,
    pub probe_state: AnchorProbeState,
    pub effective_probe_interval: Duration,
    pub last_probe_outcome: AnchorProbeOutcome,
}

#[derive(Clone, Debug)]
pub struct PiperSnapshot {
    pub links: Vec<LinkSnapshot>,
    pub stages: Vec<StageSnapshot>,
    pub anchor: AnchorSnapshot,
    pub parked_threads: usize,
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
    shutdown: Arc<AtomicBool>,
    _marker: PhantomData<fn(In)>,
}

impl<In> Clone for PiperSender<In> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
        self.inner.send(Box::new(input)).map_err(|_| SendInputError)
    }

    pub fn is_closed(&self) -> bool {
        self.shutdown.load(Ordering::Acquire) || self.inner.is_closed()
    }
}

pub struct PiperReceiver<Out> {
    inner: kanal::Receiver<Message>,
    _marker: PhantomData<fn() -> Out>,
}

impl<Out> Clone for PiperReceiver<Out> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
        if self.output.send(Box::new(output)).is_err()
            && !self.shutdown.load(Ordering::Acquire)
            && !self.abort.load(Ordering::Acquire)
        {
            self.abort.store(true, Ordering::Release);
            let _ = self.internal_failure.send(InternalFailure::internal(
                "stage output channel closed unexpectedly",
            ));
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

pub struct StageSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Arc<dyn DynStage<E>>,
    output_acquire_builder: Option<Arc<dyn OutputAcquireBuilder + Send + Sync>>,
    anchor: Option<AnchorHints>,
    dwell_hints: StageDwellHints,
}

impl<E> StageSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    pub fn with_reusable_output<T, Factory>(mut self, factory: Factory) -> Self
    where
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        self.output_acquire_builder = Some(Arc::new(RecycleAcquireBuilder {
            factory: Arc::new(factory),
            _marker: PhantomData::<fn() -> T>,
        }));
        self
    }

    pub fn with_dwell(mut self, add: Duration, remove: Duration) -> Self {
        self.dwell_hints.add = Some(add);
        self.dwell_hints.remove = Some(remove);
        self
    }

    pub fn with_add_dwell(mut self, add: Duration) -> Self {
        self.dwell_hints.add = Some(add);
        self
    }

    pub fn with_remove_dwell(mut self, remove: Duration) -> Self {
        self.dwell_hints.remove = Some(remove);
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

    pub fn probe_interval(mut self, interval: Duration) -> Self {
        self.anchor
            .get_or_insert_with(AnchorHints::default)
            .probe_interval = Some(interval);
        self
    }

    pub fn probe_window(mut self, window: Duration) -> Self {
        self.anchor
            .get_or_insert_with(AnchorHints::default)
            .probe_window = Some(window);
        self
    }

    pub fn remove_dwell(mut self, dwell: Duration) -> Self {
        self.anchor
            .get_or_insert_with(AnchorHints::default)
            .remove_dwell = Some(dwell);
        self
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StageDwellHints {
    add: Option<Duration>,
    remove: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default)]
struct AnchorHints {
    max_threads: Option<usize>,
    initial_threads: Option<usize>,
    probe_interval: Option<Duration>,
    probe_window: Option<Duration>,
    remove_dwell: Option<Duration>,
}

pub trait IntoStageSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    fn into_stage_spec(self) -> StageSpec<E>;
}

impl<E> IntoStageSpec<E> for StageSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    fn into_stage_spec(self) -> StageSpec<E> {
        self
    }
}

impl<S> IntoStageSpec<S::Error> for S
where
    S: Stage,
{
    fn into_stage_spec(self) -> StageSpec<S::Error> {
        stage(default_stage_name::<S>(), self)
    }
}

pub trait StageExt: Stage + Sized {
    fn with_reusable_output<T, Factory>(self, factory: Factory) -> StageSpec<Self::Error>
    where
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        stage(default_stage_name::<Self>(), self).with_reusable_output(factory)
    }

    fn with_dwell(self, add: Duration, remove: Duration) -> StageSpec<Self::Error> {
        stage(default_stage_name::<Self>(), self).with_dwell(add, remove)
    }

    fn with_add_dwell(self, add: Duration) -> StageSpec<Self::Error> {
        stage(default_stage_name::<Self>(), self).with_add_dwell(add)
    }

    fn with_remove_dwell(self, remove: Duration) -> StageSpec<Self::Error> {
        stage(default_stage_name::<Self>(), self).with_remove_dwell(remove)
    }
}

impl<S> StageExt for S where S: Stage {}

pub fn stage<S>(name: impl Into<String>, stage_impl: S) -> StageSpec<S::Error>
where
    S: Stage,
{
    StageSpec {
        name: name.into(),
        stage: Arc::new(StageAdapter { stage: stage_impl }),
        output_acquire_builder: None,
        anchor: None,
        dwell_hints: StageDwellHints::default(),
    }
}

pub fn anchor<S, E>(stage_like: S) -> StageSpec<E>
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

    pub fn with_reusable_output<T, Factory>(self, factory: Factory) -> StageSpec<E>
    where
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
    fn into_stage_spec(self) -> StageSpec<E> {
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
    dwell_hints: StageDwellHints,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedAnchor {
    max_threads: usize,
    initial_threads: usize,
    probe_interval: Duration,
    probe_window: Duration,
    remove_dwell: Duration,
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
    output: kanal::Sender<Message>,
    output_acquire: Option<DynAcquire>,
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
}

struct PendingScale {
    worker_id: usize,
    stage_index: usize,
    direction: ScaleDirection,
    reason: ScaleReason,
}

#[derive(Clone, Copy)]
struct PressureTimer {
    state: WaterState,
    low_since: Option<Instant>,
    high_since: Option<Instant>,
}

impl Default for PressureTimer {
    fn default() -> Self {
        Self {
            state: WaterState::Starved,
            low_since: None,
            high_since: None,
        }
    }
}

struct StageControl {
    base_add_dwell: Duration,
    base_remove_dwell: Duration,
    effective_add_dwell: Duration,
    effective_remove_dwell: Duration,
    processed_count: u64,
    busy_ratio: f64,
    busy_ceiling: f64,
    last_sample_processed: u64,
    scaling_state: StageScalingState,
    settling: bool,
    last_operation: Option<(ScaleDirection, Instant)>,
    anchor: Option<AnchorControl>,
}

struct AnchorControl {
    max_threads: usize,
    probe_interval: Duration,
    probe_window: Duration,
    remove_dwell: Duration,
    warmup_complete: bool,
    probe: Option<AnchorProbe>,
    next_probe_after: Instant,
    last_probe_outcome: AnchorProbeOutcome,
}

struct AnchorProbe {
    worker_id: usize,
    started_at: Instant,
}

#[derive(Default)]
struct StageSample {
    process_nanos: u64,
    wait_nanos: u64,
    processed_items: u64,
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

    Ok(ResolvedAnchor {
        max_threads,
        initial_threads: initial_threads.min(max_threads),
        probe_interval: hints.probe_interval.unwrap_or(Duration::from_millis(100)),
        probe_window: hints.probe_window.unwrap_or(Duration::from_millis(250)),
        remove_dwell: hints.remove_dwell.unwrap_or(Duration::from_millis(500)),
    })
}

fn build_stage_controls<E>(config: &PiperConfig, stages: &[RuntimeStage<E>]) -> Vec<StageControl>
where
    E: Debug + Display + Send + 'static,
{
    let now = Instant::now();
    stages
        .iter()
        .map(|stage| {
            let base_add_dwell = stage.dwell_hints.add.unwrap_or(config.add_dwell);
            let base_remove_dwell = stage.dwell_hints.remove.unwrap_or(config.remove_dwell);
            StageControl {
                base_add_dwell,
                base_remove_dwell,
                effective_add_dwell: base_add_dwell,
                effective_remove_dwell: base_remove_dwell,
                processed_count: 0,
                busy_ratio: 0.0,
                busy_ceiling: 0.0,
                last_sample_processed: 0,
                scaling_state: StageScalingState::Eligible,
                settling: false,
                last_operation: None,
                anchor: stage.anchor.map(|anchor| AnchorControl {
                    max_threads: anchor.max_threads,
                    probe_interval: anchor.probe_interval,
                    probe_window: anchor.probe_window,
                    remove_dwell: anchor.remove_dwell,
                    warmup_complete: false,
                    probe: None,
                    next_probe_after: now,
                    last_probe_outcome: AnchorProbeOutcome::None,
                }),
            }
        })
        .collect()
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
    pub fn start(config: PiperConfig, stages: Vec<StageSpec<E>>) -> Result<Self, E> {
        if stages.is_empty() {
            return Err(PiperError::NoStages);
        }
        let anchor_count = stages.iter().filter(|stage| stage.anchor.is_some()).count();
        if anchor_count != 1 {
            return Err(PiperError::InvalidAnchorCount {
                count: anchor_count,
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

        let mut links = Vec::with_capacity(stages.len() + 1);
        for _ in 0..=stages.len() {
            let (sender, receiver) = kanal::unbounded::<Message>();
            links.push(Link {
                sender: Some(sender),
                receiver,
            });
        }

        let input_sender = links[0]
            .sender
            .as_ref()
            .expect("input sender exists")
            .clone();
        let output_receiver = links[stages.len()].receiver.clone();

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
                    dwell_hints: stage.dwell_hints,
                })
            })
            .collect::<Result<Vec<_>, E>>()?;

        let anchor_index = runtime_stages
            .iter()
            .position(|stage| stage.anchor.is_some())
            .expect("validated exactly one anchor");
        let anchor_stage = runtime_stages[anchor_index]
            .anchor
            .expect("validated exactly one anchor");

        let snapshot = Arc::new(RwLock::new(PiperSnapshot {
            links: (0..=runtime_stages.len())
                .map(|index| LinkSnapshot {
                    index,
                    len: 0,
                    state: WaterState::Starved,
                })
                .collect(),
            stages: runtime_stages
                .iter()
                .enumerate()
                .map(|(index, stage)| StageSnapshot {
                    index,
                    name: stage.name.clone(),
                    active_threads: 0,
                    processed_count: 0,
                    busy_ratio: 0.0,
                    busy_ceiling: 0.0,
                    effective_add_dwell: stage.dwell_hints.add.unwrap_or(config.add_dwell),
                    effective_remove_dwell: stage.dwell_hints.remove.unwrap_or(config.remove_dwell),
                    scaling_state: StageScalingState::Eligible,
                    is_anchor: stage.anchor.is_some(),
                })
                .collect(),
            anchor: AnchorSnapshot {
                stage_index: anchor_index,
                stage_name: runtime_stages[anchor_index].name.clone(),
                active_threads: 0,
                max_threads: anchor_stage.max_threads,
                probe_state: AnchorProbeState::WarmingUp,
                effective_probe_interval: anchor_stage.probe_interval,
                last_probe_outcome: AnchorProbeOutcome::None,
            },
            parked_threads: 0,
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
                shutdown: Arc::clone(&shutdown),
                _marker: PhantomData,
            },
            receiver: PiperReceiver {
                inner: output_receiver,
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
        "anchor_index".to_string(),
        "anchor_name".to_string(),
        "anchor_active_threads".to_string(),
        "anchor_max_threads".to_string(),
        "anchor_probe_state".to_string(),
        "anchor_effective_probe_interval_ms".to_string(),
        "anchor_last_probe_outcome".to_string(),
    ];

    for link in &snapshot.links {
        fields.push(format!("link{}_len", link.index));
        fields.push(format!("link{}_state", link.index));
    }

    for stage in &snapshot.stages {
        fields.push(format!("stage{}_name", stage.index));
        fields.push(format!("stage{}_active_threads", stage.index));
        fields.push(format!("stage{}_processed_count", stage.index));
        fields.push(format!("stage{}_busy_ratio", stage.index));
        fields.push(format!("stage{}_busy_ceiling", stage.index));
        fields.push(format!("stage{}_effective_add_dwell_ms", stage.index));
        fields.push(format!("stage{}_effective_remove_dwell_ms", stage.index));
        fields.push(format!("stage{}_scaling_state", stage.index));
        fields.push(format!("stage{}_is_anchor", stage.index));
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
        snapshot.anchor.stage_index.to_string(),
        csv_escape(&snapshot.anchor.stage_name),
        snapshot.anchor.active_threads.to_string(),
        snapshot.anchor.max_threads.to_string(),
        format!("{:?}", snapshot.anchor.probe_state),
        format!(
            "{:.3}",
            snapshot.anchor.effective_probe_interval.as_secs_f64() * 1000.0
        ),
        format!("{:?}", snapshot.anchor.last_probe_outcome),
    ];

    for link in &snapshot.links {
        fields.push(link.len.to_string());
        fields.push(format!("{:?}", link.state));
    }

    for stage in &snapshot.stages {
        fields.push(csv_escape(&stage.name));
        fields.push(stage.active_threads.to_string());
        fields.push(stage.processed_count.to_string());
        fields.push(format!("{:.6}", stage.busy_ratio));
        fields.push(format!("{:.6}", stage.busy_ceiling));
        fields.push(format!(
            "{:.3}",
            stage.effective_add_dwell.as_secs_f64() * 1000.0
        ));
        fields.push(format!(
            "{:.3}",
            stage.effective_remove_dwell.as_secs_f64() * 1000.0
        ));
        fields.push(format!("{:?}", stage.scaling_state));
        fields.push(stage.is_anchor.to_string());
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
    let mut pressure = vec![PressureTimer::default(); links.len()];
    let mut controls = build_stage_controls(&config, &stages);
    let anchor_index = stages
        .iter()
        .position(|stage| stage.anchor.is_some())
        .expect("validated exactly one anchor");
    let mut pending_scale = None;
    let mut last_scale_completed = Instant::now();
    let mut stored_failure = None;
    let mut supervisor_holds_links = true;

    for stage_index in 0..stages.len() {
        let active_count = if let Some(anchor) = stages[stage_index].anchor {
            anchor.initial_threads
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
                &shutdown,
                &abort,
                &internal_failure_sender,
                &mut workers,
                &mut active_by_stage,
            )?;
        }
    }

    for _ in 0..stages.len() {
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
        &pressure,
        &controls,
        anchor_index,
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
            &mut last_scale_completed,
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

        sample_pressure(&links, &config, &mut pressure);
        collect_stage_samples(&workers, &active_by_stage, &pressure, &mut controls);

        if !shutdown.load(Ordering::Acquire)
            && !abort.load(Ordering::Acquire)
            && stored_failure.is_none()
            && pending_scale.is_none()
            && last_scale_completed.elapsed() >= config.scale_cooldown
        {
            if let Some(operation) =
                choose_scale_operation(&pressure, &active_by_stage, &mut controls, anchor_index)
            {
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

                        while parked.len() < stages.len() {
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
            &pressure,
            &controls,
            anchor_index,
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
        &pressure,
        &controls,
        anchor_index,
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

#[allow(clippy::too_many_arguments)]
fn assign_worker<E>(
    worker_id: usize,
    stage_index: usize,
    stages: &[RuntimeStage<E>],
    links: &[Link],
    config: &PiperConfig,
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
    let output = links[stage_index + 1]
        .sender
        .as_ref()
        .ok_or_else(|| PiperError::Internal {
            worker: workers[worker_id].name.clone(),
            message: "cannot assign worker after output sender was dropped".to_string(),
        })?
        .clone();
    let assignment = WorkerAssignment {
        stage_index,
        stage: Arc::clone(&stages[stage_index].stage),
        input: links[stage_index].receiver.clone(),
        output,
        output_acquire: stages[stage_index].output_acquire.clone(),
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
            && stage_index == 0
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
                let ctx = RuntimeStageContext {
                    output: assignment.output.clone(),
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
    last_scale_completed: &mut Instant,
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
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Settling;
                        }
                        ScaleReason::AnchorProbe => {
                            if let Some(anchor) = controls[pending.stage_index].anchor.as_mut() {
                                anchor.probe = Some(AnchorProbe {
                                    worker_id,
                                    started_at: Instant::now(),
                                });
                            }
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Probing;
                        }
                        ScaleReason::AnchorRevert => {}
                    }
                    *last_scale_completed = Instant::now();
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
                        if pending.reason == ScaleReason::AnchorRevert {
                            anchor.probe = None;
                            anchor.last_probe_outcome = AnchorProbeOutcome::Reverted;
                            anchor.probe_interval = grow_duration(anchor.probe_interval);
                            anchor.next_probe_after = Instant::now() + anchor.probe_interval;
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::BackingOff;
                        } else {
                            controls[pending.stage_index].scaling_state =
                                StageScalingState::Eligible;
                        }
                    } else {
                        controls[pending.stage_index].scaling_state = StageScalingState::Eligible;
                    }
                    *last_scale_completed = Instant::now();
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
                    *last_scale_completed = Instant::now();
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
    let now = Instant::now();
    let control = &mut controls[stage_index];
    if let Some((last_direction, last_at)) = control.last_operation {
        if last_direction != direction
            && now.duration_since(last_at) <= control.effective_remove_dwell
        {
            control.effective_add_dwell = grow_duration(control.effective_add_dwell);
            control.effective_remove_dwell = grow_duration(control.effective_remove_dwell);
        }
    }
    control.last_operation = Some((direction, now));
}

fn grow_duration(duration: Duration) -> Duration {
    let grown = duration.saturating_mul(2);
    grown.min(Duration::from_secs(5))
}

fn decay_duration(current: Duration, base: Duration) -> Duration {
    if current <= base {
        return current;
    }
    let current_nanos = duration_nanos_u64(current);
    let base_nanos = duration_nanos_u64(base);
    let decayed = base_nanos + ((current_nanos - base_nanos) * 9 / 10);
    Duration::from_nanos(decayed.max(base_nanos))
}

fn sample_pressure(links: &[Link], config: &PiperConfig, pressure: &mut [PressureTimer]) {
    let now = Instant::now();
    for (index, link) in links.iter().enumerate() {
        let state = WaterState::classify(link.receiver.len(), config.low_water, config.high_water);
        let was_low = pressure[index].state.is_low_pressure();
        let is_low = state.is_low_pressure();

        if is_low && (!was_low || pressure[index].low_since.is_none()) {
            pressure[index].low_since = Some(now);
        } else if !is_low {
            pressure[index].low_since = None;
        }

        if state == WaterState::AboveHighWater
            && (pressure[index].state != WaterState::AboveHighWater
                || pressure[index].high_since.is_none())
        {
            pressure[index].high_since = Some(now);
        } else if state != WaterState::AboveHighWater {
            pressure[index].high_since = None;
        }

        pressure[index].state = state;
    }
}

fn collect_stage_samples<E>(
    workers: &[WorkerSlot<E>],
    active_by_stage: &[Vec<usize>],
    pressure: &[PressureTimer],
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

        let input_healthy = !pressure[stage_index].state.is_low_pressure();
        let output_unblocked = pressure.get(stage_index + 1).map_or(true, |pressure| {
            pressure.state != WaterState::AboveHighWater
        });
        if input_healthy && output_unblocked && total_nanos > 0 {
            update_busy_ceiling(control);
        }

        if control.settling && sample.processed_items > 0 {
            control.settling = false;
            control.scaling_state = StageScalingState::Eligible;
        }

        if !control.settling
            && !matches!(
                control.scaling_state,
                StageScalingState::Probing | StageScalingState::BackingOff
            )
        {
            control.effective_add_dwell =
                decay_duration(control.effective_add_dwell, control.base_add_dwell);
            control.effective_remove_dwell =
                decay_duration(control.effective_remove_dwell, control.base_remove_dwell);
        }

        if let Some(anchor) = control.anchor.as_mut() {
            if !anchor.warmup_complete
                && sample.processed_items > 0
                && input_healthy
                && output_unblocked
            {
                anchor.warmup_complete = true;
                control.scaling_state = StageScalingState::Eligible;
            }
        }
    }
}

fn update_busy_ceiling(control: &mut StageControl) {
    let busy = control.busy_ratio.clamp(0.0, 1.0);
    if busy > control.busy_ceiling {
        control.busy_ceiling = (control.busy_ceiling * 0.7) + (busy * 0.3);
    } else if control.busy_ceiling > 0.0 {
        control.busy_ceiling = (control.busy_ceiling * 0.995).max(busy);
    } else {
        control.busy_ceiling = busy;
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

fn choose_scale_operation(
    pressure: &[PressureTimer],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    anchor_index: usize,
) -> Option<ScaleOperation> {
    let now = Instant::now();

    if let Some(operation) = choose_anchor_probe_operation(pressure, controls, anchor_index, now) {
        return Some(operation);
    }

    if let Some(operation) =
        choose_support_operation(pressure, active_by_stage, controls, anchor_index, now)
    {
        return Some(operation);
    }

    choose_anchor_operation(pressure, active_by_stage, controls, anchor_index, now)
}

fn choose_anchor_probe_operation(
    pressure: &[PressureTimer],
    controls: &mut [StageControl],
    anchor_index: usize,
    now: Instant,
) -> Option<ScaleOperation> {
    let control = &mut controls[anchor_index];
    let Some(anchor) = control.anchor.as_mut() else {
        return None;
    };

    if let Some(probe) = anchor.probe.as_ref() {
        if now.duration_since(probe.started_at) < anchor.probe_window {
            return None;
        }

        let input_low = pressure[anchor_index].state.is_low_pressure();
        let output_high = pressure[anchor_index + 1].state == WaterState::AboveHighWater;
        let too_idle =
            control.busy_ceiling > 0.0 && control.busy_ratio < control.busy_ceiling * 0.55;

        if input_low || output_high || too_idle {
            return Some(ScaleOperation::Remove {
                stage_index: anchor_index,
                worker_id: Some(probe.worker_id),
                reason: ScaleReason::AnchorRevert,
            });
        }

        anchor.probe = None;
        anchor.last_probe_outcome = AnchorProbeOutcome::Kept;
        anchor.probe_interval = shrink_duration(anchor.probe_interval);
        anchor.next_probe_after = now + anchor.probe_interval;
        control.scaling_state = StageScalingState::Eligible;
    }

    None
}

fn choose_support_operation(
    pressure: &[PressureTimer],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    anchor_index: usize,
    now: Instant,
) -> Option<ScaleOperation> {
    for link_index in (1..=anchor_index).rev() {
        if low_elapsed(
            pressure[link_index],
            now,
            controls[link_index - 1].effective_add_dwell,
        ) {
            for stage_index in (0..link_index).rev() {
                if stage_index == anchor_index {
                    continue;
                }
                if support_can_add(stage_index, pressure, controls) {
                    return Some(ScaleOperation::Add {
                        stage_index,
                        reason: ScaleReason::Support,
                    });
                }
            }
        }
    }

    for stage_index in anchor_index + 1..active_by_stage.len() {
        if high_elapsed(
            pressure[stage_index],
            now,
            controls[stage_index].effective_add_dwell,
        ) && support_can_add(stage_index, pressure, controls)
        {
            return Some(ScaleOperation::Add {
                stage_index,
                reason: ScaleReason::Support,
            });
        }
    }

    for stage_index in 0..active_by_stage.len() {
        if stage_index == anchor_index {
            continue;
        }

        let remove_pressure = if stage_index < anchor_index {
            high_elapsed(
                pressure[stage_index + 1],
                now,
                controls[stage_index].effective_remove_dwell,
            ) && !pressure[stage_index].state.is_low_pressure()
        } else {
            low_elapsed(
                pressure[stage_index],
                now,
                controls[stage_index].effective_remove_dwell,
            )
        };

        if remove_pressure && active_by_stage[stage_index].len() > 1 {
            return Some(ScaleOperation::Remove {
                stage_index,
                worker_id: None,
                reason: ScaleReason::Support,
            });
        }
    }

    None
}

fn choose_anchor_operation(
    pressure: &[PressureTimer],
    active_by_stage: &[Vec<usize>],
    controls: &mut [StageControl],
    anchor_index: usize,
    now: Instant,
) -> Option<ScaleOperation> {
    let control = &mut controls[anchor_index];
    let Some(anchor) = control.anchor.as_mut() else {
        return None;
    };

    if anchor.probe.is_some() {
        return None;
    }

    if now < anchor.next_probe_after {
        control.scaling_state = StageScalingState::BackingOff;
        return None;
    }

    let active = active_by_stage[anchor_index].len();
    if active > 1
        && (low_elapsed(pressure[anchor_index], now, anchor.remove_dwell)
            || high_elapsed(pressure[anchor_index + 1], now, anchor.remove_dwell))
    {
        return Some(ScaleOperation::Remove {
            stage_index: anchor_index,
            worker_id: None,
            reason: ScaleReason::Support,
        });
    }

    if active >= anchor.max_threads {
        control.scaling_state = StageScalingState::Eligible;
        return None;
    }

    if !anchor.warmup_complete {
        control.scaling_state = StageScalingState::BackingOff;
        return None;
    }

    let input_ready = !pressure[anchor_index].state.is_low_pressure();
    let downstream_ready = pressure[anchor_index + 1].state != WaterState::AboveHighWater;
    let enough_busy_signal = control.busy_ceiling <= 0.0
        || control.busy_ratio >= (control.busy_ceiling * 0.8)
        || pressure[anchor_index].state == WaterState::AboveHighWater;

    if input_ready && downstream_ready && enough_busy_signal {
        return Some(ScaleOperation::Add {
            stage_index: anchor_index,
            reason: ScaleReason::AnchorProbe,
        });
    }

    control.scaling_state = StageScalingState::Eligible;
    None
}

fn support_can_add(
    stage_index: usize,
    pressure: &[PressureTimer],
    controls: &[StageControl],
) -> bool {
    if controls[stage_index].settling
        || matches!(
            controls[stage_index].scaling_state,
            StageScalingState::Settling | StageScalingState::Probing
        )
    {
        return false;
    }
    !pressure[stage_index].state.is_low_pressure()
}

fn shrink_duration(duration: Duration) -> Duration {
    let nanos = duration_nanos_u64(duration);
    Duration::from_nanos((nanos * 3 / 4).max(10_000_000))
}

fn low_elapsed(pressure: PressureTimer, now: Instant, dwell: Duration) -> bool {
    pressure
        .low_since
        .is_some_and(|since| now.duration_since(since) >= dwell)
}

fn high_elapsed(pressure: PressureTimer, now: Instant, dwell: Duration) -> bool {
    pressure
        .high_since
        .is_some_and(|since| now.duration_since(since) >= dwell)
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
    } else if Instant::now() < anchor.next_probe_after {
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
    pressure: &[PressureTimer],
    controls: &[StageControl],
    anchor_index: usize,
    shutdown_requested: bool,
    abort_requested: bool,
    pending_scale_operation: bool,
) where
    E: Debug + Display + Send + 'static,
{
    let mut snapshot = snapshot.write();
    snapshot.links = links
        .iter()
        .enumerate()
        .map(|(index, link)| LinkSnapshot {
            index,
            len: link.receiver.len(),
            state: pressure[index].state,
        })
        .collect();
    snapshot.stages = stages
        .iter()
        .enumerate()
        .map(|(index, stage)| StageSnapshot {
            index,
            name: stage.name.clone(),
            active_threads: active_by_stage[index].len(),
            processed_count: controls[index].processed_count,
            busy_ratio: controls[index].busy_ratio,
            busy_ceiling: controls[index].busy_ceiling,
            effective_add_dwell: controls[index].effective_add_dwell,
            effective_remove_dwell: controls[index].effective_remove_dwell,
            scaling_state: controls[index].scaling_state,
            is_anchor: controls[index].anchor.is_some(),
        })
        .collect();
    if let Some(anchor) = controls[anchor_index].anchor.as_ref() {
        snapshot.anchor = AnchorSnapshot {
            stage_index: anchor_index,
            stage_name: stages[anchor_index].name.clone(),
            active_threads: active_by_stage[anchor_index].len(),
            max_threads: anchor.max_threads,
            probe_state: anchor_probe_state(anchor, active_by_stage[anchor_index].len()),
            effective_probe_interval: anchor.probe_interval,
            last_probe_outcome: anchor.last_probe_outcome,
        };
    }
    snapshot.parked_threads = parked_threads;
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

    fn test_config() -> PiperConfig {
        PiperConfig {
            sample_interval: Duration::from_millis(1),
            poll_interval: Duration::from_millis(1),
            scale_cooldown: Duration::ZERO,
            add_dwell: Duration::from_millis(5),
            remove_dwell: Duration::from_millis(5),
            low_water: 2,
            high_water: 4,
            csv_telemetry: None,
        }
    }

    fn support_control() -> StageControl {
        StageControl {
            base_add_dwell: Duration::from_millis(5),
            base_remove_dwell: Duration::from_millis(5),
            effective_add_dwell: Duration::from_millis(5),
            effective_remove_dwell: Duration::from_millis(5),
            processed_count: 0,
            busy_ratio: 0.0,
            busy_ceiling: 0.0,
            last_sample_processed: 0,
            scaling_state: StageScalingState::Eligible,
            settling: false,
            last_operation: None,
            anchor: None,
        }
    }

    fn anchor_control(max_threads: usize) -> StageControl {
        StageControl {
            anchor: Some(AnchorControl {
                max_threads,
                probe_interval: Duration::from_millis(10),
                probe_window: Duration::from_millis(10),
                remove_dwell: Duration::from_millis(5),
                warmup_complete: true,
                probe: None,
                next_probe_after: Instant::now() - Duration::from_millis(1),
                last_probe_outcome: AnchorProbeOutcome::None,
            }),
            busy_ratio: 0.8,
            busy_ceiling: 0.8,
            ..support_control()
        }
    }

    #[test]
    fn water_state_classification_keeps_starved_and_low_distinct() {
        assert_eq!(WaterState::classify(0, 2, 4), WaterState::Starved);
        assert_eq!(WaterState::classify(1, 2, 4), WaterState::BelowLowWater);
        assert_eq!(WaterState::classify(2, 2, 4), WaterState::Nominal);
        assert_eq!(WaterState::classify(5, 2, 4), WaterState::AboveHighWater);
        assert!(WaterState::Starved.is_low_pressure());
        assert!(WaterState::BelowLowWater.is_low_pressure());
    }

    #[test]
    fn starved_and_below_low_share_the_low_dwell_timer() {
        let config = test_config();
        let (sender, receiver) = kanal::unbounded::<Message>();
        let links = vec![Link {
            sender: Some(sender.clone()),
            receiver,
        }];
        let mut pressure = vec![PressureTimer::default()];

        sample_pressure(&links, &config, &mut pressure);
        let low_started = pressure[0].low_since.expect("starved starts low timer");

        sender.send(Box::new(1_u8)).unwrap();
        sample_pressure(&links, &config, &mut pressure);

        assert_eq!(pressure[0].state, WaterState::BelowLowWater);
        assert_eq!(pressure[0].low_since, Some(low_started));
    }

    #[test]
    fn support_add_skips_starved_upstream_stages() {
        let config = test_config();
        let elapsed = Instant::now() - config.add_dwell - Duration::from_millis(1);
        let active = vec![vec![0], vec![1], vec![2]];
        let mut pressure = vec![
            PressureTimer {
                state: WaterState::Starved,
                low_since: Some(elapsed),
                high_since: None,
            },
            PressureTimer {
                state: WaterState::Starved,
                low_since: Some(elapsed),
                high_since: None,
            },
            PressureTimer {
                state: WaterState::BelowLowWater,
                low_since: Some(elapsed),
                high_since: None,
            },
            PressureTimer::default(),
        ];
        let mut controls = vec![support_control(), support_control(), anchor_control(1)];

        assert!(choose_scale_operation(&pressure, &active, &mut controls, 2).is_none());

        pressure[0].state = WaterState::Nominal;
        pressure[0].low_since = None;
        assert!(matches!(
            choose_scale_operation(&pressure, &active, &mut controls, 2),
            Some(ScaleOperation::Add {
                stage_index: 0,
                reason: ScaleReason::Support
            })
        ));
    }

    #[test]
    fn post_anchor_scaling_uses_local_input_pressure() {
        let config = test_config();
        let elapsed = Instant::now() - config.add_dwell - Duration::from_millis(1);
        let active = vec![vec![0], vec![1]];
        let pressure = vec![
            PressureTimer {
                state: WaterState::Nominal,
                low_since: None,
                high_since: None,
            },
            PressureTimer {
                state: WaterState::AboveHighWater,
                low_since: None,
                high_since: Some(elapsed),
            },
            PressureTimer::default(),
        ];
        let mut controls = vec![anchor_control(1), support_control()];

        assert!(matches!(
            choose_scale_operation(&pressure, &active, &mut controls, 0),
            Some(ScaleOperation::Add {
                stage_index: 1,
                reason: ScaleReason::Support
            })
        ));

        let active = vec![vec![0], vec![1, 2]];
        let elapsed = Instant::now() - config.remove_dwell - Duration::from_millis(1);
        let pressure = vec![
            PressureTimer {
                state: WaterState::Nominal,
                low_since: None,
                high_since: None,
            },
            PressureTimer {
                state: WaterState::BelowLowWater,
                low_since: Some(elapsed),
                high_since: None,
            },
            PressureTimer::default(),
        ];
        let mut controls = vec![anchor_control(1), support_control()];

        assert!(matches!(
            choose_scale_operation(&pressure, &active, &mut controls, 0),
            Some(ScaleOperation::Remove {
                stage_index: 1,
                reason: ScaleReason::Support,
                ..
            })
        ));
    }

    #[test]
    fn anchor_scales_up_after_warmup_when_busy_and_fed() {
        let active = vec![vec![0]];
        let pressure = vec![
            PressureTimer {
                state: WaterState::Nominal,
                low_since: None,
                high_since: None,
            },
            PressureTimer {
                state: WaterState::Nominal,
                low_since: None,
                high_since: None,
            },
        ];
        let mut controls = vec![anchor_control(2)];

        assert!(matches!(
            choose_scale_operation(&pressure, &active, &mut controls, 0),
            Some(ScaleOperation::Add {
                stage_index: 0,
                reason: ScaleReason::AnchorProbe
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
        let piper = Piper::<u8, u8, TestError>::start(
            config,
            vec![
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
                .max_threads(1)
                .into_stage_spec(),
            ],
        )
        .unwrap();

        piper.shutdown();
        let error = piper.join().expect_err("cleanup failure should fail join");
        assert!(matches!(error, PiperError::UserCleanup { .. }));
    }
}
