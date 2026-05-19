use parking_lot::RwLock;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

    #[error("compute_stage index {compute_stage} is out of range for {stages} stages")]
    InvalidComputeStage { compute_stage: usize, stages: usize },

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
    pub compute_stage: usize,
    pub compute_threads: usize,
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
            compute_stage: 0,
            compute_threads: 1,
        }
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
}

#[derive(Clone, Debug)]
pub struct PiperSnapshot {
    pub links: Vec<LinkSnapshot>,
    pub stages: Vec<StageSnapshot>,
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
            .send(InternalFailure {
                message: message.into(),
            })
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
            let _ = self.internal_failure.send(InternalFailure {
                message: "stage output channel closed unexpectedly".to_string(),
            });
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

pub trait Stage<In, Out, E>: Send + Sync + 'static
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type State: Send + 'static;

    fn init(&self) -> std::result::Result<Self::State, E>;

    fn process(
        &self,
        state: &mut Self::State,
        input: In,
        ctx: &mut StageContext<Out, E>,
    ) -> std::result::Result<(), E>;

    fn cleanup(&self, _state: Self::State) -> std::result::Result<(), E> {
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

struct StageAdapter<S, In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
    S: Stage<In, Out, E>,
{
    stage: S,
    _marker: PhantomData<fn(In, Out, E)>,
}

impl<S, In, Out, E> DynStage<E> for StageAdapter<S, In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
    S: Stage<In, Out, E>,
{
    fn init_box(&self) -> std::result::Result<Box<dyn Any + Send>, StageFailure<E>> {
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
    ) -> std::result::Result<(), StageFailure<E>> {
        let state = state
            .downcast_mut::<S::State>()
            .ok_or_else(|| StageFailure::Internal("stage state type mismatch".to_string()))?;
        let input = input
            .downcast::<In>()
            .map(|input| *input)
            .map_err(|_| StageFailure::Internal("stage input type mismatch".to_string()))?;
        let output_acquire = match ctx.output_acquire {
            Some(acquire) => Some(
                Arc::downcast::<Arc<AcquireFn<Out>>>(acquire)
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

    fn cleanup_box(&self, state: Box<dyn Any + Send>) -> std::result::Result<(), StageFailure<E>> {
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

impl<Init, Process, Cleanup, State, In, Out, E> Stage<In, Out, E>
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
    type State = State;

    fn init(&self) -> std::result::Result<Self::State, E> {
        (self.init)()
    }

    fn process(
        &self,
        state: &mut Self::State,
        input: In,
        ctx: &mut StageContext<Out, E>,
    ) -> std::result::Result<(), E> {
        (self.process)(state, input, ctx)
    }

    fn cleanup(&self, state: Self::State) -> std::result::Result<(), E> {
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
}

pub fn trait_stage<S, In, Out, E>(name: impl Into<String>, stage: S) -> StageSpec<E>
where
    S: Stage<In, Out, E>,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    StageSpec {
        name: name.into(),
        stage: Arc::new(StageAdapter {
            stage,
            _marker: PhantomData::<fn(In, Out, E)>,
        }),
        output_acquire_builder: None,
    }
}

pub fn inline_stage<Init, Process, Cleanup, State, In, Out, E>(
    name: impl Into<String>,
    init: Init,
    process: Process,
    cleanup: Cleanup,
) -> StageSpec<E>
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
    trait_stage(
        name,
        InlineStage {
            init,
            process,
            cleanup,
            _marker: PhantomData::<fn(State, In, Out, E)>,
        },
    )
}

pub fn inline_stage_no_cleanup<Init, Process, State, In, Out, E>(
    name: impl Into<String>,
    init: Init,
    process: Process,
) -> StageSpec<E>
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
    inline_stage(name, init, process, |_state| Ok(()))
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
struct InternalFailure {
    message: String,
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
}

enum PendingScale {
    Add { worker_id: usize },
    Remove { worker_id: usize },
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
        if config.compute_threads == 0 {
            return Err(PiperError::ZeroWorkers);
        }
        if config.compute_stage >= stages.len() {
            return Err(PiperError::InvalidComputeStage {
                compute_stage: config.compute_stage,
                stages: stages.len(),
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
            .map(|stage| RuntimeStage {
                name: stage.name,
                stage: stage.stage,
                output_acquire: stage
                    .output_acquire_builder
                    .map(|builder| builder.build(lease_runtime.clone())),
            })
            .collect();

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
                })
                .collect(),
            parked_threads: 0,
            shutdown_requested: false,
            abort_requested: false,
            pending_scale_operation: false,
        }));

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

    pub fn snapshot(&self) -> PiperSnapshot {
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

fn run_supervisor<E>(
    config: PiperConfig,
    stages: Vec<RuntimeStage<E>>,
    mut links: Vec<Link>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    internal_failure_sender: kanal::Sender<InternalFailure>,
    internal_failure_receiver: kanal::Receiver<InternalFailure>,
) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    let (worker_event_sender, worker_event_receiver) = kanal::unbounded::<WorkerEvent<E>>();
    let mut workers = Vec::new();
    let mut active_by_stage = vec![Vec::<usize>::new(); stages.len()];
    let mut parked = Vec::new();
    let mut pressure = vec![PressureTimer::default(); links.len()];
    let mut pending_scale = None;
    let mut last_scale_completed = Instant::now();
    let mut stored_failure = None;
    let mut supervisor_holds_links = true;

    for stage_index in 0..stages.len() {
        let active_count = if stage_index == config.compute_stage {
            config.compute_threads
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
            &mut pending_scale,
            &mut last_scale_completed,
            &abort,
            &mut stored_failure,
        );

        while let Ok(Some(failure)) = internal_failure_receiver.try_recv() {
            stored_failure = Some(PiperError::Internal {
                worker: "piper-supervisor".to_string(),
                message: failure.message,
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

        if !shutdown.load(Ordering::Acquire)
            && !abort.load(Ordering::Acquire)
            && stored_failure.is_none()
            && pending_scale.is_none()
            && last_scale_completed.elapsed() >= config.scale_cooldown
        {
            if let Some(operation) = choose_scale_operation(&config, &pressure, &active_by_stage) {
                match operation {
                    ScaleOperation::Add(stage_index) => {
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
                        pending_scale = Some(PendingScale::Add { worker_id });

                        while parked.len() < stages.len() {
                            let name = format!("piper-worker-{}", workers.len());
                            let worker_id =
                                spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
                            parked.push(worker_id);
                        }
                    }
                    ScaleOperation::Remove(stage_index) => {
                        if let Some(worker_id) = active_by_stage[stage_index].first().copied() {
                            if let Some(retire) = workers[worker_id].retire.as_ref() {
                                retire.store(true, Ordering::Release);
                                pending_scale = Some(PendingScale::Remove { worker_id });
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
        shutdown.load(Ordering::Acquire),
        abort.load(Ordering::Acquire),
        false,
    );

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

        match assignment.input.recv_timeout(assignment.poll_interval) {
            Ok(input) => {
                let ctx = RuntimeStageContext {
                    output: assignment.output.clone(),
                    output_acquire: assignment.output_acquire.clone(),
                    shutdown: Arc::clone(&assignment.shutdown),
                    abort: Arc::clone(&assignment.abort),
                    internal_failure: assignment.internal_failure.clone(),
                };
                if let Err(failure) = assignment.stage.process_box(state.as_mut(), input, ctx) {
                    let _ = event_sender.send(WorkerEvent::Failed {
                        worker_id,
                        stage_index,
                        worker: worker_name.to_string(),
                        failure,
                    });
                    return;
                }
            }
            Err(kanal::ReceiveErrorTimeout::Timeout) => {
                if assignment.input.is_terminated() {
                    break;
                }
            }
            Err(kanal::ReceiveErrorTimeout::Closed)
            | Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
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
                if matches!(pending_scale, Some(PendingScale::Add { worker_id: id }) if *id == worker_id)
                {
                    *pending_scale = None;
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
                if matches!(pending_scale, Some(PendingScale::Remove { worker_id: id }) if *id == worker_id)
                {
                    *pending_scale = None;
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
                if matches!(pending_scale, Some(PendingScale::Add { worker_id: id } | PendingScale::Remove { worker_id: id }) if *id == worker_id)
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

enum ScaleOperation {
    Add(usize),
    Remove(usize),
}

fn choose_scale_operation(
    config: &PiperConfig,
    pressure: &[PressureTimer],
    active_by_stage: &[Vec<usize>],
) -> Option<ScaleOperation> {
    let now = Instant::now();
    let compute = config.compute_stage;

    for link_index in (1..=compute).rev() {
        if low_elapsed(pressure[link_index], now, config.add_dwell) {
            for stage_index in (0..link_index).rev() {
                if !pressure[stage_index].state.is_low_pressure() {
                    return Some(ScaleOperation::Add(stage_index));
                }
            }
        }
    }

    for link_index in 1..=compute {
        let stage_index = link_index - 1;
        if high_elapsed(pressure[link_index], now, config.remove_dwell)
            && !pressure[stage_index].state.is_low_pressure()
            && active_by_stage[stage_index].len() > 1
        {
            return Some(ScaleOperation::Remove(stage_index));
        }
    }

    for stage_index in compute + 1..active_by_stage.len() {
        if high_elapsed(pressure[stage_index], now, config.add_dwell) {
            return Some(ScaleOperation::Add(stage_index));
        }
        if low_elapsed(pressure[stage_index], now, config.remove_dwell)
            && active_by_stage[stage_index].len() > 1
        {
            return Some(ScaleOperation::Remove(stage_index));
        }
    }

    None
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

#[allow(clippy::too_many_arguments)]
fn update_snapshot<E>(
    snapshot: &Arc<RwLock<PiperSnapshot>>,
    links: &[Link],
    stages: &[RuntimeStage<E>],
    active_by_stage: &[Vec<usize>],
    parked_threads: usize,
    pressure: &[PressureTimer],
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
        })
        .collect();
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
            compute_stage: 1,
            compute_threads: 1,
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
    fn pre_compute_add_skips_starved_upstream_stages() {
        let mut config = test_config();
        config.compute_stage = 2;
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

        assert!(choose_scale_operation(&config, &pressure, &active).is_none());

        pressure[0].state = WaterState::Nominal;
        pressure[0].low_since = None;
        assert!(matches!(
            choose_scale_operation(&config, &pressure, &active),
            Some(ScaleOperation::Add(0))
        ));
    }

    #[test]
    fn post_compute_scaling_uses_local_input_pressure() {
        let mut config = test_config();
        config.compute_stage = 0;
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

        assert!(matches!(
            choose_scale_operation(&config, &pressure, &active),
            Some(ScaleOperation::Add(1))
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

        assert!(matches!(
            choose_scale_operation(&config, &pressure, &active),
            Some(ScaleOperation::Remove(1))
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
        assert!(failure_receiver.recv().unwrap().message.contains("recycle"));
    }

    #[test]
    fn cleanup_failure_is_reported_from_join() {
        let config = PiperConfig {
            compute_stage: 0,
            ..test_config()
        };
        let piper = Piper::<u8, u8, TestError>::start(
            config,
            vec![inline_stage(
                "cleanup",
                || -> std::result::Result<(), TestError> { Ok(()) },
                |_state: &mut (), input: u8, ctx: &mut StageContext<u8, TestError>| {
                    ctx.emit(input);
                    Ok(())
                },
                |_state| Err(TestError::Boom),
            )],
        )
        .unwrap();

        piper.shutdown();
        let error = piper.join().expect_err("cleanup failure should fail join");
        assert!(matches!(error, PiperError::UserCleanup { .. }));
    }
}
