use parking_lot::RwLock;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use thiserror::Error;

pub use kanal;
pub use parking_lot;

pub type Result<T, E = String> = std::result::Result<T, AccumulatorError<E>>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AccumulatorError<E: Debug + Display = String> {
    #[error("Config::num_workers must be greater than 0")]
    ZeroWorkers,

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

    #[error("handle closure failed in worker `{worker}`: {error}")]
    UserHandle { worker: String, error: E },

    #[error("finalize closure failed in worker `{worker}`: {error}")]
    UserFinalize { worker: String, error: E },
}

pub struct Config {
    pub num_workers: usize,
    pub poll_interval: Duration,
    pub cancel: Arc<RwLock<bool>>,
}

pub struct Accumulator<Msg, Output, UserErr = String>
where
    UserErr: Debug + Display + Send + 'static,
{
    sender: kanal::Sender<Msg>,
    workers: Vec<(
        String,
        JoinHandle<std::result::Result<Output, AccumulatorError<UserErr>>>,
    )>,
}

impl<Msg, Output, UserErr> Accumulator<Msg, Output, UserErr>
where
    Msg: Send + 'static,
    Output: Send + 'static,
    UserErr: Debug + Display + Send + 'static,
{
    pub fn new<Storage, Init, Handle, Finalize>(
        config: Config,
        init: Init,
        handle: Handle,
        finalize: Finalize,
    ) -> Result<Self, UserErr>
    where
        Storage: Send + 'static,
        Init: Fn() -> std::result::Result<Storage, UserErr> + Send + Sync + 'static,
        Handle: Fn(&mut Storage, Msg) -> std::result::Result<(), UserErr> + Send + Sync + 'static,
        Finalize: Fn(Storage) -> std::result::Result<Output, UserErr> + Send + Sync + 'static,
    {
        if config.num_workers == 0 {
            return Err(AccumulatorError::ZeroWorkers);
        }

        let (sender, receiver) = kanal::unbounded::<Msg>();
        let init = Arc::new(init);
        let handle = Arc::new(handle);
        let finalize = Arc::new(finalize);
        let mut workers = Vec::with_capacity(config.num_workers);

        for worker_index in 0..config.num_workers {
            let name = format!("accumulator-worker-{worker_index}");
            let receiver = receiver.clone();
            let cancel = Arc::clone(&config.cancel);
            let poll_interval = config.poll_interval;
            let init = Arc::clone(&init);
            let handle = Arc::clone(&handle);
            let finalize = Arc::clone(&finalize);
            let thread_name = name.clone();

            let worker = thread::Builder::new()
                .name(name.clone())
                .spawn(
                    move || -> std::result::Result<Output, AccumulatorError<UserErr>> {
                        let mut storage = init().map_err(|error| AccumulatorError::UserInit {
                            worker: thread_name.clone(),
                            error,
                        })?;
                        loop {
                            if *cancel.read() {
                                break;
                            }
                            match receiver.recv_timeout(poll_interval) {
                                Ok(msg) => handle(&mut storage, msg).map_err(|error| {
                                    AccumulatorError::UserHandle {
                                        worker: thread_name.clone(),
                                        error,
                                    }
                                })?,
                                Err(kanal::ReceiveErrorTimeout::Timeout) => continue,
                                Err(kanal::ReceiveErrorTimeout::Closed)
                                | Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
                            }
                        }
                        finalize(storage).map_err(|error| AccumulatorError::UserFinalize {
                            worker: thread_name.clone(),
                            error,
                        })
                    },
                )
                .map_err(|source| AccumulatorError::SpawnFailed {
                    worker: name.clone(),
                    source,
                })?;

            workers.push((name, worker));
        }

        drop(receiver);

        Ok(Accumulator { sender, workers })
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
            let inner_result =
                worker
                    .join()
                    .map_err(|payload| AccumulatorError::WorkerPanicked {
                        worker: name.clone(),
                        message: panic_payload_to_string(payload),
                    })?;
            let output = inner_result?;
            results.push(output);
        }
        Ok(results)
    }
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
