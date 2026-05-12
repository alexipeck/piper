use parking_lot::RwLock;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub use kanal;
pub use parking_lot;

pub struct Config {
    pub num_workers: usize,
    pub poll_interval: Duration,
    pub cancel: Arc<RwLock<bool>>,
}

pub struct Accumulator<Msg, Output> {
    sender: kanal::Sender<Msg>,
    workers: Vec<JoinHandle<Output>>,
}

impl<Msg, Output> Accumulator<Msg, Output>
where
    Msg: Send + 'static,
    Output: Send + 'static,
{
    pub fn new<Storage, Init, Handle, Finalize>(
        config: Config,
        init: Init,
        handle: Handle,
        finalize: Finalize,
    ) -> Self
    where
        Storage: Send + 'static,
        Init: Fn() -> Storage + Send + Sync + 'static,
        Handle: Fn(&mut Storage, Msg) + Send + Sync + 'static,
        Finalize: Fn(Storage) -> Output + Send + Sync + 'static,
    {
        let (sender, receiver) = kanal::unbounded::<Msg>();
        let init = Arc::new(init);
        let handle = Arc::new(handle);
        let finalize = Arc::new(finalize);
        let mut workers = Vec::with_capacity(config.num_workers);

        for worker_index in 0..config.num_workers {
            let receiver = receiver.clone();
            let cancel = Arc::clone(&config.cancel);
            let poll_interval = config.poll_interval;
            let init = Arc::clone(&init);
            let handle = Arc::clone(&handle);
            let finalize = Arc::clone(&finalize);

            let worker = thread::Builder::new()
                .name(format!("accumulator-worker-{worker_index}"))
                .spawn(move || {
                    let mut storage = init();
                    loop {
                        if *cancel.read() {
                            break;
                        }
                        match receiver.recv_timeout(poll_interval) {
                            Ok(msg) => handle(&mut storage, msg),
                            Err(kanal::ReceiveErrorTimeout::Timeout) => continue,
                            Err(kanal::ReceiveErrorTimeout::Closed)
                            | Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
                        }
                    }
                    finalize(storage)
                })
                .expect("failed to spawn accumulator worker thread");

            workers.push(worker);
        }

        drop(receiver);

        Accumulator { sender, workers }
    }

    pub fn sender(&self) -> kanal::Sender<Msg> {
        self.sender.clone()
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn join(self) -> Vec<Output> {
        drop(self.sender);
        let mut results = Vec::with_capacity(self.workers.len());
        for worker in self.workers {
            results.push(
                worker
                    .join()
                    .expect("accumulator worker thread panicked"),
            );
        }
        results
    }
}
