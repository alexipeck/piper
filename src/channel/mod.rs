use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
    Timeout,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError;

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("channel send failed")
    }
}

impl std::error::Error for SendError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("channel receive failed")
    }
}

impl std::error::Error for RecvError {}

#[derive(Clone)]
struct EndpointCounts {
    senders: Arc<AtomicUsize>,
    receivers: Arc<AtomicUsize>,
}

impl EndpointCounts {
    fn new() -> Self {
        Self {
            senders: Arc::new(AtomicUsize::new(1)),
            receivers: Arc::new(AtomicUsize::new(1)),
        }
    }

    fn sender_clone(&self) -> Self {
        self.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            senders: Arc::clone(&self.senders),
            receivers: Arc::clone(&self.receivers),
        }
    }

    fn receiver_clone(&self) -> Self {
        self.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            senders: Arc::clone(&self.senders),
            receivers: Arc::clone(&self.receivers),
        }
    }

    fn shared(&self) -> Self {
        Self {
            senders: Arc::clone(&self.senders),
            receivers: Arc::clone(&self.receivers),
        }
    }

    fn drop_sender(&self) {
        self.senders.fetch_sub(1, Ordering::Relaxed);
    }

    fn drop_receiver(&self) {
        self.receivers.fetch_sub(1, Ordering::Relaxed);
    }

    fn senders(&self) -> usize {
        self.senders.load(Ordering::Relaxed)
    }

    fn receivers(&self) -> usize {
        self.receivers.load(Ordering::Relaxed)
    }

    fn is_terminated(&self) -> bool {
        self.senders() == 0
    }

    fn is_closed(&self) -> bool {
        self.senders() == 0 && self.receivers() == 0
    }
}

#[cfg(feature = "channel-kanal")]
mod backend {
    use super::*;
    use kanal::{ReceiveError, ReceiveErrorTimeout};

    pub struct Sender<T> {
        inner: kanal::Sender<T>,
        endpoints: EndpointCounts,
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                endpoints: self.endpoints.sender_clone(),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.endpoints.drop_sender();
        }
    }

    impl<T> Sender<T> {
        pub fn send(&self, value: T) -> Result<(), SendError> {
            self.inner.send(value).map_err(|_| SendError)
        }

        pub fn is_closed(&self) -> bool {
            self.endpoints.receivers() == 0
                || self.endpoints.is_terminated()
                || self.inner.is_closed()
        }
    }

    pub struct Receiver<T> {
        inner: kanal::Receiver<T>,
        endpoints: EndpointCounts,
    }

    impl<T> Clone for Receiver<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                endpoints: self.endpoints.receiver_clone(),
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.endpoints.drop_receiver();
        }
    }

    impl<T> Receiver<T> {
        pub fn recv(&self) -> Result<T, RecvError> {
            self.inner.recv().map_err(|_| RecvError)
        }

        pub fn try_recv(&self) -> Result<T, TryRecvError> {
            match self.inner.try_recv() {
                Ok(Some(value)) => Ok(value),
                Ok(None) => {
                    if self.endpoints.is_terminated() && self.inner.is_empty() {
                        Err(TryRecvError::Closed)
                    } else {
                        Err(TryRecvError::Empty)
                    }
                }
                Err(ReceiveError::Closed) | Err(ReceiveError::SendClosed) => Err(TryRecvError::Closed),
            }
        }

        pub fn recv_timeout(&self, duration: Duration) -> Result<T, RecvTimeoutError> {
            if self.is_terminated() && self.is_empty() {
                return Err(RecvTimeoutError::Closed);
            }
            match self.inner.recv_timeout(duration) {
                Ok(value) => Ok(value),
                Err(ReceiveErrorTimeout::Timeout) => {
                    if self.is_terminated() && self.is_empty() {
                        Err(RecvTimeoutError::Closed)
                    } else {
                        Err(RecvTimeoutError::Timeout)
                    }
                }
                Err(ReceiveErrorTimeout::Closed) | Err(ReceiveErrorTimeout::SendClosed) => {
                    Err(RecvTimeoutError::Closed)
                }
            }
        }

        pub fn len(&self) -> usize {
            self.inner.len()
        }

        pub fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        pub fn is_terminated(&self) -> bool {
            self.endpoints.is_terminated() || self.inner.is_terminated()
        }

        pub fn is_closed(&self) -> bool {
            self.endpoints.is_closed()
                || (self.endpoints.is_terminated() && self.inner.is_empty())
        }
    }

    pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let (inner_tx, inner_rx) = kanal::unbounded::<T>();
        let endpoints = EndpointCounts::new();
        (
            Sender {
                inner: inner_tx,
                endpoints: endpoints.shared(),
            },
            Receiver {
                inner: inner_rx,
                endpoints,
            },
        )
    }
}

#[cfg(all(feature = "channel-crossbeam", not(feature = "channel-kanal")))]
mod backend {
    use super::*;
    use crossbeam_channel::{self, RecvTimeoutError as CrossbeamRecvTimeoutError};

    pub struct Sender<T> {
        inner: crossbeam_channel::Sender<T>,
        endpoints: EndpointCounts,
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                endpoints: self.endpoints.sender_clone(),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.endpoints.drop_sender();
        }
    }

    impl<T> Sender<T> {
        pub fn send(&self, value: T) -> Result<(), SendError> {
            self.inner.send(value).map_err(|_| SendError)
        }

        pub fn is_closed(&self) -> bool {
            self.endpoints.receivers() == 0 || self.endpoints.is_terminated()
        }
    }

    pub struct Receiver<T> {
        inner: crossbeam_channel::Receiver<T>,
        endpoints: EndpointCounts,
    }

    impl<T> Clone for Receiver<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                endpoints: self.endpoints.receiver_clone(),
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.endpoints.drop_receiver();
        }
    }

    impl<T> Receiver<T> {
        pub fn recv(&self) -> Result<T, RecvError> {
            self.inner.recv().map_err(|_| RecvError)
        }

        pub fn try_recv(&self) -> Result<T, TryRecvError> {
            match self.inner.try_recv() {
                Ok(value) => Ok(value),
                Err(crossbeam_channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
                Err(crossbeam_channel::TryRecvError::Disconnected) => Err(TryRecvError::Closed),
            }
        }

        pub fn recv_timeout(&self, duration: Duration) -> Result<T, RecvTimeoutError> {
            match self.inner.recv_timeout(duration) {
                Ok(value) => Ok(value),
                Err(CrossbeamRecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
                Err(CrossbeamRecvTimeoutError::Disconnected) => Err(RecvTimeoutError::Closed),
            }
        }

        pub fn len(&self) -> usize {
            self.inner.len()
        }

        pub fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        pub fn is_terminated(&self) -> bool {
            self.endpoints.is_terminated()
        }

        pub fn is_closed(&self) -> bool {
            self.endpoints.is_closed()
                || (self.endpoints.is_terminated() && self.is_empty())
        }
    }

    pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let (inner_tx, inner_rx) = crossbeam_channel::unbounded::<T>();
        let endpoints = EndpointCounts::new();
        (
            Sender {
                inner: inner_tx,
                endpoints: endpoints.shared(),
            },
            Receiver {
                inner: inner_rx,
                endpoints,
            },
        )
    }
}

pub use backend::{Receiver, Sender, unbounded};

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn send_recv_round_trip() {
        let (tx, rx) = unbounded::<u32>();
        tx.send(7).unwrap();
        assert_eq!(rx.recv().unwrap(), 7);
    }

    #[test]
    fn try_recv_empty() {
        let (_tx, rx) = unbounded::<u32>();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn recv_timeout_times_out() {
        let (_tx, rx) = unbounded::<u32>();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(1)),
            Err(RecvTimeoutError::Timeout)
        );
    }

    #[test]
    fn closed_after_sender_drop() {
        let (tx, rx) = unbounded::<u32>();
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(rx.recv().unwrap(), 1);
        assert!(rx.is_terminated());
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
        assert_eq!(rx.recv_timeout(Duration::ZERO), Err(RecvTimeoutError::Closed));
    }

    #[test]
    fn send_fails_after_receiver_drop() {
        let (tx, rx) = unbounded::<u32>();
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.send(1), Err(SendError));
    }

    #[test]
    fn len_is_empty_and_closed_checks() {
        let (tx, rx) = unbounded::<u32>();
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert!(!rx.is_empty());
        assert_eq!(rx.len(), 2);
        drop(tx);
        assert!(!rx.is_closed());
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), 2);
        assert!(rx.is_empty());
        assert!(rx.is_terminated());
        drop(rx);
    }

    #[test]
    fn cloned_endpoints_share_channel() {
        let (tx, rx) = unbounded::<u32>();
        let tx2 = tx.clone();
        let rx2 = rx.clone();
        tx.send(1).unwrap();
        tx2.send(2).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx2.recv().unwrap(), 2);
    }

    #[test]
    fn concurrent_send_recv() {
        let (tx, rx) = unbounded::<u32>();
        let producer = thread::spawn(move || {
            for value in 0..100 {
                tx.send(value).unwrap();
            }
        });
        for expected in 0..100 {
            assert_eq!(rx.recv().unwrap(), expected);
        }
        producer.join().unwrap();
    }
}
