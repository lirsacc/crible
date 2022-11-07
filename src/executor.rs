use std::sync::Arc;

use crible_lib::Index;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::sync::{oneshot, Semaphore, TryAcquireError};

use crate::backends::Backend;

static DEFAULT_QUEUE_SIZE_TO_POOL_SIZE_RATIO: usize = 10;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Too many requests")]
    TooManyRequests,
    #[error("Unknown {0}")]
    Unknown(eyre::Report),
}

pub struct ExecutorBuilder {
    index: Arc<RwLock<Index>>,
    backend: Arc<Mutex<Box<dyn Backend>>>,
    read_only: bool,
    pool_size: Option<usize>,
    queue_size: Option<usize>,
}

impl ExecutorBuilder {
    pub fn new(
        index: Arc<RwLock<Index>>,
        backend: Arc<Mutex<Box<dyn Backend>>>,
    ) -> Self {
        Self {
            index,
            backend,
            read_only: false,
            pool_size: None,
            queue_size: None,
        }
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = Some(pool_size);
        if self.queue_size.is_none() {
            self.queue_size = self.pool_size;
        }
        self
    }

    pub fn queue_size(mut self, queue_size: usize) -> Self {
        self.queue_size = Some(queue_size);
        self
    }

    pub fn build(self) -> eyre::Result<Executor> {
        let pool_size = self.pool_size.unwrap_or_else(num_cpus::get);
        let queue_size = self
            .queue_size
            .unwrap_or(pool_size * DEFAULT_QUEUE_SIZE_TO_POOL_SIZE_RATIO);

        Ok(Executor {
            index: self.index,
            backend: self.backend,
            read_only: self.read_only,
            queue: Semaphore::new(queue_size),
            thread_pool: rayon::ThreadPoolBuilder::new()
                .thread_name(|n| format!("crible-executor-thread-{}", n))
                .num_threads(pool_size)
                .build()?,
        })
    }
}

pub struct Executor {
    queue: Semaphore,
    thread_pool: rayon::ThreadPool,
    index: Arc<RwLock<Index>>,
    backend: Arc<Mutex<Box<dyn Backend>>>,
    pub read_only: bool,
}

impl Executor {
    pub async fn spawn<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(Arc<RwLock<Index>>) -> T + Send + 'static,
        T: Sync + Send + 'static,
    {
        // TODO: Can we support both queued and unlimited queue?
        let maybe_permit = self.queue.try_acquire();
        match maybe_permit {
            Err(TryAcquireError::NoPermits) => {
                return Err(Error::TooManyRequests);
            }
            Err(e) => {
                return Err(Error::Unknown(eyre::Report::new(e)));
            }
            _ => {}
        };

        let index = self.index.clone();

        let (tx, rx) = oneshot::channel();

        self.thread_pool.spawn(move || {
            let result = func(index);
            // TODO: Handle error?
            let _ = tx.send(result);
        });

        rx.await.map_err(|e| Error::Unknown(eyre::Report::new(e)))
    }

    pub async fn reload(&self) -> eyre::Result<()> {
        let backend = self.backend.clone();
        self.spawn(move |index| {
            *index.as_ref().write() = backend.lock().load()?;
            Ok(())
        })
        .await?
    }

    // TODO: Expose partial writes.
    pub async fn flush(&self) -> eyre::Result<()> {
        if !self.read_only {
            let backend = self.backend.clone();
            self.spawn(move |index| {
                backend.lock().dump(&index.read())?;
                Ok(())
            })
            .await?
        } else {
            Ok(())
        }
    }
}
