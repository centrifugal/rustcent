//! A blocking client, for code without an async runtime.
//!
//! [`Client`] has the same methods, options, errors and behaviour as the
//! async [`crate::Client`] - it runs one, on a tokio runtime of its own - and
//! returns results instead of futures:
//!
//! ```no_run
//! let client = cent::blocking::Client::new(cent::Config::new("http://localhost:8000/api").api_key("secret"))?;
//! let data = cent::json(&serde_json::json!({"text": "hello"}))?;
//! let res = client.publish(cent::PublishRequest::new("news").data(data))?;
//! # Ok::<(), cent::Error>(())
//! ```

use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::client::Bound;
use crate::error::TimeoutError;
use crate::{
    BatchOptions, BatchResult, BroadcastRequest, BroadcastResult, BroadcastRetryOptions, Config,
    Error, Pending,
};

/// Calls Centrifugo's HTTP server API, blocking: the async
/// [`crate::Client`]'s methods, each returning its result. It is cheap to
/// clone, and safe for concurrent use from many threads: create one and
/// share it.
///
/// It runs its calls on a tokio runtime of its own, with two threads, which
/// also send its automatic batches. After a fork - Gunicorn-style
/// preforking, say - a client used in the child starts a runtime and
/// connections of its own.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
    bound: Option<Bound>,
}

struct Inner {
    client: crate::Client,
    runtime: Mutex<(u32, Arc<Runtime>)>,
}

/// A runtime which shuts down without waiting, so that dropping the last
/// client never blocks, nor panics inside another runtime.
struct Runtime(Option<tokio::runtime::Runtime>);

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("client", &self.inner.client)
            .finish_non_exhaustive()
    }
}

fn runtime() -> Result<Arc<Runtime>, Error> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("rustcent")
        .enable_all()
        .build()
        .map(|rt| Arc::new(Runtime(Some(rt))))
        .map_err(|e| Error::invalid_config(format!("starting the client's runtime: {e}")))
}

impl Client {
    /// Returns a client for `config`, or an [`Error::InvalidConfig`] naming
    /// what is wrong with it.
    pub fn new(config: Config) -> Result<Self, Error> {
        let client = crate::Client::new(config)?;
        let runtime = runtime()?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                runtime: Mutex::new((std::process::id(), runtime)),
            }),
            bound: None,
        })
    }

    /// Returns a handle to the same client whose calls each time out after
    /// `timeout`: see [`crate::Client::with_timeout`].
    #[must_use]
    pub fn with_timeout(&self, timeout: Duration) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            bound: Some(Bound::Timeout(timeout)),
        }
    }

    /// Returns a handle to the same client whose calls end at `deadline`:
    /// see [`crate::Client::with_deadline`].
    #[must_use]
    pub fn with_deadline(&self, deadline: Instant) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            bound: Some(Bound::Deadline(tokio::time::Instant::from_std(deadline))),
        }
    }

    /// The runtime of this process: after a fork the parent's has no threads
    /// here, so the child starts its own.
    fn current_runtime(&self) -> Result<Arc<Runtime>, Error> {
        let pid = std::process::id();
        let mut rt = self
            .inner
            .runtime
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if rt.0 != pid {
            let old = std::mem::replace(&mut rt.1, runtime()?);
            // Its threads are the parent's: shutting it down would wait for
            // nothing, or signal threads which do not exist here.
            std::mem::forget(old);
            rt.0 = pid;
        }
        Ok(Arc::clone(&rt.1))
    }

    /// Runs `f` with the async client on the runtime, and waits for it.
    pub(crate) fn run<T, F, Fut>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(crate::Client) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Error>> + Send + 'static,
        T: Send + 'static,
    {
        let rt = self.current_runtime()?;
        let client = self.inner.client.with_bound(self.bound);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        rt.0.as_ref().expect("a running runtime").spawn(async move {
            let _ = tx.send(CatchUnwind(Box::pin(f(client))).await);
        });
        match rx.recv() {
            Ok(Ok(result)) => result,
            // A panic in the call - in on_request, say - is the caller's.
            Ok(Err(panic)) => resume_unwind(panic),
            Err(_) => Err(Error::Closed),
        }
    }

    /// Returns an empty batch: see [`crate::Client::new_batch`].
    #[must_use]
    pub fn new_batch(&self, options: BatchOptions) -> Batch {
        Batch {
            inner: self.inner.client.with_bound(self.bound).new_batch(options),
            client: self.clone(),
        }
    }

    /// Publishes the same data into many channels in one request: see
    /// [`crate::Client::broadcast`].
    pub fn broadcast(&self, request: BroadcastRequest) -> Result<BroadcastResult, Error> {
        self.run(move |c| async move { c.broadcast(request).await })
    }

    /// Broadcasts, retrying the channels which may still succeed: see
    /// [`crate::Client::broadcast_with_retry`].
    pub fn broadcast_with_retry(
        &self,
        request: BroadcastRequest,
        options: BroadcastRetryOptions,
    ) -> Result<BroadcastResult, Error> {
        self.run(move |c| async move { c.broadcast_with_retry(request, options).await })
    }

    /// Stops the client and waits for the calls in progress: see
    /// [`crate::Client::close`].
    pub fn close(&self, timeout: Duration) -> Result<(), Error> {
        self.run(move |c| async move { c.close(timeout).await })
    }
}

/// A batch of a blocking [`Client`]: see [`crate::Batch`].
#[derive(Debug)]
pub struct Batch {
    pub(crate) inner: crate::Batch,
    client: Client,
}

impl Batch {
    /// How many commands the batch has.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the batch has no commands.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Adds a `broadcast` command: see [`crate::Batch::broadcast`].
    pub fn broadcast(&mut self, request: BroadcastRequest) -> Pending<BroadcastResult> {
        self.inner.broadcast(request)
    }

    /// Sends the batch and waits for Centrifugo's reply to every command:
    /// see [`crate::Batch::send`].
    pub fn send(self) -> Result<BatchResult, Error> {
        let inner = self.inner;
        self.client.run(move |_| inner.send())
    }
}

/// Waits for `d`, or until `deadline`, and returns [`Error::Timeout`] when
/// the deadline comes first: see [`crate::wait`].
pub fn wait(deadline: Instant, d: Duration) -> Result<(), Error> {
    let now = Instant::now();
    let until = now.checked_add(d).map_or(deadline, |t| t.min(deadline));
    std::thread::sleep(until.saturating_duration_since(now));
    if until >= deadline {
        return Err(Error::Timeout(TimeoutError {
            method: None,
            unsent: false,
        }));
    }
    Ok(())
}

/// A future which catches a panic of the one it wraps, boxed so that it is
/// `Unpin` without unsafe pin projection.
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::thread::Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.get_mut().0.as_mut();
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}
