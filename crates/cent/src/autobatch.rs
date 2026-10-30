//! Automatic batching (spec 8): see [`AutoBatch`](crate::AutoBatch).
//!
//! The batcher counts requests in flight. Below the cap a call is sent at
//! once, as a request of its own; at the cap it queues. Whenever a request
//! finishes and calls are queued, its slot passes straight to one batch
//! carrying them - so requests in flight never exceed the cap, and nothing
//! ever waits on a timer.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::client::{
    Engine, RequestInfo, batch_body, command_bytes, decode_batch, decode_reply, is_error_reply,
};
use crate::error::TimeoutError;
use crate::{Client, Error};

pub(crate) struct Batcher {
    max_in_flight: usize,
    max_batch: usize,
    /// The runtime batches are sent on: the client's.
    handle: Handle,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    in_flight: usize,
    queue: VecDeque<Arc<Queued>>,
}

/// A call waiting to be sent in a batch.
struct Queued {
    /// The call's command, encoded by the caller before it queued: the batch
    /// sends these bytes, and never reads the caller's request.
    command: Vec<u8>,
    /// The call's API method, for the errors of its batch.
    method: &'static str,
    /// The caller's deadline: the batch carrying the call lasts until the
    /// latest deadline of its calls, and a call past it is not sent.
    deadline: Instant,
    /// Set under the batcher's lock: `taken` once the call is in a batch,
    /// `gone` once its caller gave up waiting before that.
    taken: AtomicBool,
    gone: AtomicBool,
    /// Taken by the batch carrying the call: if that batch is dropped
    /// unsent - its task panicked, or the runtime shut down - the caller
    /// learns at once, not at its deadline.
    reply: Mutex<Option<oneshot::Sender<Reply>>>,
}

/// A call taken into a batch, with the sender of its reply.
struct Taken {
    q: Arc<Queued>,
    tx: oneshot::Sender<Reply>,
}

/// A call's reply in a batch, or the error of the batch.
type Reply = Result<Box<RawValue>, Error>;

/// What a call gets from the batcher.
enum Turn {
    /// A free slot: it is sent alone.
    Slot,
    /// No free slot: it queued.
    Queued(Arc<Queued>, oneshot::Receiver<Reply>),
    /// No free slot, and it has no command to queue yet.
    Encode,
}

impl State {
    /// Removes calls whose callers gave up from the head of the queue. It
    /// only saves work: a batch skips them anyway.
    fn drop_gone(&mut self) {
        while self
            .queue
            .front()
            .is_some_and(|q| q.gone.load(Ordering::Relaxed))
        {
            self.queue.pop_front();
        }
    }
}

impl Batcher {
    pub(crate) fn new(max_in_flight: usize, max_batch: usize, handle: Handle) -> Self {
        Self {
            max_in_flight,
            max_batch,
            handle,
            state: Mutex::new(State::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends a call as a request of its own when a slot is free, and in the
    /// next batch otherwise.
    pub(crate) async fn call<T: DeserializeOwned + Default>(
        self: Arc<Self>,
        client: &Client,
        engine: &Arc<Engine>,
        method: &'static str,
        pro: bool,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<T, Error> {
        let mut command = None;
        let (q, mut rx) = loop {
            match self.turn(method, &mut command, deadline) {
                Turn::Queued(q, rx) => break (q, rx),
                Turn::Slot => {
                    // The slot is released however the call ends: a
                    // cancelled call must not keep it.
                    let _slot = Slot {
                        batcher: Arc::clone(&self),
                        client: client.clone(),
                        engine: Arc::clone(engine),
                    };
                    return client.send_alone(engine, method, pro, body, deadline).await;
                }
                // The call will queue: encode its command first, outside the
                // lock, then look again - a slot may have freed meanwhile,
                // and a call must never queue while one is free, or nothing
                // would send it.
                Turn::Encode => command = Some(command_bytes(method, &body)),
            }
        };
        drop(body);
        // Marks the call gone if its caller leaves - by timing out or by
        // dropping this future - before it is in a batch.
        let _leave = Leave {
            batcher: &self,
            q: &q,
        };
        let reply = match tokio::time::timeout_at(deadline, &mut rx).await {
            Ok(Ok(reply)) => reply,
            // The batch carrying the call was dropped before its reply: its
            // task panicked - in on_request, say - or the runtime shut down.
            // Whether the call took effect is unknown, as after a network
            // failure.
            Ok(Err(_)) => Err(Error::Network(crate::error::NetworkError::new(
                method,
                std::sync::Arc::from(crate::BoxError::from(
                    "the batch carrying the call was dropped before its reply",
                )),
                false,
            ))),
            Err(_) => {
                let taken = {
                    let _st = self.lock();
                    let taken = q.taken.load(Ordering::Relaxed);
                    if !taken {
                        q.gone.store(true, Ordering::Relaxed);
                    }
                    taken
                };
                match rx.try_recv() {
                    // The reply came as the deadline passed: it is the call's
                    // outcome. A timeout instead would have the caller retry
                    // a publication which happened.
                    Ok(reply) => reply,
                    Err(_) => Err(Error::Timeout(TimeoutError {
                        method: Some(method),
                        unsent: !taken,
                    })),
                }
            }
        };
        decode_reply(&reply?, method)
    }

    /// Takes a free slot, or queues the call with its command.
    fn turn(&self, method: &'static str, command: &mut Option<Vec<u8>>, deadline: Instant) -> Turn {
        let mut st = self.lock();
        st.drop_gone();
        if st.in_flight < self.max_in_flight && st.queue.is_empty() {
            st.in_flight += 1;
            return Turn::Slot;
        }
        let Some(command) = command.take() else {
            return Turn::Encode;
        };
        let (tx, rx) = oneshot::channel();
        let q = Arc::new(Queued {
            command,
            method,
            deadline,
            taken: AtomicBool::new(false),
            gone: AtomicBool::new(false),
            reply: Mutex::new(Some(tx)),
        });
        st.queue.push_back(Arc::clone(&q));
        Turn::Queued(q, rx)
    }

    /// Hands the slot of a finished request to the calls waiting, if any,
    /// and frees it otherwise.
    fn release(self: &Arc<Self>, client: &Client, engine: &Arc<Engine>) {
        let items = {
            let mut st = self.lock();
            st.drop_gone();
            if st.queue.is_empty() {
                st.in_flight -= 1;
                return;
            }
            let now = Instant::now();
            let mut items = Vec::with_capacity(st.queue.len().min(self.max_batch));
            while items.len() < self.max_batch {
                let Some(q) = st.queue.pop_front() else { break };
                // A call past its deadline is not sent, though its caller
                // has not seen the timeout yet: it would report the call
                // unsent.
                if q.deadline <= now {
                    q.gone.store(true, Ordering::Relaxed);
                }
                if q.gone.load(Ordering::Relaxed) {
                    continue;
                }
                let Some(tx) = q
                    .reply
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                else {
                    continue;
                };
                q.taken.store(true, Ordering::Relaxed);
                items.push(Taken { q, tx });
            }
            items
        };
        // Sent from a task of its own, so that no caller is held by work
        // done for others, and a caller leaving does not cancel it. Counted
        // as a call: close waits for it even when its callers left.
        let call = client.shared.add();
        let slot = Slot {
            batcher: Arc::clone(self),
            client: client.clone(),
            engine: Arc::clone(engine),
        };
        self.handle.spawn(async move {
            let _call = call;
            flush(&slot.client, &slot.engine, items).await;
            drop(slot); // passes the slot on
        });
    }
}

/// A request slot, released when dropped.
struct Slot {
    batcher: Arc<Batcher>,
    client: Client,
    engine: Arc<Engine>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.batcher.release(&self.client, &self.engine);
    }
}

/// Marks a queued call gone when its caller leaves before it is taken.
struct Leave<'a> {
    batcher: &'a Batcher,
    q: &'a Queued,
}

impl Drop for Leave<'_> {
    fn drop(&mut self) {
        let _st = self.batcher.lock();
        if !self.q.taken.load(Ordering::Relaxed) {
            self.q.gone.store(true, Ordering::Relaxed);
        }
    }
}

/// Sends one automatic batch and gives each call its reply.
async fn flush(client: &Client, engine: &Engine, items: Vec<Taken>) {
    if items.is_empty() {
        return;
    }
    // The batch serves several callers, so no one caller may end it: it
    // lasts until the latest of their deadlines, and never less than the
    // request timeout from now.
    let deadline = items.iter().map(|t| t.q.deadline).fold(
        crate::client::after(client.shared.request_timeout),
        Instant::max,
    );
    // Parallel: the calls of an automatic batch were made concurrently - a
    // task waits for one call before making the next - so nothing orders
    // them, and Centrifugo may pipeline them as separate requests.
    let body = batch_body(items.iter().map(|t| t.q.command.as_slice()), true);
    let start = std::time::Instant::now();
    let mut status = 0;
    let replies = tokio::time::timeout_at(
        deadline,
        client.post(engine, "batch", false, body, &mut status),
    )
    .await
    .unwrap_or_else(|_| Err(Error::timeout("batch")))
    .and_then(|b| decode_batch(&b, items.len()));
    let failed = replies
        .as_ref()
        .map_or(0, |r| r.iter().filter(|r| is_error_reply(r)).count());
    client.shared.report(&RequestInfo {
        method: "batch",
        commands: items.len(),
        failed,
        auto_batch: true,
        duration: start.elapsed(),
        status,
        error: replies.as_ref().err(),
    });
    match replies {
        Ok(replies) => {
            for (t, r) in items.into_iter().zip(replies) {
                let _ = t.tx.send(Ok(r));
            }
        }
        // A batch request failing as a whole fails every call in it - a
        // timeout as each call's own.
        Err(e) => {
            for t in items {
                let e = match &e {
                    Error::Timeout(_) => Error::timeout(t.q.method),
                    e => e.clone(),
                };
                let _ = t.tx.send(Err(e));
            }
        }
    }
}
