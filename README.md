# cent

[![crates.io](https://img.shields.io/crates/v/cent.svg)](https://crates.io/crates/cent)
[![docs.rs](https://docs.rs/cent/badge.svg)](https://docs.rs/cent)

Rust SDK for the [HTTP server API](https://centrifugal.dev/docs/server/server_api)
of [Centrifugo](https://github.com/centrifugal/centrifugo): every API method,
including those of Centrifugo PRO, in an async `Client` and a blocking one;
connection, subscription and server API tokens; the proxy handlers and the
endpoints client SDKs call. For a real-time client over WebSocket, see
[centrifuge-rust](https://github.com/centrifugal/centrifuge-rust).

```sh
cargo add cent
```

| | Version |
|---|---|
| Rust | 1.85 or newer |
| Centrifugo | v6, any release |
| Map and shared poll methods | Centrifugo v6.8.0 or newer |
| `ApiError::temporary`, code 111 (too many requests), refused requests reported as such rather than as internal errors | Centrifugo v6.10.0 or newer |
| Ed25519 tokens verified without JWKS, by `client.token.ed25519_public_key` | Centrifugo v6.10.2 or newer |
| Methods marked "Centrifugo PRO only" | Centrifugo PRO v6 |

| Feature | Default | What |
|---|---|---|
| `client` | yes | The async `Client`, on tokio, with hyper and rustls (the operating system's trust store) |
| `token` | yes | Connection, subscription and API tokens, JWKS, shared poll signatures |
| `blocking` | | `blocking::Client`, for code without an async runtime |
| `tower` | | The HTTP handlers as `tower::Service`s, for axum, hyper and tower stacks |

cent implements the [Centrifugo server SDK
specification](https://github.com/centrifugal/gocent/blob/master/spec.md),
version 0.2, whose reference implementation is
[gocent](https://github.com/centrifugal/gocent).

## Example

```rust,no_run
use cent::{Client, Config, PublishRequest};
use serde_json::json;

# async fn run() -> Result<(), cent::Error> {
let client = Client::new(
    Config::new("http://localhost:8000/api").api_key(std::env::var("CENTRIFUGO_API_KEY").unwrap()),
)?;
let data = cent::json(&json!({"text": "hello"}))?;
let res = client.publish(PublishRequest::new("news").data(data)).await?;
println!("published at offset {}", res.offset);
# Ok(()) }
```

Every API method is a `Client` method taking its request and returning its
result: a struct generated from Centrifugo's `api.proto`, with its
documentation. A request's `new` takes the fields Centrifugo requires, and a
method per field sets the others. Create the client once and share it: it is
cheap to clone, and safe for concurrent use.

## Payloads

`data`, `info`, `meta` and the other payloads are JSON, as `Box<RawValue>` of
serde_json - in requests, and in results, where a payload is kept exactly as
Centrifugo sent it, number text included. A payload from a reply sent back
unchanged is the same JSON value. Encode any serde type with `cent::json`;
pass JSON you already have - from a database, a queue - with
`RawValue::from_string(text)`, which checks it is JSON; decode one with
`PayloadExt::decode`. A payload set to JSON `null` is sent as `null`; one not
set is not sent.

```rust
use cent::PayloadExt;

#[derive(serde::Serialize, serde::Deserialize)]
struct Message { text: String }

# fn run() -> Result<(), cent::Error> {
let request = cent::PublishRequest::new("news").data(cent::json(&Message { text: "hi".into() })?);
let message: Message = request.data.decode()?; // also a publication's data, a proxy request's
# Ok(()) }
```

serde_json writes NaN and infinities as `null`, which the client cannot tell
from a `null` you meant: check floats before encoding them. Binary data goes
base64-encoded in the `b64` fields, such as `b64data`.

Write integers which may pass 2^53 - a 64-bit ID, say - as JSON strings:
browsers and JavaScript backends decode JSON numbers as doubles, and round
larger integers silently.

## Timeouts

Each call is bounded by one deadline: the client's `request_timeout`, ten
seconds by default, or one of its own:

```rust,no_run
# async fn run(client: cent::Client) -> Result<(), cent::Error> {
use std::time::Duration;
let info = client.with_timeout(Duration::from_millis(500)).info(cent::InfoRequest::new()).await?;
# Ok(()) }
```

The deadline bounds the whole call - the endpoint and token functions,
connecting, the reply read, a reply trickling in included. However a call
times out, the error is `Error::Timeout`. `with_deadline` gives several calls
one deadline, such as the attempts of a retry loop and the waits between
them. A timeout around a call's future, `tokio::time::timeout`, ends it too.

`broadcast_with_retry` makes several requests in one call: its deadline bounds
them all, waits between them included, and `attempt_timeout` each one.

## Errors

| `Error` | Meaning |
|---|---|
| `Api` | Centrifugo refused the request or failed to carry it out: `code`, `message`, `temporary`. Compare `err.api_code()` with `ApiError::UNKNOWN_CHANNEL` and the other constants |
| `BroadcastFailedChannels`, `BatchFailedCommands` | some parts of a broadcast or batch failed - see below |
| `Http` | the request did not reach the API method: `status`, `body`, `headers`; `err.is_unauthorized()` for refused credentials |
| `Decode` | the reply was not Centrifugo's - usually `api_endpoint` points elsewhere |
| `InvalidRequest` | the client could tell the request is wrong and did not send it |
| `InvalidConfig` | the client was not created, or is used where it cannot work: the message names the option and the fix |
| `Timeout`, `Network`, `Closed` | as their names say |
| `Callback` | the application's `api_endpoint` or `bearer_token` function failed |

Centrifugo adds error codes over time: an unknown code is still an
`Error::Api`, so match the codes you handle and treat the rest by kind.

## Retries

`cent::retryable(&err)`, or `err.is_retryable()`, reports whether a new
attempt, with time left, may succeed: the error came from a temporary
condition - a broker being unavailable, a rate limit, the network - not from
the request.

A temporary error does not mean nothing was done: after a timeout, or even an
internal error, the publication may have happened. So retry a publication only
with an `idempotency_key`, and back off between a bounded number of attempts:

```rust,no_run
# async fn run(client: cent::Client, data: Box<cent::RawValue>) -> Result<(), cent::Error> {
use std::time::{Duration, Instant};

let backoff = cent::exponential_backoff(Duration::from_millis(200), Duration::from_secs(5));
let deadline = Instant::now() + Duration::from_secs(10);
let client = client.with_deadline(deadline); // every attempt, and the waits
// The same key on every attempt: a publication which happened in an earlier
// attempt, even one reported as failed, is not published again.
let request = cent::PublishRequest::new("orders:42").data(data).idempotency_key("order-42-paid");
for attempt in 1.. {
    match client.publish(request.clone()).await {
        Err(e) if e.is_retryable() && attempt < 3 => cent::wait(deadline, backoff.delay(attempt)).await?,
        result => return result.map(drop),
    }
}
# Ok(()) }
```

Choosing keys:

- One key per publication or broadcast, reused only to retry that same one. A
  broadcast needs one key, not one per channel.
- Centrifugo remembers a key per channel, for five minutes by default. A retry
  within that time is answered with the first result and not published again -
  and so is a *different* publication into the same channel under the same
  key. A key built from an ID must name the event too: `order-42-paid`, not
  `order-42`.
- The memory, Redis and PostgreSQL brokers keep keys. The NATS broker ignores
  them, so with it a retry publishes again.

Retry a broadcast with `broadcast_with_retry` rather than a loop: see below.

## Broadcast and batch

`broadcast` publishes the same data into many channels in one request; a batch
sends many commands at once. Both return an error unless every part succeeded:

```rust,no_run
# async fn run(client: cent::Client, data: Box<cent::RawValue>) -> Result<(), cent::Error> {
use cent::{BatchOptions, BroadcastRequest, PresenceStatsRequest, PublishRequest};

client.broadcast(BroadcastRequest::new(["user:1", "user:2"]).data(data.clone()).idempotency_key("message-42")).await?;

let mut batch = client.new_batch(BatchOptions::default());
let publication = batch.publish(PublishRequest::new("news").data(data));
let stats = batch.presence_stats(PresenceStatsRequest::new("news"));
batch.send().await?;
let offset = publication.result()?.offset; // each command has its own result, or its own error
# Ok(()) }
```

Parts succeed or fail on their own. When Centrifugo carried the request out
and some parts failed, up to all, the error is `Error::BroadcastFailedChannels`
or `Error::BatchFailedCommands`: its `failed()` lists them, and its `result()`
holds every part's outcome. Any other error means the request failed as a
whole - the network, the credentials, an invalid request - and no part got a
reply; after a timeout or a network error Centrifugo may still have carried it
out.

A partial error is not an error of its parts: `err.api_code()` is `None` for a
broadcast in which a channel was unknown, also one into a single channel -
`publish` is the call for one channel. To go on despite some failed parts:

```rust,no_run
# async fn run(client: cent::Client, request: cent::BroadcastRequest) -> Result<(), cent::Error> {
match client.broadcast(request).await {
    Err(cent::Error::BroadcastFailedChannels(e)) => {
        for (channel, err) in e.errors() {
            eprintln!("not published into {channel}: {err}");
        }
    }
    other => { other?; }
}
# Ok(()) }
```

Retrying:

- a **broadcast**: `broadcast_with_retry` does it. It sends again only the
  channels which may still succeed, all attempts with one `idempotency_key` -
  made for the call when the request has none - and returns every channel's
  last outcome, as `broadcast` does. By default it retries once, after about
  100 milliseconds:

  ```rust,no_run
  # async fn run(client: cent::Client, request: cent::BroadcastRequest) -> Result<(), cent::Error> {
  use std::time::Duration;
  let backoff = cent::exponential_backoff(Duration::from_millis(200), Duration::from_secs(5));
  let options = cent::BroadcastRetryOptions::default()
      .max_attempts(5)
      .attempt_timeout(Duration::from_secs(2))
      .on_attempt_error(move |a| {
          eprintln!("attempt {}: {} channels to retry", a.attempt, a.retry.len());
          cent::Retry::After(backoff.delay(a.attempt)) // or Retry::Default, Retry::stop(reason)
      });
  let res = client.broadcast_with_retry(request, options).await?;
  # Ok(()) }
  ```

  The callback returns a `Retry`: the default wait, a wait of its own, or
  stop - so a callback which only logs cannot stop the retries by accident. An
  `Error::Http` in `a.error` carries the reply's `headers`: wait at least its
  `Retry-After` to honour one. `res.idempotency_key` is the key the attempts
  used: to retry the broadcast later, after it gave up - from a job queue, say
  - send it with that key. Repeating `broadcast` on any error would instead
  send it to every channel again, those which already got it included.
- a **batch**: build a new one, with the same keys, from the commands whose
  error is retryable. A batch is sent once: `send` takes it.

`BatchOptions::default()` runs the commands one by one, in order;
`.parallel(true)` runs them concurrently. Centrifugo PRO sends a batch's
publications to its broker together: each channel's publications still take
effect in order, but publications into different channels may not, unless the
namespace sets `publication_grouping_disabled`.

## Automatic batching

With `auto_batch`, calls made concurrently go out together once enough
requests are in flight - with no delay otherwise, since nothing waits for a
timer:

```rust,no_run
# fn run(key: String) -> Result<(), cent::Error> {
let client = cent::Client::new(
    cent::Config::new("http://localhost:8000/api").api_key(key).auto_batch(cent::AutoBatch::new(8)),
)?;
# Ok(()) }
```

It is for processes making many calls at once - a server publishing for every
request it serves, a worker draining a queue concurrently. Choose
`max_in_flight` at about half the calls the process makes at once: above them
it never batches, and just below it batches calls one at a time, only
limiting them.

Every call still gets its own result or error, and keeps its own deadline.
Calls sharing a batch share its request, though: if the request itself fails
- the network, a body too large for a proxy in front of Centrifugo (nginx
refuses bodies over 1 MB by default) - every call in it fails. Lower
`max_batch_size`, 1000 by default, or raise the proxy's limit.

## Authentication

`api_key` is sent in the `X-API-Key` header. `bearer_token` instead sends a
token as `Authorization: Bearer`, for Centrifugo PRO's JWKS authentication -
it is called for every request, so return a cached token. Leave both unset
when mutual TLS, set up in your `HttpClient`, or a proxy authenticates
instead.

To sign the tokens yourself, `token::ApiTokens` issues them and reuses each for
half its lifetime:

```rust,no_run
# fn run(api_key: cent::token::Key) -> Result<(), cent::Error> {
use cent::token::{ApiClaims, ApiTokens};

let tokens = ApiTokens::new(
    api_key, // its own key - never one of the client tokens
    ApiClaims::new().audience("centrifugo-api").scope(["centrifugo:api"]),
);
let client = cent::Client::new(cent::Config::new("https://centrifugo.example.com/api").bearer_token(tokens))?;
# Ok(()) }
```

Centrifugo PRO accepts a token signed with any key of `http_api.jwks.endpoint`,
and checks the audience, issuer and scope only when configured to. So keep API
tokens apart from the connection and subscription tokens your users hold - or
any user can call the server API with their connection token:

- publish the API keys with `endpoints::jwks(&api_keys)` at an endpoint of
  their own, not the one of `client.token.jwks_public_endpoint`;
- set `http_api.jwks.audience` and `http_api.jwks.scope` to values only API
  tokens carry - `token::api` refuses claims with neither.

Refused credentials make every call fail with `Error::Http` 401:
`err.is_unauthorized()`.

## HTTP client, tracing and metrics

The default HTTP client is hyper with rustls, verifying certificates with the
operating system's trust store. It keeps up to 256 connections to Centrifugo
alive, keeps all of them while idle, closes idle ones after 50 seconds, speaks
HTTP/2 where TLS negotiates it, and never follows redirects - a redirect is an
`Error::Http` with a hint to check the endpoint. It sets no timeouts of its
own: each call's deadline bounds it. It does not read the `HTTP_PROXY`
variables: give the client an `HttpClient` of your own to go through a proxy.

An `HttpClient` is any function from an `http::Request<Bytes>` to a future of
the response - for tracing, wrap the default one:

```rust,no_run
# fn run() -> Result<(), cent::Error> {
use cent::HttpClient;

let inner = cent::DefaultHttpClient::new()?;
let traced = move |req: http::Request<bytes::Bytes>| {
    let method = req.uri().path().rsplit('/').next().unwrap_or("").to_owned();
    let reply = inner.send(req);
    async move {
        // Start a span here - with OpenTelemetry, inject its context into the
        // request's headers - and end it when the reply arrives.
        let started = std::time::Instant::now();
        let r = reply.await;
        eprintln!("centrifugo {method}: {:?}", started.elapsed());
        r
    }
};
let client = cent::Client::new(cent::Config::new("http://localhost:8000/api").http_client(traced))?;
# Ok(()) }
```

`on_request` is told of every request - method, commands and how many failed,
duration, status, error - for metrics without a dependency of the crate's. It
and the handlers' `on_error` must not panic: a panic is the call's, or the
handler's.

## Testing your code

Code using the client is tested without Centrifugo by giving the client an
`HttpClient` answering as Centrifugo does: a method's reply is
`{"result": {...}}` or `{"error": {"code": ..., "message": ...}}`; a
broadcast's, `{"result": {"responses": [...]}}` with one response per channel;
a batch's, `{"replies": [...]}` with one reply per command, `{"<method>": {...}}`
or `{"error": {...}}`.

```rust
# #[tokio::main(flavor = "current_thread")] async fn main() -> Result<(), cent::Error> {
use bytes::Bytes;

let fake = |req: http::Request<Bytes>| async move {
    let reply = match req.uri().path() {
        "/api/publish" => r#"{"result":{"offset":1}}"#,
        "/api/broadcast" => r#"{"result":{"responses":[{"result":{"offset":1}},{"error":{"code":102,"message":"unknown channel"}}]}}"#,
        _ => r#"{"replies":[{"publish":{"offset":1}},{"error":{"code":102,"message":"unknown channel"}}]}"#,
    };
    Ok::<_, cent::BoxError>(http::Response::new(Bytes::from(reply)))
};
let client = cent::Client::new(cent::Config::new("http://centrifugo/api").http_client(fake))?;
let data = || cent::RawValue::from_string("{}".into()).unwrap();
let e = client.broadcast(cent::BroadcastRequest::new(["a", "b"]).data(data())).await.unwrap_err();
assert_eq!(e.broadcast_result().unwrap().failed()[0].channel, "b");
# Ok(()) }
```

The partial-failure errors and the results they carry are constructible for
test doubles of your own: `BroadcastResult::new`, `ChannelResult::new`,
`BroadcastFailedChannelsError::new`, `BatchResult::new`, `FailedCommand::new`,
`BatchFailedCommandsError::new`, `Pending::new`.

## Shutdown

`client.close(timeout)` refuses new calls with `Error::Closed` and waits for
the ones in progress, automatic batches included, so that what was accepted is
sent before the process exits. It returns `Error::Timeout` when calls are still
in progress after `timeout`; they go on.

## The blocking client

`cent::blocking::Client` - with the `blocking` feature - has the same methods,
options, errors and behaviour, and returns results instead of futures. It
runs the async client on a tokio runtime of its own. After a fork, a client
used in the child starts a runtime and connections of its own.

The async `Client` runs on the tokio runtime its first call was made from,
whose tasks send its automatic batches and own its connections: a program
with several runtimes creates a client per runtime - the client says so
rather than failing with the runtime's error.

## HTTP handlers

The proxy handlers, the endpoints client SDKs call and the JWKS endpoint are
each a `Handler`: one function, `handler.handle(http::Request<Bytes>)`, from a
request with its body to a response, holding all of the endpoint's behaviour.
With the `tower` feature a `Handler` is a `tower::Service`: in axum,
`Router::new().route_service("/centrifugo/connect", handler)`. From any other
framework, build the `http::Request` and turn the response back. The function
a handler calls gets the request's `http::request::Parts`: headers, URI and
extensions - where a middleware puts the authenticated user.

## Proxy

`cent::proxy` is the backend side of Centrifugo's HTTP proxy, which calls your
application when a client connects, subscribes, publishes or makes an RPC
call: a handler per proxy type, taking an async function.

```rust
use cent::proxy::{self, Rejection};

let handlers = proxy::Handlers::new().on_error(|parts, err| eprintln!("{}: {err}", parts.uri.path()));
let subscribe = handlers.subscribe(|parts, req| async move {
    if !can_read(&req.user, &req.channel) {
        return Err(Rejection::from(proxy::Error::new(403, "permission denied")));
    }
    Ok(proxy::SubscribeResult::new())
});
# fn can_read(_: &str, _: &str) -> bool { true }
```

Return a result to accept the request, a `proxy::Error` - a code in
[400, 1999] - to reject it with an error the client receives, or a
`proxy::Disconnect` - a code in [4000, 4999], those in [4500, 4999] telling the
client not to reconnect, and a reason of at most 32 bytes - to close the
client's connection. Any other error, which `?` turns into a rejection, fails
the request with 500 and goes to `on_error` to be logged. Keep proxy endpoints
reachable by Centrifugo only: whoever can call them can claim to be any user.

## Tokens

`cent::token` issues the JWTs Centrifugo authenticates clients with -
connection and subscription tokens, with Centrifugo's claims, signed with
HMAC, RSA, ECDSA or Ed25519 - and `cent::endpoints` serves the endpoints the
`getToken` callbacks of client SDKs call:

```rust
use std::time::Duration;
use cent::endpoints::{self, Unauthorized};
use cent::token::{self, ConnectionClaims, Key};

let key = Key::hmac("secret")?; // client.token.hmac_secret_key
let connection_token = endpoints::Handlers::new().connection_token(move |parts| {
    let key = key.clone();
    async move {
        let Some(user) = parts.extensions.get::<String>() else {
            return Err(Unauthorized.into()); // 403: the client stops asking
        };
        let claims = ConnectionClaims::new(user, cent::Expiry::after(Duration::from_secs(3600)));
        Ok(token::connection(&key, &claims)?)
    }
});
# Ok::<(), cent::token::TokenError>(())
```

An expiry is required: `Expiry::At(time)`, or `Expiry::Never` for a token
accepted forever, chosen explicitly. A connection token's `meta` is never shown
to other clients, but the user owning the token can read it: put nothing in it
that user must not see. `subscription_token` serves subscription tokens the same
way, with the channel the client asks for.

With RSA, ECDSA or Ed25519 keys - `Key::from_pem`, `Key::ed25519` -
`endpoints::jwks(&keys)` publishes the public keys for Centrifugo's
`jwks_public_endpoint`: the way to rotate keys without restarting Centrifugo.
Publish a new key next to the old one; sign with it only once every instance
of the application publishes it - from an activation time set ahead, or in a
second deploy; drop the old key once its tokens expired. Never give a key's ID
to another key: Centrifugo caches keys by ID. The `backend` example rotates by
activation time.

## Shared poll signatures

Clients tracking keys of a [shared poll](https://centrifugal.dev/docs/server/shared_poll)
channel need a signature from the backend. `cent::sharedpoll::sign` makes one,
and `endpoints::Handlers::shared_poll_signature` serves the endpoint a
subscription's `getSignature` callback calls. Shared poll subscriptions are
experimental in Centrifugo, and the module follows their changes.

```rust
use std::time::Duration;
use cent::endpoints::{self, SignedKeys};

let secret = b"shared_poll.hmac_secret_key".to_vec(); // from your config
let signature = endpoints::Handlers::new().shared_poll_signature(move |parts, channel, keys| {
    let secret = secret.clone();
    async move {
        let user = "42"; // the user your authentication found in `parts`
        let keys: Vec<String> = keys.into_iter().filter(|k| may_track(user, k)).collect();
        // secret, user, channel, keys, expiry - user before channel.
        let sig = cent::sharedpoll::sign(&secret, user, &channel, &keys, cent::Expiry::after(Duration::from_secs(3600)))?;
        Ok(SignedKeys::new(keys, sig))
    }
});
# fn may_track(_: &str, _: &str) -> bool { true }
```

## Commands for async consumers

`Command::publish(request)` - a function per API method - checks a request as
the client would and encodes it, for Centrifugo's [async
consumers](https://centrifugal.dev/docs/server/consumers): its `method()` and
`payload()` for an outbox table, or `to_json()` for a Kafka message.

```rust
let data = cent::RawValue::from_string(r#"{"text":"hi"}"#.into()).unwrap();
let cmd = cent::Command::publish(cent::PublishRequest::new("news").data(data))?;
// INSERT INTO centrifugo_outbox (method, payload, partition) VALUES ($1, $2::jsonb, 0)
assert_eq!((cmd.method(), cmd.payload().get()), ("publish", r#"{"channel":"news","data":{"text":"hi"}}"#));
# Ok::<(), cent::Error>(())
```

A consumer keeps commands in order only within a partition: to keep a
channel's commands in order, derive their partition from the channel the same
way in every writer.

## Streaming AI output

Publish a model's output into a channel of its own, in a namespace with history
and recovery, from one task - each publish waiting for the previous one - in
chunks of what arrived every 50 ms or so, not a publication per token: the
`stream` example. See [Centrifugo for AI
apps](https://centrifugal.dev/docs/getting-started/ai_apps), which also covers
sizing history for a whole response.

## More

- [API documentation](https://docs.rs/cent), and the
  [examples](crates/cent/examples).
- Docs for agents and LLMs: [centrifugal.dev/llms.txt](https://centrifugal.dev/llms.txt),
  an index of the current (v6) documentation.
- Development: see [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md).

MIT license. Centrifugo PRO, which the methods marked "Centrifugo PRO only"
call, is commercial software under its own
[license](https://centrifugal.dev/license).
