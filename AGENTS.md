# Working on rustcent

rustcent - the crate `cent` - is the Rust SDK for Centrifugo's HTTP server API,
written to the [Centrifugo server SDK
specification](https://github.com/centrifugal/gocent/blob/master/spec.md),
version 0.4, whose reference implementation is gocent. Applications call it on
their hot paths, from tokio tasks and from plain threads, so it has to be
correct under concurrency and hard to misuse. Read this before changing code.

## Layout

| Path | What |
|---|---|
| `proto/api.proto`, `proto/proxy.proto` | Copies of Centrifugo's protos, from a released tag (`cargo xtask sync-proto v6.10.2`) |
| `xtask/` | The generator: `proto.rs` parses the protos, `generate.rs` writes the code - the types, the constants of the fields whose comment says `One of: ...`, the docs of the presence wrappers - and its tests pin what it reads from the comments |
| `xtask/constants_api.txt`, `xtask/constants_proxy.txt` | The names of the constants, as the spec's table in section 3.3 pins them: `Message.field Name`, or `*.field Name`. Generation fails on a "One of:" field a list lacks, a pin no field uses, and one name over different values: name a new field in the spec's table, then here |
| `xtask/required_fields.txt` | The fields the protos mark "Required.", made required arguments of `new` - the reviewed list the other SDKs share. Generation fails when the protos' list differs |
| `xtask/constructors.txt` | The constructors with several required arguments, in the protos' field order. Generation fails when one changes: a proto reordering two fields of one type would swap what callers pass without a compile error |
| `crates/cent/src/generated/` | Generated - never edited: request and result types with their constructors and setters, the client, batch, command and blocking client methods, the proxy types |
| `crates/cent/tests/methods.rs` | Generated - never edited: every method reaches its endpoint, batch command and consumer command, async and blocking |
| `crates/cent/src/client.rs` | `Config`, `Client`, the request path, the runtime and fork checks, close |
| `crates/cent/src/http_client.rs` | The `HttpClient` trait and `DefaultHttpClient` (hyper, rustls, the OS trust store) |
| `crates/cent/src/error.rs` | `Error`, its kinds and `retryable` |
| `crates/cent/src/broadcast.rs`, `retry.rs`, `batch.rs`, `autobatch.rs` | Broadcast results, retries and backoff, batches, automatic batching |
| `crates/cent/src/pages.rs` | `Pager` and the `*_all` methods of the paged methods, `position()` on the results holding a stream position |
| `crates/cent/src/blocking.rs` | The blocking client: the async one on a runtime of its own |
| `crates/cent/src/handler.rs` | `Handler`: the HTTP handlers' framework-free core, and its tower adapter |
| `crates/cent/src/proxy.rs`, `endpoints.rs`, `token/`, `sharedpoll.rs`, `command.rs` | Proxy handlers, endpoints for client SDKs, tokens and JWKS, shared poll signatures, commands |
| `crates/cent/tests/` | Tests against a fake Centrifugo (`common/`), local servers (`network.rs`), property tests, and against a real Centrifugo (`integration.rs`) |
| `testdata/` | The Centrifugo config and the Centrifugo PRO compose file the integration tests use |

## Invariants

These are not preferences. A change that breaks one is wrong even if every test
passes; if you find yourself about to break one, stop and say so instead.

1. **The generated files are never edited.** Change `proto/` or `xtask/`, then
   `cargo xtask generate`; `cargo xtask generate --check` fails when they are
   stale.
2. **A call returns an error unless it did everything asked.** A broadcast or
   batch in which some parts failed returns `Error::BroadcastFailedChannels` or
   `Error::BatchFailedCommands`, holding every part's outcome. A partial error
   never has its parts' `api_code` - also with one channel.
3. **Errors follow Centrifugo's model.** A Centrifugo error is `Error::Api`,
   matched by code alone; an unknown code is still one. The transport error
   mode gives the same error as a 200 reply. A reply which is not a JSON
   object of the reply's shape is `Error::Decode`.
4. **One deadline per call.** `request_timeout`, or the call's own
   `with_timeout`, which replaces it, from the call's start; a `with_deadline`
   only shortens it, never extends it (spec 3.2). It bounds the whole call -
   the endpoint and token functions, the connection, the reply read - through
   `tokio::time::timeout_at`, so a reply trickling in stops at the deadline.
   Every timeout is `Error::Timeout`, an `HttpClient`'s own included.
   `broadcast_with_retry`'s attempts each time out after `attempt_timeout`
   or `request_timeout`, the call's deadline (`with_timeout` from its start,
   `with_deadline`) ending them sooner. The `*_all` pagers take no per-call
   timeout - a handle's `with_timeout` does not apply to them - and each page
   times out after `Pager::page_timeout` or `request_timeout`. An automatic
   batch request lasts until the latest of its calls' deadlines, at least
   `request_timeout`.
5. **A payload is carried exactly.** Payloads are `Box<RawValue>` in and out:
   their text, number text included, is sent and returned as it is. JSON
   `null` is `Some` of a null.
6. **A caller's `HttpClient` is never changed**: credentials and headers go on
   each request.
7. **Automatic batching never delays a call below `max_in_flight`**, never
   gives a caller a reply which is not its own, never sends a call whose caller
   gave up - timed out, or dropped its future - while it queued, and `close`
   waits for batches whose callers left. Batches are sent from tasks of the
   client's runtime.
8. **The blocking client is the async one.** It runs `crate::Client` on a
   runtime of its own: a change of behaviour is made once, and
   `tests/blocking.rs` and the generated `tests/methods.rs` run both.
9. **A handler answers with exactly one of result, error and disconnect,** as
   its proxy type can carry them; anything else is a 500 reported to
   `on_error`.
10. **No unsafe code**, and no type of a pre-1.0 dependency in the public API
    but `http`'s and `bytes`' 1.x types and serde_json's `RawValue`.

## Running things

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo xtask generate --check
```

The integration tests need a Centrifugo set up as `testdata/` says:

```sh
docker compose -f testdata/centrifugo-pro.compose.yml up -d --wait
CENTRIFUGO_TESTS=1 RUSTCENT_TEST_PRO=1 cargo test -p cent --test integration
```

They serve the proxy handlers on port 3100 and an API JWKS on 3101 of the host,
which Centrifugo reaches as `host.docker.internal`. CONTRIBUTING.md lists the
rest of what CI runs.

## Updating to a new Centrifugo API

1. `cargo xtask sync-proto vX.Y.Z` - always a released tag.
2. Read the diff of `proto/`: a removed field is a breaking change. Keep it as
   a deprecated field until the next breaking release (spec 16).
3. If generation stops on `required_fields.txt` or `constructors.txt`, review
   the change, then `cargo xtask generate --accept-required`.
4. The checks, then the integration tests against the new release.
5. Bump the Centrifugo image in `.github/workflows/ci.yml` and
   `testdata/centrifugo-pro.compose.yml`, and the versions table in the README
   when a feature needs the new release.

## Things that look wrong but are deliberate

- **Requests are `#[non_exhaustive]` structs with `new` and setters**, not
  struct literals: a field the protos add is not a breaking change, and `new`
  takes the fields marked "Required." (spec 3.3).
- **Payloads are `Box<RawValue>`**, not `serde_json::Value` or generics: the
  spec's rule 7 for Rust - one JSON type in and out, exact - and no generic
  parameter on every request. `cent::json` and `PayloadExt::decode` convert
  serde types; their failures are `Error::Payload`, not `Error::Decode`, which
  means a reply not Centrifugo's.
- **`BoolValue` and `Int32Value` fields are `Option<bool>` and `Option<i32>`**:
  `None` leaves the server's setting alone, which `false` cannot say. No
  helper is needed.
- **Every message field is an `Option`, and `Some(empty)` is not sent** but
  for the presence wrappers of spec 2 - `device_update`'s `user_update`,
  `timezone_update`, `locale_update`, `meta_update` - which are sent whenever
  `Some`, even empty: their empty value has a meaning. Any other empty
  message is left out (`crate::wire::absent`, from the generated
  `wire::Empty`): Centrifugo refuses `"limit_strategy": {}`.
- **Required fields are both arguments of `new` and checked before
  sending** (spec 3.3, 3.5): the signature makes the caller pass a field,
  but `new("")` passes it empty, which on the wire is missing. The generated
  `validate::Required` gives the path of the first one without a value;
  those whose empty value has a meaning are not checked.
- **`*_all` return a `Pager` with `async fn next`**, not a
  `futures::Stream`: the futures crate is pre-1.0 (invariant 10). The
  blocking client's are `Iterator`s.
- **The constants are `&str` consts, the fields `String`s**, not enums:
  Centrifugo adds values, and one without a constant must still be
  sendable. API constants are at the crate's root, proxy ones in
  `cent::proxy`. Their names are the spec's pinned table (section 3.3),
  held in `xtask/constants_api.txt` and `xtask/constants_proxy.txt`, not
  derived from the protos: the same field has the same name in both protos
  (`FILTER_NODE_OP_AND`, `proxy::FILTER_NODE_OP_AND`), and a message added
  upstream never renames one.
- **Maps are `BTreeMap`**: requests encode the same way every time.
- **`RPCRequest` is `RpcRequest`** in Rust, as Rust names acronyms; the wire
  names do not change.
- **Per-call timeouts and deadlines by handle**:
  `client.with_timeout(d).publish(...)` is the call's own timeout, replacing
  `request_timeout`; `client.with_deadline(t)` is the deadline it runs under,
  which only shortens it - the two kinds of bound of spec 3.2. A future's own
  `tokio::time::timeout` ends a call too, but the client's lets it report the
  timeout and lets an automatic batch know how long its calls wait.
- **`Batch::send` takes the batch**: adding to, or sending, a sent batch does
  not compile, so the spec's `BatchSent` error does not exist.
- **The async client refuses another runtime** (spec 3.1): its pooled
  connections and batch tasks belong to the runtime of its first call. After
  a fork, both clients start anew - connections, runtime binding, and the
  blocking client's runtime.
- **The blocking client spawns each call on its runtime and waits on a
  channel**, rather than `block_on`: it then works from any thread, an async
  one included, and a panic in a hook is resumed in the caller.
- **`on_attempt_error` returns a `Retry`** - the default wait, a wait, or
  stop (spec 7.3's form for exceptions): synchronous, so the async and
  blocking clients share it, and a callback which only logs cannot stop the
  retries by accident.
- **`impl<E: std::error::Error> From<E> for Rejection`**: `?` on any error
  fails a proxy request, so `proxy::Error` and `proxy::Disconnect` stay plain
  values - never `std::error::Error`s, whose impl would conflict - and a
  `BoxError` goes through `Rejection::fail`.
- **No `rpc`**: it calls server API extensions, which Centrifugo does not
  provide.
- **No `batch_context`** (spec 10): Rust has no request-scoped context values
  to carry into a batch; `on_request` and an `HttpClient` see every request.
- **aws-lc-rs for all cryptography**: rustls' default provider, and the one
  with P-521 for ES512. HMAC, SHA-256 and the random idempotency keys use it
  too, so there is one crypto library.
- **The default `HttpClient` ignores `HTTP_PROXY`**: calls to Centrifugo are
  usually internal; an `HttpClient` of the application's goes through a proxy.
- **`Handler` takes `http::Request<Bytes>`**, the body read: the body limit is
  checked in the core, and the tower adapter reads up to it.
- **The test certificate in `tests/testdata/` is committed**: generating one
  needed a dependency whose `time` crate's fixed version is past the MSRV.
- **An oversized handler body is a 400**, as in gocent, not a 413.
