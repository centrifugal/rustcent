# Changelog

Release notes are on the [releases page](https://github.com/centrifugal/rustcent/releases).

## [0.2.0]

Implements the [Centrifugo server SDK
specification](https://github.com/centrifugal/gocent/blob/master/spec.md) 0.3.

The protos are synced from Centrifugo's `master`, which has the `One of:`
sentences and `ConnectResult.profile` before any release does: they must be
synced again from a released Centrifugo tag before 0.2.0 is published.

### Changed

- No call or type changes incompatibly. Spec 0.3's two breaking changes for
  typed SDKs do not apply to rustcent: `device_update`'s `user_update`,
  `timezone_update`, `locale_update` and `meta_update` were already
  `Option`s sent whenever set - an empty one as `{}`, clearing the timezone,
  locale or meta, or detaching the devices from their user - and the fields
  the protos mark "Required." were already arguments of `new`, so no request
  fails before sending which did not before.
- The `User-Agent` is `rustcent/0.2`.
- The new fields of `proxy::ConnectResult`, `token::ConnectionClaims` and
  `token::SubscriptionClaims` are on `#[non_exhaustive]` structs, built with
  their constructors.

### Added

- Constants for every string field with a fixed set of values, generated
  from the protos' `One of:` sentences: `OP_ADD`, `PROVIDER_FCM`,
  `PLATFORM_IOS`, `STATUS_DELIVERED`, `KEY_MODE_IF_NEW`, `CMP_EQ`,
  `FILTER_NODE_OP_AND` and the rest at the crate's root;
  `proxy::ENCODING_BINARY`, `proxy::PROTOCOL_JSON`, `proxy::ALLOW_SUB`,
  `proxy::MATCH_WILDCARD`, `proxy::TYPE_OCCUPIED` and the rest for the proxy.
  The fields stay strings.
- `history_all`, `device_list_all`, `device_topic_list_all`,
  `user_topic_list_all` and `map_read_state_all` read every item of a paged
  method, a page at a time: a `Pager` with `async fn next` on the async
  client, an `Iterator` on the blocking one. `history_all` refuses a `limit`
  below 1 and `reverse` before sending.
- `position()` on `PublishResult`, `HistoryResult`, `MapReadStateResult` and
  `MapReadStreamResult`: the `StreamPosition` to read on from.
- `SubscriptionClaims::client`, binding a subscription token to one
  connection, and `ConnectionClaims::profile`, the Centrifugo PRO
  compression profile.
- `proxy::ConnectResult::profile` (Centrifugo PRO newer than v6.10.2).
- The docs of `device_update`'s `user_update`, `timezone_update`,
  `locale_update` and `meta_update` say an empty one is sent, and clears
  or detaches - as rustcent already sent it.

### Fixed

- Docs: the Redis plus NATS broker keeps idempotency keys; the redirect hint
  no longer says Centrifugo never redirects, and names the endpoint's path;
  commands given apart from their method need the payload as a JSON object.

## [0.1.0]

The first release, implementing the [Centrifugo server SDK
specification](https://github.com/centrifugal/gocent/blob/master/spec.md) 0.2
for Centrifugo v6.

### Added

- `Client`, async on tokio, and `blocking::Client` with the same methods: every
  API method of Centrifugo v6.10.2 but `rpc`, Centrifugo PRO's included, with
  request and result types generated from `api.proto`.
- Payloads as `Box<RawValue>`, carried exactly, with `cent::json` and
  `PayloadExt::decode` to and from serde types; one deadline per call; the
  error model with hints, the transport error mode, `retryable`.
- `broadcast` with every channel's outcome, `broadcast_with_retry`, batches
  with typed pending results, automatic batching, `close`.
- Commands for async consumers.
- Connection, subscription and Centrifugo PRO API tokens (HMAC, RSA, ECDSA,
  Ed25519), JWKS, shared poll signatures.
- Proxy handlers, the endpoints client SDKs call and the JWKS endpoint, as
  framework-free `Handler`s and `tower::Service`s.
