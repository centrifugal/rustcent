# Changelog

Release notes are on the [releases page](https://github.com/centrifugal/rustcent/releases).

## [0.2.0]

Implements the [Centrifugo server SDK
specification](https://github.com/centrifugal/gocent/blob/master/spec.md) 0.3.

The protos are synced from Centrifugo's `master`, which has the `One of:`
sentences and `ConnectResult.profile` before any release does: they must be
synced again from a released Centrifugo tag before 0.2.0 is published.

### Changed

- **Breaking:** a field the protos mark "Required." is checked for a value
  before sending: `new("")`, a zero number, an empty list or an empty
  message fails with `Error::InvalidRequest` naming its path -
  `channel is required`,
  `limit_strategy.rate_limit.policies[0].rate is required` - from a call, a
  batch (failing the batch before sending, as before) and a `Command`. A
  required argument of `new` made the caller pass the field, not give it a
  value, and an empty field is a missing one on the wire. Nested messages
  are checked when they are sent, list elements each. Not checked: the
  fields whose empty value has a meaning (`DeviceUserUpdate::new("")`
  detaches the devices), and the fields "Required unless ..." or "Required
  when ...". The checks are generated from the protos and are no stricter
  than Centrifugo PRO's own.
- **Breaking:** a message field set to an empty message is no longer sent,
  as when it is `None`: `.limit_strategy(PushLimitStrategy::new())` sent
  `"limit_strategy": {}`, which Centrifugo refuses with code 107 for want of
  `expire_at`. The presence wrappers are still sent whenever set, even empty:
  `BoolValue` and `Int32Value` fields, and `device_update`'s `user_update`,
  `timezone_update`, `locale_update` and `meta_update`, an empty one of which
  clears the timezone, locale or meta or detaches the devices from their
  user. No type changes.
- **Breaking:** `request_timeout` bounds every call, and `with_deadline`
  only shortens a call, never extends it. In 0.1 a handle's deadline
  replaced `request_timeout`, so a call under a deadline minutes away could
  wait that long on a stuck Centrifugo; it now gives up after
  `request_timeout`. To let calls take longer, raise `request_timeout`, or
  give them a timeout of their own: `with_timeout` still replaces
  `request_timeout` for a call, longer or shorter. A handle with both keeps
  both - `with_timeout` no longer drops a deadline set before it, and a
  second `with_deadline` keeps the earlier one. The blocking client is the
  same.
- **Breaking:** `broadcast_with_retry`'s `attempt_timeout` is each attempt's
  own timeout, replacing `request_timeout` for it - it was capped by the
  call's deadline, which by default was `request_timeout` for the whole
  call. Each attempt times out after `attempt_timeout` or `request_timeout`;
  the call's deadline - `with_timeout` from its start, or `with_deadline` -
  bounds them all, waits included; without either, `max_attempts` and the
  attempts' timeouts bound the call.
- An automatic batch request lasts until the latest of its calls' deadlines,
  and at least `request_timeout` from when it is sent, as before: it may
  outlast a call's deadline, which the docs now say.
- **Breaking:** `endpoints::Unauthorized` is renamed `endpoints::Forbidden`:
  the 403 of the token and shared poll endpoints, which client SDKs report
  as their `UnauthorizedError`. The old name read like the API client's
  `Error::is_unauthorized` - Centrifugo refusing this backend's credentials
  - with the opposite meaning. Its `Display` is `forbidden`.
- The `User-Agent` is `rustcent/0.2`.
- The new fields of `proxy::ConnectResult`, `token::ConnectionClaims` and
  `token::SubscriptionClaims` are on `#[non_exhaustive]` structs, built with
  their constructors.

### Added

- Constants for every string field with a fixed set of values, generated
  from the protos' `One of:` sentences and named as the spec's table pins
  them: `OP_ADD`, `PROVIDER_FCM`, `PLATFORM_IOS`, `STATUS_DELIVERED`,
  `KEY_MODE_IF_NEW`, `CMP_EQ`, `FILTER_NODE_OP_AND` and the rest at the
  crate's root; `proxy::ENCODING_BINARY`, `proxy::PROTOCOL_JSON`,
  `proxy::ALLOW_SUB`, `proxy::CAPABILITY_MATCH_WILDCARD`,
  `proxy::CHANNEL_EVENT_TYPE_OCCUPIED`, `proxy::FILTER_NODE_OP_AND` and the
  rest for the proxy. The fields stay strings.
- `history_all`, `device_list_all`, `device_topic_list_all`,
  `user_topic_list_all` and `map_read_state_all` read every item of a paged
  method, a page at a time: a `Pager` with `async fn next` on the async
  client, an `Iterator` on the blocking one. `history_all` refuses a `limit`
  below 1 and `reverse` before sending. `history_all` yields `HistoryItem`s,
  each publication with its `position()`, and `map_read_state_all`
  `MapStateItem`s, each entry with the `state` position it was read at; both
  deref to the publication or entry. `map_read_state_all` reads every
  page after the first at the first one's revision, unless the request sets
  one, so that a map state reset between pages fails with code 112 instead
  of mixing two states.
- `position()` on every result holding a stream position: `PublishResult`,
  `HistoryResult`, `MapPublishResult`, `MapRemoveResult`,
  `MapReadStateResult` and `MapReadStreamResult` - the `StreamPosition` to
  read on from.
- `ConnectionClaims::profile`, the Centrifugo PRO compression profile.
- `proxy::ConnectResult::profile` (Centrifugo PRO newer than v6.10.2).
- The docs of `device_update`'s `user_update`, `timezone_update`,
  `locale_update` and `meta_update` say an empty one is sent, and clears
  or detaches - as rustcent already sent it.

### Fixed

- Docs: the redirect hint no longer says Centrifugo never redirects, and
  names the endpoint's path;
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
