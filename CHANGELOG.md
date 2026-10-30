# Changelog

Release notes are on the [releases page](https://github.com/centrifugal/rustcent/releases).

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
