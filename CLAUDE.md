# CLAUDE.md

See [AGENTS.md](AGENTS.md). It is the guide for working on this crate and
applies here in full: the invariants, how to run the suite, how to update to a
new Centrifugo API, and the list of things that look wrong but are deliberate.

Three points from it are worth repeating, because they are the ones most easily
lost:

- **Never edit `crates/cent/src/generated/` or `crates/cent/tests/methods.rs`.**
  Change `proto/` or `xtask/`, then run `cargo xtask generate`.
- **Every call returns an error unless it did everything asked.** The parts of
  a broadcast or batch always have an outcome each; when some failed, the
  error is `Error::BroadcastFailedChannels` or `Error::BatchFailedCommands`,
  holding them.
- **Automatic batching never delays a call below `max_in_flight`** and never
  gives a caller a reply that is not its own.

Run the checks of CONTRIBUTING.md before reporting a change as done, and the
integration tests with Centrifugo running for a change to the transport,
errors or batching.
