# Contributing

## Design first

Behaviour is specified in the [Centrifugo server SDK
specification](https://github.com/centrifugal/gocent/blob/master/spec.md),
which every Centrifugal server SDK follows, and the choices particular to Rust
in [AGENTS.md](AGENTS.md). When the code and the spec disagree, one of them is
a bug.

## Local checks

These run in CI on every pull request:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo xtask generate --check
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc -p cent --no-deps --all-features
cargo hack check -p cent --feature-powerset --no-dev-deps   # cargo install cargo-hack
cargo +1.85 check -p cent --lib --all-features             # the MSRV
cargo deny check                                           # cargo install cargo-deny
typos                                                      # cargo install typos-cli
```

### Test servers

Most tests need no server: they run against a fake Centrifugo
(`crates/cent/tests/common`) and small local servers. The integration tests
run against Centrifugo `v6.10.2`, and are skipped unless enabled:

```sh
docker compose -f testdata/centrifugo-pro.compose.yml up -d --wait   # Centrifugo PRO, Redis, PostgreSQL
CENTRIFUGO_TESTS=1 RUSTCENT_TEST_PRO=1 cargo test -p cent --test integration
docker compose -f testdata/centrifugo-pro.compose.yml down
```

Against Centrifugo OSS - as CI's `integration` job runs it - the PRO tests
skip. The tests serve the proxy handlers on port 3100 and an API JWKS on 3101
of the host. Centrifugo PRO runs in sandbox mode, without a license key; it is
commercial software under the [Centrifugal Labs License
Agreement](https://centrifugal.dev/license).

### The automatic batching stress test

`tests/autobatch.rs`'s `stress` runs 20 rounds by default; CI runs 500:

```sh
RUSTCENT_STRESS_ROUNDS=5000 cargo test --release -p cent --test autobatch stress
```

## Layout

- `crates/cent`: the SDK, the crate `cent`.
- `xtask`: maintainer tasks (`cargo xtask generate`, `cargo xtask sync-proto`).
- `proto/`: Centrifugo's protos, from a released tag.

## Commits and pull requests

- PRs are squash-merged. **The PR title becomes the commit message**, and it must follow [Conventional Commits](https://www.conventionalcommits.org) (checked in CI):
  - `feat: ...` → minor version bump before 1.0 (and a changelog "Added" entry)
  - `fix: ...` → patch bump ("Fixed")
  - `feat!: ...` or a `BREAKING CHANGE:` footer → breaking bump
  - `perf`, `refactor`, `docs`, `revert` → changelog entries; `test`, `ci`, `build`, `chore` → no entry
- Each bug fix comes with a test that fails without the fix.

## Releases

As in the other Centrifugal SDKs: bump the version, then tag. A release can include any number of merged PRs.

1. **Bump the version** in a normal PR (or commit) to `main`, titled `chore: bump to X.Y.Z`: `version` in `[workspace.package]` of the root `Cargo.toml`, the `User-Agent` in `crates/cent/src/client.rs` when the minor version changes before 1.0 (a test checks it), and the `CHANGELOG.md` entry. Run `cargo check` to update `Cargo.lock`. Bump by semver: before 1.0, a breaking change bumps the minor version (`0.1.x` → `0.2.0`); anything else bumps the patch.
2. **Wait for CI** to pass on `main`.
3. **Tag it.** Either create the release on GitHub (Releases → Draft a new release → new tag `vX.Y.Z` on `main`, write the notes, publish), or push a tag, `git tag vX.Y.Z && git push origin vX.Y.Z`, and let the workflow create the release with generated notes. Edit the notes afterwards if needed.
4. The tag runs `.github/workflows/release.yml`. It checks that the tag names the crate's version, points at a commit on `main` that passed CI, and that the bump matches the API change (`cargo-semver-checks`). Then it publishes the crate to crates.io through **trusted publishing** (OIDC, no long-lived token).

A version already on crates.io is skipped, so rerunning the workflow after a failure finishes the release. If the semver check fails, the bump was too small: delete the tag and the release, bump again, and retag. Nobody runs `cargo publish` by hand, except for the first publish below.

### Release setup (one time, maintainers)

1. **First publish.** crates.io trusted publishing can't create a new crate, so publish `0.1.0` once by hand:
   - Create a crates.io API token with only the `publish-new` scope, limited to the crate `cent`, expiring in a day.
   - From a clean checkout of the pushed `main` commit: `cargo publish -p cent`.
   - Revoke the token.
   - Create the `v0.1.0` release on GitHub, with the `0.1.0` section of `CHANGELOG.md` as its notes. The release workflow finds the crate published and has nothing else to do.
2. **Trusted publisher.** On crates.io, for `cent`, go to Settings → Trusted Publishing and add GitHub: owner `centrifugal`, repository `rustcent`, workflow `release.yml`, environment `release`. After the first release through CI works, also turn on **Trusted Publishing Only** mode there, which rejects token publishes.
3. **GitHub environment.** Create an environment named `release` (Settings → Environments). Under deployment branches and tags, allow only tags matching `v*`. Optionally add required reviewers: they approve each publish.
4. **Tag protection.** Add a tag ruleset for `v*` (Settings → Rules → Rulesets) that only maintainers can bypass, so only they can create, move or delete release tags.
5. **Branch protection on `main`.** Require the `CI success` and `Conventional commit title` checks. Allow squash merges only.
6. **Codecov** (optional). Install the Codecov GitHub App for the repository. Uploads use OIDC, so no token is needed.
