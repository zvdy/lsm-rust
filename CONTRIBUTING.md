# Contributing to lsm-rust

Thank you for your interest in contributing! This document explains how to
propose changes and what we expect from contributions.

## Ways to Contribute

- Report bugs or request features through [GitHub issues](https://github.com/zvdy/lsm-rust/issues)
- Improve documentation
- Submit bug fixes or new features via pull requests
- Review open pull requests

## Development Setup

You need a recent stable Rust toolchain (install via [rustup](https://rustup.rs/)).

```bash
git clone https://github.com/zvdy/lsm-rust.git
cd lsm-rust
cargo build
cargo test
```

## Before Submitting a Pull Request

All pull requests must pass CI. Reproduce the full gate locally with a single
command:

```bash
make check   # formatting, clippy, the test suite and the docs build
```

CI additionally runs the crate against its declared minimum supported Rust
version (`make msrv`), a license/advisory policy check (`make deny`),
`cargo audit`, coverage, and a release build. The `CI` job is the aggregate
status check: it passes only when every other job does.

Please also:

1. **Add tests** for any behavior change. Bug fixes should include a
   regression test that fails without the fix.
2. **Keep changes focused.** One logical change per pull request makes
   review faster and history clearer.
3. **Write clear commit messages.** Use a short imperative summary line
   (optionally with a [Conventional Commits](https://www.conventionalcommits.org/)
   prefix like `feat:`, `fix:`, or `docs:`), followed by a body explaining
   *why* the change is needed.
4. **Update documentation** (README, rustdoc) when behavior or public API
   changes.

## Pull Request Process

1. Fork the repository and create a branch from `main`.
2. Make your changes, following the guidelines above.
3. Open a pull request against `main`, filling in the pull request template.
4. A maintainer will review your PR. Address review feedback by pushing new
   commits to your branch.
5. Once approved and CI is green, a maintainer will merge it.

## Releasing

Releases are cut by pushing a tag; there is nothing to run by hand.

1. Update `CHANGELOG.md`, moving entries from `Unreleased` into a new version
   section.
2. Bump `version` in `Cargo.toml` and open a PR with both changes.
3. Once merged, tag the merge commit and push it:

   ```bash
   git tag v0.2.0 && git push origin v0.2.0
   ```

The release workflow then verifies the tag matches `Cargo.toml`, runs the full
gate, builds Linux and macOS binaries, publishes a GitHub Release with generated
notes, and publishes to crates.io when a `CARGO_REGISTRY_TOKEN` secret is
configured (it skips that step cleanly when it is not).

## Reporting Bugs

Use the bug report issue template. Include:

- What you did (ideally a minimal reproduction)
- What you expected to happen
- What actually happened
- Your platform and Rust version

## Reporting Security Issues

Please do **not** open public issues for security vulnerabilities. See
[SECURITY.md](SECURITY.md) for the private reporting process.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you agree to uphold it.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE) that covers this project.
