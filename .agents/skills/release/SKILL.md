---
name: release
description: Prepare and publish a new kafka-rust release. Use when the user asks to release, publish, tag, dry-run, or validate a new version of rustfs-kafka and rustfs-kafka-async.
---

# kafka-rust Release

Use this skill for new releases of the `houseme/kafka-rust` workspace. The workspace publishes two crates:

- `rustfs-kafka`
- `rustfs-kafka-async`

The async crate depends on `rustfs-kafka`, so publish `rustfs-kafka` first and wait for the crates.io index before publishing `rustfs-kafka-async`.

## Required Input

- Exact target version, for example `1.3.1`.
- Desired scope: local preparation, dry-run validation, or final publication.

If the target version is missing or ambiguous, inspect the latest tags and current workspace version, then ask the user to confirm the exact version before editing files or creating tags.

## Release Files

For a normal version bump, inspect and update only the files that actually carry release information:

- `Cargo.toml`
- `Cargo.lock`
- `CHANGELOG.md`
- `README.md`
- `docs/usage-guide.md`
- `crates/rustfs-kafka-async/README.md`
- `AGENT.md`
- `crates/rustfs-kafka/Agent.md`
- `crates/rustfs-kafka-async/Agent.md`

Keep the diff minimal. Do not include unrelated source edits, IDE files, worktree metadata, generated target files, or dependency drift that is not required for the release.

## Preflight

Before making changes or publishing:

- Read `AGENT.md`, the relevant crate `Agent.md` files, and the current release workflow at `.github/workflows/release.yml`.
- Run `git status --short --branch` and identify unrelated local changes. Preserve them and stage only release files.
- Fetch tags before making tag decisions: `git fetch origin --tags`.
- Confirm the current version from `[workspace.package].version` and the internal workspace dependency versions in `Cargo.toml`.
- Confirm both package versions with `cargo metadata --no-deps --format-version 1`.

## Version Preparation

- Set `[workspace.package].version` in `Cargo.toml` to the target version.
- Keep the workspace dependency versions for `rustfs-kafka` and `rustfs-kafka-async` aligned with the target version.
- Regenerate `Cargo.lock` only as needed for workspace package version changes.
- Update release notes in `CHANGELOG.md`. New release entries belong above older releases and below `Unreleased`.
- Update README, usage guide, and agent release-target references only when they mention the old version.
- Re-scan for stale version strings before validation.

## Local Validation

Use the lightest gate that proves the release state is coherent, then widen if the changed files or user request warrants it:

- `cargo metadata --no-deps --format-version 1`
- `cargo fmt --all --check`
- `cargo test --workspace`
- `cargo test --workspace --doc`
- `cargo build --workspace --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo package --package rustfs-kafka --allow-dirty`
- `cargo package --package rustfs-kafka-async --allow-dirty`
- `cargo publish --dry-run --registry crates-io --package rustfs-kafka --allow-dirty`
- `cargo publish --dry-run --registry crates-io --package rustfs-kafka-async --allow-dirty`
- `git diff --check`

The `integration_tests` feature requires a Kafka test environment. Do not treat a normal workspace release gate as an integration-test run unless the user explicitly asks for Docker integration validation.

## Git and PR Delivery

When the user asks to commit and push:

- Inspect the final diff before staging.
- Stage only release-related files.
- Use an English commit message such as `chore(release): prepare v1.3.1`.
- Include the standard co-author trailers required by the user's repository workflow.
- Push to the configured upstream branch or create a scoped release branch when appropriate.

When the user asks for a PR, write the PR title and body in English and include the version, release scope, and validation evidence.

## Publication

The repository release workflow publishes from a version tag or manual dispatch:

- Preferred trigger: create and push a `vX.Y.Z` tag that matches the prepared workspace version.
- Manual trigger: run `.github/workflows/release.yml` with `version` set to `X.Y.Z`.
- Use `dry_run: true` first when the user asks to validate the pipeline without publishing.
- Final publication requires explicit user authorization immediately before pushing a release tag or running a non-dry-run workflow.

The workflow should:

- Verify both crate versions match the release version.
- Run the release build, test, and lint gates.
- Publish `rustfs-kafka`.
- Wait for the crates.io index.
- Publish `rustfs-kafka-async`.
- Create a GitHub Release from `CHANGELOG.md` or commit history.

## Post-Release Verification

After a real publication, verify and report:

- The pushed tag and commit hash.
- The GitHub Actions release run status and URL.
- The GitHub Release URL.
- crates.io availability for both packages at the target version.
- Any skipped, failed, or still-running checks.

If publication fails after one crate is already published, stop and report the exact partial state. Do not retry broad publication steps until the failure mode is understood.

## Output Contract

When using this skill, report:

- Target version.
- Files changed.
- Validation commands and results.
- Commit hash and push status when applicable.
- Tag, workflow run, GitHub Release, and crates.io status when applicable.
- Any release risk or manual follow-up still required.
