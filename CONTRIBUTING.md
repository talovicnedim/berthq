# Contributing

Thanks for helping! Bug reports, ideas and pull requests are all welcome.

## Getting started

```bash
pnpm install
cargo run -p berthq-cli -- start   # broker on :7331, data in ./data
pnpm dev                           # dashboard with hot reload on :5176 (proxies to :7331)
pnpm check                         # must pass before a PR is merged
```

## How the code is organized

```
crates/core/     Queues, consumers, delivery, retention and storage (Rust).
crates/server/   HTTP API, server-sent events, metrics, auth; serves the dashboard.
crates/cli/      The `berthq` binary: `start`, plus client commands built on the Rust SDK.
apps/dashboard/  SvelteKit dashboard (Svelte 5, TypeScript, Tailwind, shadcn-svelte), embedded
                 into the binary at build time.
sdks/            Client libraries: rust, node, python, go, php (with a Laravel queue driver).
docs/            spec.md (semantics and decisions), api.md (HTTP reference), guarantees.md.
```

Core has one module per concept:

- Queues and messages: `queues.rs`, `settings.rs` (queue settings, name checks), `publish.rs`
  and `dedup.rs`, `messages.rs` (browse, get, remove), `deadletters.rs` (requeue), `copies.rs`
  (requeued dead letters, private to one consumer).
- Delivery: `delivery.rs` (the consume API), `polling.rs` (long-polling), `take.rs` (handing
  messages to a consumer), `settle.rs` (ack, nack, extend, leases), `failure.rs` (retries, dead
  letters), `positions.rs` (`from`, start positions, what comes next).
- Consumers: `consumers/` (create, replay, remove, info, the consumer messages view).
- Housekeeping: `retention.rs` (floors, backlog, trimming), `tick.rs` (timers), `disk.rs` (free
  space, `--min-free-disk`), `stats.rs`, `backup.rs`.
- Storage: `tables.rs` (table definitions), `records.rs` (stored types), `log.rs` (storing and
  deleting messages), `pending.rs` (what a consumer is processing or retrying), `store.rs` (the
  writer thread), `tx.rs` (one write transaction), `load.rs` (startup), `memory.rs` (the only
  in-RAM state), `format.rs` (file format version).
- Plus `broker.rs` (the public handle), `ids.rs`, `time.rs`, `error.rs`, and `testing.rs` (the
  test harness).

All message changes go through `log.rs` and all pending changes through `pending.rs`, which keep
the indexes and counts in step.

The dependency direction is one-way: server → core, cli → server and the Rust SDK. Core knows
nothing about HTTP; SDKs only know the HTTP API.

## Guidelines

- **Simple over clever.** Plain functions and data; no abstraction until there's a third use.
- **Disk is the source of truth.** Messages and their indexes live in redb; memory holds only
  per-queue bookkeeping, so it stays flat however deep queues get.
- **Nothing is confirmed before it is on disk.** Changes run as jobs on the writer thread and
  answer after the commit.
- **Consistent naming across layers:** `queues_create` in core is `POST /v1/queues`,
  `berthq queue create`, `client.queues_create()` and `api.queues.create()` in the dashboard.
- **API changes** update `docs/api.md`, the dashboard's `src/lib/api/types.ts` and the SDKs'
  types in the same pull request.
- **Tests:** core logic is tested against a temporary database with an explicit clock, no
  sleeps. SDK tests run against a real broker; CI runs every SDK's suite.
- **Commits:** [Conventional Commits](https://www.conventionalcommits.org) (`feat: …`,
  `fix(core): …`).

## Releasing

1. Set the version in `Cargo.toml` (`workspace.package`), `sdks/node/package.json`,
   `sdks/python/pyproject.toml` and `sdks/python/berthq_client/__init__.py`.
2. Give `CHANGELOG.md` a `## <version> (<date>)` section.
3. Tag the commit (`git tag v0.1.0 && git push origin v0.1.0`). The release workflow refuses a
   tag that doesn't match those versions or the changelog, then pushes the Docker image, builds
   the binaries into a draft GitHub release and tags the Go SDK (`sdks/go/v0.1.0`).
4. Check the draft and publish it.
