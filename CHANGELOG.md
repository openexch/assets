# Changelog

All notable changes to `assets` (the Open Exchange Assets Engine — the
deterministic money ledger) are documented here. The stack (`match`, `oms`,
`admin-gateway`, `trading-ui`, `assets`) is versioned together; one version
spans all five repos.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.5.0-beta] - 2026-07-27

The durability release: the ledger's state stops living only on the box that
produced it, and the money path gets 24.5x faster on the way. Snapshots are
captured as bundles with the log behind them, the archive is reclaimed instead
of growing forever, and snapshot bytes are now a function of the ledger's state
rather than of the history that reached it — without which a bundle could not
be verified at all.

### Added
- Bundle capture: a snapshot plus the log behind it, as one durable unit
  (#26), with a retention watermark the purge honours (#27) and an ephemeral
  control port so a capture cannot collide with the live archive (#28).
- Archive housekeeping, so an AE snapshot actually reclaims disk (#25).
- Per-session egress subscription: a session declares which channels it reads
  (#21). A session that does not subscribe still receives everything, so no
  existing consumer changed.
- `assets-loadgen`, the money-path benchmark driver with no OMS in the loop:
  it performs the hold gate exactly as the OMS does, and every latency is
  measured on its own clock from the scheduled send time (#13). Multi-generator
  runs via `--gen-id` (#16), an `--ae-only` substrate mode (#19), and an
  isolated ME leg (#22).
- Settlement-latency histograms and replay-lag gauges on the bridge (#14).
- The engine, the bridge and the load generator are published to the artifact
  store, addressed by commit sha and by release tag (#34, #37). The bridge had
  no published build at all before this: it sits between the ME journal and the
  AE ledger, so "which bridge applied these settlements" was unanswerable.

### Changed
- The cluster machinery is consumed from `cluster-kit` rather than kept as a
  second copy (#29, #30, #34).
- **Money path 4.4k → 120k orders/s**, and not saturated there. Egress moved
  from an inline per-session offer — which parked the deterministic thread on
  one back-pressured session, stopping the whole engine — to a queue drained in
  bulk off the critical path (#17), then to per-session filtering, which cut
  the bridge's share of the traffic to a fifth (#21).

### Fixed
- **Snapshot bytes were a function of history, not of state.** Accounts and
  holds were serialized in hash-table order, which depends on which users were
  touched first and which holds were released; a node replaying from genesis
  and a node resuming from a snapshot wrote different bytes for an identical
  ledger. Harmless to the ledger, fatal to verification: bundle replay is
  checked by comparing snapshots, so it would have raised divergence alarms for
  a ledger that had not diverged. One canonical ordering primitive fixes it;
  the format is unchanged and old snapshots still load (#36).
- A credit that would wrap a balance negative is rejected. `deposit` added
  without an overflow check, so past `Long.MAX` a balance silently went
  negative — no log, no fault, and the conservation check still passed (#20).
- `FixedPoint` in this repo had no guard tests; the multiply overflow guards
  are ported so both copies are held to the same standard (#31).
- The bridge resumes its chain walk instead of restarting it at the head (#24).
- Cross-host journal archive follow uses a routable replay host (#15).
- The publish job built the wrong modules and referenced an undefined
  timestamp (#35).

## [0.4.0-beta] - 2026-07-22

First tagged release, joining the stack at the coordinated version per the
stack-alignment convention.

### Added
- The Assets Engine: deterministic money ledger on Aeron Cluster (Phases
  0-2a); v2 engine/domain behavior; money-schema v2 — correlation ids, feed
  messages, snapshot queries, cutover primer; determinism corpus extended
  for v2 behaviors.
- Feed routing: Settle/TerminalRelease via SettlementProjector +
  FeedPositionReport.
- Settlement bridge: journal→AE feed with stateless resume and gap-halt
  (D2); assets-bridge module + pure journal→money translator.
- Money journal Part 1: replayable per-node archive recording of applied
  money movements (#9); Part 2: `assets-projector` live-follows the AE money
  journal into Postgres (#11).
- Bridge metrics endpoint — `/metrics` + `/health` (#1); `AeDump` read-only
  AE ledger snapshot CLI for money-check (#3).
- CI build + security workflow (#8).

### Fixed
- settle-shortfall never throws — graceful drawdown + SettleFault event
  (D5).
- Bridge detects journal-source death instead of idling at a dead
  recording's EOF (#4).
- Hold release ownership — omsManagedRelease gates feed terminals (D6b).
- Settle reaps exhausted holds — no remaining=0 tombstones (#6).
- Loud warning when the AE state dir is tmpfs (#2).
