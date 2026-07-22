# Changelog

All notable changes to `assets` (the Open Exchange Assets Engine — the
deterministic money ledger) are documented here. The stack (`match`, `oms`,
`admin-gateway`, `trading-ui`, `assets`) is versioned together; one version
spans all five repos.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
