# Settlement replay source isolation

## Problem and fix

On 2026-09-10 the demo bridge changed archive source `9010 -> 9110 -> 9010`.
Both sources exposed active recording `(2,0,-1)`, but their byte histories differed.
Node1's remembered `81464704` was inside a valid node0 frame (`81464576..81464736`).
The resulting future-position/invalid-frame retry loop was not evidence of journal corruption.
An independent empty-recording/EOF bug was also reproduced; it was not the direct cause of this outage.

`ChainResumeMemo` now scopes both the drained-recording prefix and skipped-byte positions to:

* Archive control endpoint, control stream, journal stream and channel.
* Catalog recording id/start and incarnation: `startTimestamp`, `initialTermId`, `sessionId`,
  `termBufferLength`, `mtuLength`, `streamId`.
* Monotonic AE `consumePosition` AND `lastAppliedTradeId`.

Changing source discards all prior memo state (there is no per-source cache to resurrect on A/B/A).
Any changed, removed or reordered catalog history discards both proofs. Normal append and
active-to-stopped transitions preserve the existing byte proof; changing a stopped extent does not.
Either AE watermark regressing clears both, including a return to genesis. Advancing watermarks
preserves the proofs: already applied trade ids and terminals below the previous watermark remain applied.
Legacy three-argument `Recording` construction remains source/binary-constructor compatible, but without
incarnation metadata the bridge cannot enable memoization. The projector's existing API still compiles.

`sessionId` may change when an Aeron recording is extended; including it conservatively rescans that
history. `archiveId` is not a durable generation identifier (it can be configured/reused), so it is not
used as proof. Metadata cannot detect offline replacement of segment bytes while every catalog field
is preserved, nor an AE history replacement that returns indistinguishable watermarks between syncs.
Such operations need an explicit generation protocol or a bridge restart and reconciliation; they are
not made safe by treating byte offsets as application checkpoints.

The byte mark advances only over each recording's uninterrupted SKIP prefix **before any forward,
staging, halt or unknown entry in that recording**. A later recording may prove its own byte prefix
after an earlier inclusive terminal was forwarded; the whole-chain drained prefix stays sealed.
Trade byte proofs additionally require `tradeId <= epoch-initial AE lastAppliedTradeId`. A duplicate
of a trade staged earlier in the epoch can SKIP without AE having applied it, including across recording
boundaries. Sealing and the fixed AE trade watermark prevent that pending event from becoming a
checkpoint. The separate byte proof preserves catch-up when an earlier recording's inclusive terminal
is re-sent on every epoch (the incident's 96-byte stopped-recording remainder). Inclusive terminal watermark replay remains intact;
AE idempotency handles that boundary. Neither `BridgeFilter`, settlement logic nor `haltOnGap` changes.

Empty stopped recordings join the drained prefix. A trusted byte mark at stopped EOF advances the
chain without a zero-length replay, including an entirely exhausted chain followed by a later successor.
An active frontier remains an unbounded live replay. Stopped completion requires the image's actual
position to reach the catalog EOF; premature EOS/closure cannot certify a drained prefix.
Malformed bounds and invalid frame offsets remain visible errors. If the archive rejects a remembered
offset with Aeron's `INVALID_POSITION` code, both memo proofs are invalidated and that error is propagated; the next epoch starts from the
catalog start and fresh AE sync. No bytes are skipped to recover, and actual archive corruption is still
reported. Unrelated resource/session replay failures preserve the memo. `connectFirstHealthy` continues to check connectivity only; this is not a new failover policy.
Logs report source/catalog identity and invalidation reasons, without journal/account payloads.

## Build provenance and compatibility

The live reference is `origin/fix/bridge-byte-resume`, commit
`5c6471e4425c425f3ffadee4c400026c456a6eb1`, jar SHA256:

```
aed96ef6a02f04708b630e5228a2281972e0d013fa9ac24c660472824bc9d0d5
```

The review branch starts from fetched main `aabee7a`. Its source changes since the initial local
`3ef0fdf` checkout are dependency/CI updates only. This patch carries the deployed byte-resume
(`172b0a4`) and AE ingress `mtu=8192` (`af85048`) behavior. Main already contains empty-recording #89;
this patch completes its prefix/EOF handling. Main readiness/snapshot fixes are retained.
No cloud-console rebuild or taskset change is included.

Main pins Aeron 1.53.0, Agrona 2.6.0, SBE 1.40.1, cluster-kit 0.1.7 and
match-common `1.0.20260909213922.g7f85ca5`. The live substrate is Aeron 1.52.2/Agrona 2.5.0/SBE 1.39.0.
Both test configurations are recorded below; a rollout build must record its chosen dependency set.

The settlement XML and committed generated journal tree are identical from deployed match `f873fed`
through main pin `7f85ca5` (XML blob `c62f0adcd588c233b64735d190055960460527db`, generated tree
`a7bc6bab13efc5d13710a4bf44faaf8d0017b74c`). Assets money XML and committed generated tree are likewise
unchanged between deployed bridge base and current main. No wire/schema file is part of this patch.
The baseline repository contains stale checked-in money codecs: ordinary Maven generation emits its
existing v5 schema. Generated build drift is excluded from the review diff; it is not a schema change.

Local validation uses an isolated Maven cache. Uncached private artifacts are built from exact source
snapshots, **not** relabelled old jars: cluster-kit `2b797fb215e8f60a35b6c41ca46f6846b6e21550` and
match `7f85ca51a320985530a2fdb3b93a14c51248dbab`. These are exact-source test builds, not assertions of
byte identity with GitHub Packages artifacts. CI resolves the normal immutable package coordinates.

## Validation

Both full Maven `clean verify` runs passed: current main dependencies and live substrate
(`-Daeron.version=1.52.2 -Dagrona.version=2.5.0 -Dsbe.version=1.39.0`). Java 21.0.11, Maven 3.8.7.
Each run reports **370 tests: 369 passed, one expected PostgreSQL integration skip** because
`PROJECTOR_PG_TEST_URL` is unset. The loadgen compiles/packages/verifies and has no test classes.

| Module | Tests reported | Failures/errors | Skipped |
| --- | ---: | ---: | ---: |
| assets-common | 45 | 0 | 0 |
| assets-cluster | 241 | 0 | 0 |
| assets-bridge | 72 | 0 | 0 |
| assets-projector | 12 | 0 | 1 |

Bridge coverage includes 27 memo transition tests, four real-handler prefix-safety/liveness tests,
and nine real Aeron/AE end-to-end tests. The latter assert actual queried AE watermarks, dense
ordered `SettlementApplied` acknowledgements, exact residual balances and zero `SettleFault`s.
They cover A/B/A with identical recording ids and different real frame layouts, repeated same-source
byte resume, empty/stopped/active/successor chains, premature stopped EOS, rejected invalid byte
memo, preserved memo on `MAX_REPLAYS`, and 129-trade batch catch-up followed by six seconds idle
with a five-second session timeout and further successful settlement on the same epoch.
The earlier-inclusive-terminal case verifies that a later recording's 512 applied entries are memoized:
SKIP counts across three epochs are `512 -> 1025 -> 1025`, followed by successful new settlement.
Cross-recording duplicates of trades staged but not acknowledged cannot become byte checkpoints.
Four real archive tests in common cover incarnation capture, stop/reconnect, same-endpoint archive
recreation/id reuse, actual invalid frames, strict ranges and active-frontier replay.

The unchanged deployed repro harnesses were rerun: both the cross-source invalid-frame bug and
independent empty-prefix/EOF bug reproduce. SBE 1.39.0 and 1.40.1 generated all 68 money codec
Java files byte-for-byte identically. No schema or generated file is included in the patch.

[Exact commands and real output excerpts](bridge-source-resume-validation.txt) include the module
summaries, original reproductions and non-final setup failures. Initial offline/download failures
were resolved without changing dependency versions. An overlapping local test run used the same
fixed Aeron fixture ports/directories and is excluded; both final full runs were serial and passed.
`git diff --check` passed. The CI-equivalent Trivy 0.70.0 gate (the default for
`aquasecurity/trivy-action@v0.36.0`) passed with both vulnerability and secret scanners enabled:
`--severity CRITICAL,HIGH --exit-code 1 --ignore-unfixed`. It found zero qualifying vulnerabilities
and zero qualifying secrets. The official database is dated `2026-09-10T07:06:15Z`; its complete OCI
layer digest was verified before scanning. `--skip-db-update` uses this manually fetched fresh database;
it does not suppress either scanner or any finding. The scan uses tracked and nonignored review files,
with generated codecs restored from the committed baseline and build outputs/cache excluded, matching
the source checkout scanned by CI. Sanitized results and exact commands are included in the validation evidence.

## Rollout and rollback (not executed)

1. Build off-host from the reviewed/signed revision, stamp `Build-Sha`, retain dependency versions and
   SHA256. For a narrowly scoped live hotfix, use the tested live substrate overrides shown in validation;
   do not implicitly upgrade the demo's Aeron substrate as part of the bridge restart.
2. Before replacing only the bridge jar, retain the exact running jar/hash above and capture a short
   baseline: source/recording identity, `consumePosition`, `lastAppliedTradeId`, settlement ack watermark,
   forwarded trades, epochs/errors, backlog and `settlement_flowing` canary. Verify journal retention
   contains the next unapplied trade plus intervening terminals. Preserve AE/ME state and archive files.
3. Stop the old bridge, install the verified new jar and start one bridge instance. Observe initial
   catch-up for at least 15 minutes: actual AE trade/apply and ack watermarks must advance, settle order
   must remain dense, terminal residual releases/balances must reconcile, backlog should drain, and
   epoch/error counters should stabilize. `connected=true` or forwarded counters alone are insufficient.
4. Exercise controlled source switching first in staging with matching logical histories and different
   byte layouts. In demo, measure continuity across the next observed source change; any deliberate
   failover exercise is a separately approved operation. Expect one invalidation per source/history
   change, no repeated invalid-offset loop, no duplicate economic effect, canary recovered and sustained.
5. On gap/halt, unexplained AE watermark regression, repeated replay errors, missing apply progress or
   unhealthy canary, stop rollout and retain diagnostics. Restore the saved bridge jar and restart only
   the bridge if rollback is needed. Rollback restores the old source-alias bug: its fresh in-memory memo
   may temporarily resume, but source switching remains unsafe until this fix is restored. Reconcile
   from AE watermarks; never wipe/reset/genesis or disable the gap guard as a rollback step.

At incident measurement, `/dev/shm` was 74% full with about 2 GiB free, RAM available was 760–805 MiB,
and two OOM kills were recorded. AE was 3/3 healthy; ME node2 lagged. These are separate rollout capacity
and ME recovery risks requiring their own follow-up. They do not prove node0's first restart was due to
SHM/OOM. Do not build on the constrained demo host, blindly clean SHM or reset the cluster. The separate
cloud-console AE taskset fix must be present before any future admin rebuild; no such rebuild is needed
for this bridge patch.
