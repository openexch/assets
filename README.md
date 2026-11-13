# core

The order and ledger core of Open Exchange: one Java Aeron Cluster service that runs the order lifecycle, risk checks, holds and the money ledger (balances, holds, settlement) as a deterministic, replicated state machine.

Status: nightly prerelease. Not for production use.

User documentation (architecture, configuration, operations): https://openexchange.dev/docs

## Where it fits in v1

v1 is five repositories:

| repo | role |
|---|---|
| [match](https://github.com/openexch/match) | matching engine, its own Aeron Raft cluster (Java) |
| **core** (this repo) | order and ledger core cluster (Java) |
| [cluster-kit](https://github.com/openexch/cluster-kit) | cluster machinery shared by both engines (Java) |
| [cluster-tools](https://github.com/openexch/cluster-tools) | C++ cluster client, ME forwarder, settlement bridge, projectors |
| [tools](https://github.com/openexch/tools) | probe and test tooling |

v1 has no user-facing gateway yet.

## Modules

| module | what it is | jar |
|---|---|---|
| `core-common` | shared kernel: money value objects and the SBE wire codecs, generated at build time from `src/main/resources/sbe/money-schema.xml` (schema id 2, version 7) and `money-journal-schema.xml` (schema id 3, version 1) | library |
| `core-cluster` | the engine: domain model, deterministic engine, Aeron cluster service. Entry point `com.openexchange.core.Main` | `core-cluster/target/core-cluster.jar` |
| `core-bridge` | Java settlement bridge: tails the matching engine's settlement journal and forwards trades and terminal order statuses into the cluster. Entry point `com.openexchange.core.bridge.BridgeMain` | `core-bridge/target/core-bridge.jar` |
| `core-projector` | follows the money journal (an Aeron Archive recording) and projects every money movement into PostgreSQL, resumable from the PostgreSQL high-water mark. Entry point `com.openexchange.core.projector.ProjectorMain` | `core-projector/target/core-projector.jar` |
| `core-loadgen` | money-path benchmark driver: hold, ME order, trade, settlement journal, bridge, settle, with latencies measured from the scheduled send time. Entry point `com.openexchange.core.loadgen.MoneyLoadGenerator`; the order load generator `com.openexchange.core.loadgen.CoreOrderLoadGenerator` (see Order load generator) | `core-loadgen/target/core-loadgen.jar` |

`grafana/` holds Prometheus scrape config and alert rules.

## Build and test

Requirements: Java 21 and Maven.

Two dependencies come from other repos, at the exact versions pinned in `pom.xml`: `com.openexchange:cluster-kit` (`cluster-kit.version`) and `com.match:match-common` (`match.version`, `match.journal.version`). Either install them into `~/.m2` from local checkouts of cluster-kit and match, or resolve them from GitHub Packages with the CI settings file, which reads `GITHUB_ACTOR` and `GITHUB_TOKEN` (a token with `read:packages`):

```bash
export GITHUB_ACTOR=<your-github-user> GITHUB_TOKEN=<token>
mvn -s .mvn-settings.xml clean verify      # what CI runs: unit tests, determinism corpus, cluster boot smoke test
```

With the dependencies already in `~/.m2`, the Makefile targets work without `-s`:

| target | runs |
|---|---|
| `make build` | `mvn clean package -DskipTests` |
| `make test` | `mvn test` |
| `make determinism` | the in-process determinism, snapshot-replay, conservation and recovery tests |
| `make update-goldens` | regenerates the determinism golden files after an intentional output change; review the diff before committing |
| `make integration` | boots a real single-node embedded-driver cluster in-process and drives it over the wire |
| `make nightly` | the slow tests (100k-account restore, 1M sizing, large scenario, the long replica process tests) that CI runs on its nightly schedule |

Pull requests run `mvn verify -Pfast`, which leaves the long replica process tests to the nightly job (the shadow puller, leader-gate, re-bootstrap, member-replica pull and the two external-driver pull (`-Dcore.pull.external=true`) stories run in the nightly job with three members, and the 100k-account pull with `-Dcore.pull.100k=true`); a plain `mvn verify` runs everything. The nightly replica load rows (lag, supplier loss, leader loss, replica kill, wipe and re-bootstrap under order load) take every bound as a `-Dcore.lag.*` property; the supplier-loss bound `core.lag.peerloss.bound.s` defaults to 30 s because the recovery is the supplier's liveness timeout (about 3 s), the backup's 12 s cool-down, one backup query and the catch-up of the bytes written meanwhile, measured at 16 to 28 s on the dev box; 20 s failed one run in four. `integration` and `nightly` launch clusters. On a machine that also runs a live engine, pin them to spare cores, for example `taskset -c 20-23 make integration`.

The feed harness jar used by cluster-tools' compatibility tests is built with `mvn -s .mvn-settings.xml -pl core-cluster -am -Pfeed-harness -DskipTests package` and published on demand by `.github/workflows/feed-harness.yml` (run it by hand on the commit to publish) as the asset `core-feed-harness-<sha>.jar` of the single `feed-harness` prerelease.

## Configuration

Every setting is read from the environment first, then from a JVM system property, else a default. The sources of truth are:

- `core-cluster/src/main/java/com/openexchange/core/infrastructure/persistence/AeronCluster.java`: cluster membership and state directory.
- `core-cluster/src/main/java/com/openexchange/core/infrastructure/TransportConfig.java`: media driver and transport.
- `core-cluster/src/main/java/com/openexchange/core/infrastructure/persistence/SnapshotModeSettings.java`: snapshot stub and restore settings, each with its default and allowed range.
- `core-bridge/.../BridgeConfig.java` and `core-projector/.../ProjectorConfig.java` for the two agents.

The main engine settings:

| variable | default | meaning |
|---|---|---|
| `CLUSTER_NODE` | `0` | this member's id |
| `CLUSTER_ADDRESSES` | `localhost` | comma-separated member hosts, in member-id order |
| `CLUSTER_PORT_BASE` | `9300` | base of the member's port range |
| `CLUSTER_PORT_STRIDE` | `0` | per-member port offset; set `100` when several members share one host (read by cluster-kit) |
| `BASE_DIR` | `./core-node<id>` | cluster and archive state, the durable ledger. A path under `/tmp` or `/dev/shm` logs a warning at startup |
| `TRANSPORT_DRIVER_MODE` | `embedded` | `embedded`, or `external` to attach to a running `aeronmd` |
| `AERON_DIR` | Aeron default + `-core-<id>-driver` | media driver directory in external mode |
| `TRANSPORT_IDLE_MODE` | `backoff` | `busy_spin` for dedicated cores |
| `TRANSPORT_INTERFACE`, `TRANSPORT_MTU` (`8192`), `TRANSPORT_TERM_LENGTH` (`16m`), `TRANSPORT_LOG_TERM_LENGTH` (`64m`), `TRANSPORT_SNAPSHOT_TERM_LENGTH` (`64m`) | | transport tuning |
| `TRANSPORT_LOG_RCV_WINDOW` | min(16m, term/2), logged at boot | the log channel's receiver window (`rcv-wnd`), 64k..`TRANSPORT_LOG_TERM_LENGTH`/2. Unset, it derives min(16m, `TRANSPORT_LOG_TERM_LENGTH`/2) (a 16m term gets 8m, the fixture's 1m term 512k, 32m and 64m terms 16m) and the boot guard's line names it: `[BOOT-GUARD] log channel <uri> rcv-wnd 8m derived (min(16m, term/2) of 16m): ...`; a term below 128k cannot derive a legal window and refuses. An explicit value is taken as given (`rcv-wnd <w> set`); above half the term it refuses the boot |
| `TRANSPORT_LOG_SO_RCVBUF`, `TRANSPORT_LOG_SO_SNDBUF` | `16m` | the log channel's socket buffers (64k..1g); `SO_RCVBUF` below the window refuses the boot |
| `ARCHIVE_SEGMENT_LENGTH` | max(64m, `TRANSPORT_LOG_TERM_LENGTH`), logged at boot | the archive's segment file length (members and the replica). A log purge removes whole segments only, so a log keeps up to one segment below the floor; a small box or a test fixture sets it down (the docker fixture runs 1m) so its logs actually purge. A power of two in [64k, 1g] and at least `TRANSPORT_LOG_TERM_LENGTH` (Aeron would raise the log's segments to the term silently); anything else refuses the boot. The boot guard's line names it: `[BOOT-GUARD] archive segment 1m set (1048576 B; ...)`. `-Daeron.archive.segment.file.length` is not read: the core sets the length explicitly |
| `CORE_DRIVER_DIR_ACTIVE_MS` | `10000` | embedded mode: a driver directory whose heartbeat is at most this old belongs to a live driver and refuses the boot (2000..600000); external mode ignores it |
| `BALANCE_FEED_CHANNEL`, `BALANCE_FEED_STREAM_ID` | unset (off) | optional balance update side channel |
| `CORE_LEADER_HEARTBEAT_INTERVAL_MS` | `100` | how often the leader sends a heartbeat |
| `CORE_LEADER_HEARTBEAT_TIMEOUT_MS` | `1000` | how long followers wait without a heartbeat before they start an election; at least 3 x the interval |
| `CORE_ELECTION_TIMEOUT_MS` | `1000` | election round timeout |
| `CORE_NODE_PORT` | `9700 + node id` | HTTP probe port |

The log channel's socket buffers are probed at boot in both driver modes: when the kernel grants less than `TRANSPORT_LOG_SO_RCVBUF` / `TRANSPORT_LOG_SO_SNDBUF`, the member exits 1 with `CORE BOOT GUARD:` naming `net.core.rmem_max` / `net.core.wmem_max`, the current and the needed value. Every member host needs `net.core.rmem_max` and `net.core.wmem_max` >= 16777216 with the defaults (`sysctl -w`, persisted in `/etc/sysctl.d`). In external mode the URI's `rcv-wnd`, `so-rcvbuf` and `so-sndbuf` override the aeronmd's own defaults for the log image. A restart within `CORE_DRIVER_DIR_ACTIVE_MS` of a kill of the member's own embedded driver refuses until the bound passes: the process manager's restart delay must cover it.

The three timing settings decide the leaderless window after a leader dies (about heartbeat timeout plus one election) and how often load or a GC pause on a healthy leader is taken for a death. No client is acked before a quorum commits and applies its command, so a failover never loses an acked command; during the window ingress is refused and clients retry with their request id. Every member of a cluster must use the same values.

Settings renamed from earlier releases (for example `ASSETS_ROLE` to `CORE_ROLE`, `AE_AUTH_CREDENTIAL` to `CORE_AUTH_CREDENTIAL`, `BRIDGE_AE_PORT_BASE` to `BRIDGE_CORE_PORT_BASE`, `RCS_MODE` to `SNAPSHOT_MODE`) are refused: a process that still sees an old name, as a variable, property or flag, exits 1 and names the new one. The table is `core-common/src/main/java/com/openexchange/core/config/RetiredSettings.java`.

Ports are `CLUSTER_PORT_BASE + node id * CLUSTER_PORT_STRIDE + offset`, with offsets 1 archive control, 2 client ingress, 3 member, 4 log, 5 catchup transfer. With the defaults, clients connect to port 9302.

### Shadow pulls and the purge anchor

A member in `SNAPSHOT_MODE=shadow` can copy shadows from other archives (a peer member's, the replica's) with the shadow puller. It talks to a source only through the source's archive control channel and verifies every copy locally before it counts. Details in cluster-kit's `docs/shadow.md` ("Pulling shadows").

| variable | default | meaning |
|---|---|---|
| `SHADOW_PULL_SOURCES` | empty (disabled) | ordered `host:port` list of archive control endpoints (port offset 1) to pull from; empty logs `CRITICAL shadow puller disabled`; this node's own endpoint is refused at boot |
| `SHADOW_PULL_PERIOD_MS` | `5000` | listing period (500..3600000) |
| `SHADOW_PULL_STARTUP_WAIT_MS` | `60000` | the one bound of the startup pull (1000..86400000) |
| `SHADOW_PULL_TIMEOUT_MS` | `120000` | one steady-state copy (1000..86400000) |
| `SHADOW_PULL_RETRY_BACKOFF_MS` | `5000` | retry wait, doubling to 8x (100..3600000); keep it at or above 5000 when the endpoints below are fixed |
| `SHADOW_PULL_MAX_INFLIGHT` | `1` | copies at once (1..3); sources per node x inflight must stay at or below 3 per source |
| `SHADOW_PULL_KINDS` | `ALIGNED` | comma list of `ALIGNED,STANDBY,CADENCE,LOCAL`; anything wider logs CRITICAL |
| `SHADOW_PULL_REPLICATION_ENDPOINT`, `SHADOW_PULL_RESPONSE_ENDPOINT` | empty (`<host>:0`) | fixed `host:port` a source sends the copy to and answers on; set both on an isolated host and open them to every source |
| `SHADOW_ANCHOR_RULE` | `two-producers` | below the newest snapshot a member purges its log only behind an ALIGNED shadow two distinct producers wrote with one digest; `any-verified` (one producer is enough) boots with a CRITICAL line |

The leader never pulls: while a member leads, its puller lists nothing (`shadow_pull_skipped_leader_total`) and aborts any copy in flight (`shadow_pull_aborted_leader_total`). A member that holds one producer's ALIGNED at a position pulls another producer's copy of that position once, as the second witness the anchor needs. At start, when a stub snapshot has no local shadow to restore from (EQUAL, SKIP, TAIL), the member pulls one from its sources before it considers virtual genesis; the halt text then names what the pull tried (`(3) pull: tried <n> sources, <c> candidates, refused <r>, last: <reason>` or `(3) pull: disabled: SHADOW_PULL_SOURCES empty`). The consensus module waits for the service without a bound, so restart one member at a time while pulls are enabled. Metrics: `shadow_pull_total`, `shadow_pull_bytes_total`, `shadow_pull_retries_total`, `shadow_pull_refused_total{reason}`, `shadow_pull_source_unreachable_total`, `shadow_pull_skipped_leader_total`, `shadow_pull_aborted_leader_total`, `shadow_anchor_witnesses`, `shadow_floor_no_witness_total`, `shadow_anchor_rule_two_producers`.

### Follower producer

In `SNAPSHOT_MODE=shadow` one follower per term (the member after the leader in `CLUSTER_ADDRESSES` order, wrapping) writes an ALIGNED shadow of its own image at each committed snapshot position into its own archive; with the replica's copy it is the second producer the purge anchor needs. Details in cluster-kit's `docs/shadow.md` ("The follower producer").

| variable | default | meaning |
|---|---|---|
| `SHADOW_PRODUCER` | `designated-follower` | `none`: this member never writes an ALIGNED (CRITICAL at boot); set explicitly to `designated-follower` with `SNAPSHOT_MODE=off` logs CRITICAL and is ignored |
| `SHADOW_PRODUCER_BUFFER_MB` | `512` | the image buffer (1..2047); an image above it is a counted skip |
| `SHADOW_TERM_LENGTH` | `4m` | the shadow IPC term (64k..16m, power of two) |
| `SHADOW_WRITE_CONNECT_TIMEOUT_MS`, `SHADOW_WRITE_OFFER_TIMEOUT_MS`, `SHADOW_WRITE_RECORD_TIMEOUT_MS` | `10000`, `30000`, `60000` | writer timeouts (1..86400000) |
| `SHADOW_PRODUCER_WINDOW_BUDGET_PCT` | `50` | share of the log receiver window a serialize may fill at the recent ingress rate (1..100) |
| `SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS` | `1000` | serialize time assumed before the first one (1..600000) |
| `SHADOW_PRODUCER_RATE_WINDOW_MS` | `10000` | ingress rate horizon (1000..3600000) |
| `SHADOW_WITNESS_ALARM_MS` | `7200000` | how long the newest ALIGNED may have fewer than two producers before the floor-side CRITICAL (1000..604800000) |

The leader's archive is never a pull source while it leads (`shadow_pull_source_skipped_leader_total`); a `SHADOW_PULL_SOURCES` entry that names a member's archive endpoint under another spelling than `CLUSTER_ADDRESSES` refuses the boot. Metrics: `shadow_producer_designated` (1, 0, -1 leader unknown), `shadow_producer_written_total`, `shadow_producer_serialize_total`, `shadow_producer_serialize_ms_last`, `shadow_producer_last_position`, `shadow_producer_skipped_total{reason}`, `shadow_producer_deferred_window_total`, `shadow_producer_aborted_leader_total`, `shadow_producer_leader_mismatch_total`, `shadow_producer_serialize_failed_total`, `shadow_crashed_writes_purged_total`, `shadow_floor_witness_missing{kind}`, `shadow_pull_source_skipped_leader_total`. The floor-side line `no two-producer anchor for <n> min ... missing kind <R|F>` is raised by every follower; a leader does not evaluate it.

### Replica

`java -jar core-cluster.jar replica` (or `CORE_ROLE=replica`) starts a fourth process that is not a member: it follows the cluster through Aeron `ClusterBackup`, applies the log into its own engine and writes the shadow images members restore from. Behaviour, trust after a restart, fences and readiness are in cluster-kit's `docs/replica.md`. Every setting is parsed and range-checked at boot; a bad value exits 1 with an `CORE REPLICA BOOT GUARD:` line before anything starts. The cluster settings above (`CLUSTER_ADDRESSES`, `CLUSTER_PORT_BASE`, `CLUSTER_PORT_STRIDE`, `TRANSPORT_*`) apply as well.

| variable | default | meaning |
|---|---|---|
| `CLUSTER_REPLICA_SLOT` | `3` | port slot; must not collide with a member on the same host |
| `CLUSTER_REPLICA_ADDRESS` | `localhost` (only when every member is local) | address the members reach the replica on |
| `BASE_DIR` | `./core-replica<slot>` | the replica's archive: log copy and shadows. Keep it on disk |
| `CORE_NODE_PORT` | `9700 + slot` | `/health`, `/ready`, `/metrics` |
| `REPLICA_BACKUP_SOURCE` | `FOLLOWER` | `FOLLOWER`, `LEADER` or `ANY`; `FOLLOWER` needs two or more members |
| `REPLICA_BACKUP_QUERY_INTERVAL_MS` | `60000` | how often the backup queries a member |
| `REPLICA_BACKUP_COOLDOWN_MS` | `12000` | wait after a backup reset |
| `REPLICA_VERIFY_WAIT_MS` | `180000` | stall bound for an unverified replica; at least 2 x query interval + cooldown |
| `REPLICA_ALIGNED_WAIT_MS` | `90000` | how long an ALIGNED trigger waits for a busy writer before it fences |
| `REPLICA_READY_MAX_LAG_BYTES` | `67108864` | recorded minus applied allowed for `/ready` |
| `REPLICA_READY_SHADOW_AGE_MS` | `1800000` | newest shadow age allowed for `/ready`; `0` disables |
| `REPLICA_READY_TIME_LAG_MS` | `3000` | wall clock minus cluster time of the last applied frame; `0` disables |
| `REPLICA_SHADOW_BUFFER_MB` | `512` | serialize buffer; an image larger than this fences |
| `REPLICA_MISMATCH_HOLD_MS` | `0` | an oracle mismatch holds the replica until it is stopped; a value (1..604800000) exits 1 after that long |
| `REPLICA_MISMATCH_DIR` | `BASE_DIR/mismatch` | where a mismatch dumps both images and `report.txt`; created and checked writable at boot |
| `REPLICA_MISMATCH_DUMP_MAX_MB` | `2048` | cap per dumped image file (0..16384); `0` writes the report only |
| `REPLICA_FENCE_PURGE_WAIT_MS` | `30000` | how long a history fence waits for the purge of its suspect shadows before exit 1 (1000..600000) |
| `REPLICA_MEMBER_CADENCE_MS` | `0` | the members' snapshot cadence; when set, `REPLICA_READY_SHADOW_AGE_MS` must cover it plus `REPLICA_VERIFY_WAIT_MS` |
| `SHADOW_PULL_SOURCES` (and the other `SHADOW_PULL_*` above) | empty (disabled) | members' archive endpoints; a fresh replica on a pruned log pulls an ALIGNED shadow from them (`restore_path` PULLED_SHADOW, code 3) |
| `SHADOW_REBOOTSTRAP_REPLAY_FAILURES` | `3` | replay-below-start refusals in a row before a re-bootstrap (0..1000; 0 counts and never acts) |
| `SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE`, `SHADOW_REBOOTSTRAP_WINDOW_MS` | `3`, `3600000` | the loop guard: more re-bootstraps than this inside the window refuse with exit 1 (1..100; 60000..604800000) |
| `SHADOW_REBOOTSTRAP_KEEP` | `2` | moved-aside `cluster.rebootstrap-*` dirs kept (1..100) |
| `SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS` | `60000` | at most one `re-bootstrap not taken` line per interval (1000..86400000) |
| `REPLICA_DRIVER_DIR_ACTIVE_MS` | `10000` | embedded driver only: an existing `AERON_DIR` whose driver heartbeat is at most this old belongs to a live driver, and the replica refuses to start instead of taking over an aeronmd (2000..600000); a crashed driver's directory counts as stale only after this long |

The remaining `REPLICA_*` settings (backup timeouts, catch-up buffers, driver MTU and term length, stream ids) are listed with their ranges in `core-cluster/src/main/java/com/openexchange/core/replica/ReplicaSettings.java`. After a normal restart the replica reports 503 `unverified continuation` until it has compared its state with the next member snapshot, about one member snapshot interval.

When the members pruned their logs past what the replica holds, the replica re-bootstraps instead of looping. After `SHADOW_REBOOTSTRAP_REPLAY_FAILURES` refusals it takes a VERIFIED ALIGNED above its log's stop position: one it already holds, or one it pulls from `SHADOW_PULL_SOURCES` and verifies. It logs `CRITICAL replica re-bootstrap: ...`, reads 503 `rebootstrap: restarting`, moves `BASE_DIR/cluster` to `BASE_DIR/cluster.rebootstrap-<utc>` after its backup and archive closed, and exits 3. Run the unit with `RestartForceExitStatus=3` (and `Restart=on-failure`); the restart restores from that shadow. When no source lists a newer shadow, the replica keeps running and reads 503 `re-bootstrap not taken: ...` (a single replica stays there until a second producer writes one; copy a VERIFIED shadow into `BASE_DIR/archive` or wipe `BASE_DIR`). Refusals start with `FATAL replica: re-bootstrap refused: ` and exit 1: the loop guard, or no sources and no local shadow above the stop position. Keep `CLUSTER_REPLICA_SLOT` stable for the life of `BASE_DIR`: the slot is the replica's producer identity for the two-producer anchor.

At every ALIGNED position the replica compares its state with the member snapshot its backup retrieved there. A
difference of the same history holds the replica instead of exiting: nothing is applied or written, `/ready` reads 503
`recovery=fenced:oracle mismatch at <P>: <detail>`, `/metrics` keeps serving (`shadow_oracle_mismatch_total`,
`shadow_oracle_mismatch_position`), and both images plus `report.txt` are in `REPLICA_MISMATCH_DIR/<P>/`. Read
`/metrics`, copy the dump, adjudicate offline, then stop the replica (exit 143) and wipe or keep `BASE_DIR`; a restart
re-latches at the same position. A member copy of another genesis, or a member snapshot the replica applied past
without a SNAPSHOT action, is a history fence: the replica's own shadows above its last equal compare are purged (on a restore start, only those above the restore point), then
it exits 1; wipe `BASE_DIR`. Switch a member to `SNAPSHOT_MODE=shadow` only when, over the agreed window,
`shadow_oracle_mismatch_total` and `shadow_oracle_other_genesis_total` did not move, `shadow_oracle_match_total` rose
by as much as `shadow_trigger_total{kind="ALIGNED"}`, and `shadow_replica_verified` is 1. Details in cluster-kit's
`docs/replica.md` ("Live oracle").

HTTP endpoints: each engine member serves `/health`, `/ready` and `/metrics` on its probe port. The bridge serves `/health` and `/metrics` on `BRIDGE_METRICS_PORT` (default 9600), the projector on `PROJECTOR_METRICS_PORT` (default 9601).

## Order load generator

`CoreOrderLoadGenerator` drives the core alone through one authenticated GATEWAY session (`CORE_AUTH_CREDENTIAL`): a ramp places `--open-orders` resting LIMIT GTC orders over `--users` and `--markets`, then a steady open-loop phase sends `--rate` slots per second for `--duration` seconds, a slot being one CreateOrder plus one CancelOrder of the oldest open order (`--mix create-only` sends the create alone). It ships in the same jar as a second main class:

```
java -cp core-loadgen/target/core-loadgen.jar com.openexchange.core.loadgen.CoreOrderLoadGenerator \
  --core-hosts 10.0.0.1,10.0.0.2,10.0.0.3 --core-port-base 9300 --rate 1000 --duration 60 --out results
```

- Identity: every CreateOrder, CancelOrder and Deposit carries a fresh 128-bit requestId (the core rejects a create without one); the clientOrderId is the requestId; the correlationId is the slot number.
- Elections: on a NewLeader event, or when an answer is older than `--reoffer-after-ms` (default 2000, the heartbeat timeout plus one election), the unanswered commands are offered once more with the same ids. The core answers a command it already applied with its stored decision (`duplicate` set) and applies nothing; such answers are counted as `replayed`. A command still unanswered when the run drains is `unanswered` and the run exits 1.
- The user-count rule: the core keeps 256 requestId decisions per user for 15 minutes, and the ramp's creates and the two `--prefund` deposits per user take requestIds from the same cap, so the run needs `--users` >= ceil((2 x rate x 900 + open orders) / (256 - deposits)), deposits 2 with `--prefund` and 0 without (7032 at 1000 slots/s with no ramp and no prefunding, 7481 with the default 100000 open orders and `--prefund`); below it the generator refuses to start rather than measure `TOO_MANY_PENDING_REQUESTS`.
- Measurement: create and cancel to their OrderAck from the slot's scheduled time (a stall is never hidden), the warm-up (`--warmup`) excluded; `summary.json` (build sha, settings, counts, rejects by reason, percentiles, the leader's `core_log_position` growth when `--metrics-url` is given) and `seconds.tsv` (per-second maxima) under `--out`.
- Optional: `--prefund` deposits to every user through a TOOL session (`CORE_TOOL_AUTH_CREDENTIAL`), `--gate-open` opens the global gate through an ADMIN session (`CORE_ADMIN_AUTH_CREDENTIAL`). `--aeron-dir` attaches to an external aeronmd, otherwise an embedded driver runs with `--idle busy_spin|backoff`. A bad setting exits 2 with one line naming the flag; `--help` lists them.
- In a test JVM the same `CoreOrderLoadGenerator.Session` runs against a rig's sessions (`tick` on the polling thread, the session as the egress listener).

## Releases

Release notes are in [GitHub Releases](https://github.com/openexch/core/releases). One version spans the v1 repos.

## License

Apache License 2.0.
