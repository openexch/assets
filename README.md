# assets

The order and ledger core of Open Exchange: one Java Aeron Cluster service that runs the order lifecycle, risk checks, holds and the money ledger (balances, holds, settlement) as a deterministic, replicated state machine.

Status: nightly prerelease. Not for production use.

User documentation (architecture, configuration, operations): https://openexchange.dev/docs

## Where it fits in v1

v1 is five repositories:

| repo | role |
|---|---|
| [match](https://github.com/openexch/match) | matching engine, its own Aeron Raft cluster (Java) |
| **assets** (this repo) | order and ledger core cluster (Java) |
| [cluster-kit](https://github.com/openexch/cluster-kit) | cluster machinery shared by both engines (Java) |
| [cluster-tools](https://github.com/openexch/cluster-tools) | C++ cluster client, ME forwarder, settlement bridge, projectors |
| [tools](https://github.com/openexch/tools) | probe and test tooling |

v1 has no user-facing gateway yet.

## Modules

| module | what it is | jar |
|---|---|---|
| `assets-common` | shared kernel: money value objects and the SBE wire codecs, generated at build time from `src/main/resources/sbe/money-schema.xml` (schema id 2, version 7) and `money-journal-schema.xml` (schema id 3, version 1) | library |
| `assets-cluster` | the engine: domain model, deterministic engine, Aeron cluster service. Entry point `com.openexchange.assets.Main` | `assets-cluster/target/assets-cluster.jar` |
| `assets-bridge` | Java settlement bridge: tails the matching engine's settlement journal and forwards trades and terminal order statuses into the cluster. Entry point `com.openexchange.assets.bridge.BridgeMain` | `assets-bridge/target/assets-bridge.jar` |
| `assets-projector` | follows the money journal (an Aeron Archive recording) and projects every money movement into PostgreSQL, resumable from the PostgreSQL high-water mark. Entry point `com.openexchange.assets.projector.ProjectorMain` | `assets-projector/target/assets-projector.jar` |
| `assets-loadgen` | money-path benchmark driver: hold, ME order, trade, settlement journal, bridge, settle, with latencies measured from the scheduled send time. Entry point `com.openexchange.assets.loadgen.MoneyLoadGenerator` | `assets-loadgen/target/assets-loadgen.jar` |

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
| `make nightly` | the slow flagged tests (100k-account restore, 1M sizing, large scenario) that CI runs on its nightly schedule |

`integration` and `nightly` launch clusters. On a machine that also runs a live engine, pin them to spare cores, for example `taskset -c 20-23 make integration`.

The feed harness jar used by cluster-tools' compatibility tests is built with `mvn -s .mvn-settings.xml -pl assets-cluster -am -Pfeed-harness -DskipTests package` and published on demand by `.github/workflows/feed-harness.yml` (run it by hand on the commit to publish) as the asset `assets-feed-harness-<sha>.jar` of the single `feed-harness` prerelease.

## Configuration

Every setting is read from the environment first, then from a JVM system property, else a default. The sources of truth are:

- `assets-cluster/src/main/java/com/openexchange/assets/infrastructure/persistence/AeronCluster.java`: cluster membership and state directory.
- `assets-cluster/src/main/java/com/openexchange/assets/infrastructure/TransportConfig.java`: media driver and transport.
- `assets-cluster/src/main/java/com/openexchange/assets/infrastructure/persistence/RcsSettings.java`: snapshot stub and restore settings, each with its default and allowed range.
- `assets-bridge/.../BridgeConfig.java` and `assets-projector/.../ProjectorConfig.java` for the two agents.

The main engine settings:

| variable | default | meaning |
|---|---|---|
| `CLUSTER_NODE` | `0` | this member's id |
| `CLUSTER_ADDRESSES` | `localhost` | comma-separated member hosts, in member-id order |
| `CLUSTER_PORT_BASE` | `9300` | base of the member's port range |
| `CLUSTER_PORT_STRIDE` | `0` | per-member port offset; set `100` when several members share one host (read by cluster-kit) |
| `BASE_DIR` | `./ae-node<id>` | cluster and archive state, the durable ledger. A path under `/tmp` or `/dev/shm` logs a warning at startup |
| `TRANSPORT_DRIVER_MODE` | `embedded` | `embedded`, or `external` to attach to a running `aeronmd` |
| `AERON_DIR` | Aeron default + `-assets-<id>-driver` | media driver directory in external mode |
| `TRANSPORT_IDLE_MODE` | `backoff` | `busy_spin` for dedicated cores |
| `TRANSPORT_INTERFACE`, `TRANSPORT_MTU` (`8192`), `TRANSPORT_TERM_LENGTH` (`16m`), `TRANSPORT_LOG_TERM_LENGTH` (`64m`), `TRANSPORT_SNAPSHOT_TERM_LENGTH` (`64m`) | | transport tuning |
| `BALANCE_FEED_CHANNEL`, `BALANCE_FEED_STREAM_ID` | unset (off) | optional balance update side channel |
| `ASSETS_NODE_PORT` | `9700 + node id` | HTTP probe port |

Ports are `CLUSTER_PORT_BASE + node id * CLUSTER_PORT_STRIDE + offset`, with offsets 1 archive control, 2 client ingress, 3 member, 4 log, 5 catchup transfer. With the defaults, clients connect to port 9302.

HTTP endpoints: each engine member serves `/health`, `/ready` and `/metrics` on its probe port. The bridge serves `/health` and `/metrics` on `BRIDGE_METRICS_PORT` (default 9600), the projector on `PROJECTOR_METRICS_PORT` (default 9601).

## Releases

Release notes are in [GitHub Releases](https://github.com/openexch/assets/releases). One version spans the v1 repos.

## License

Apache License 2.0.
