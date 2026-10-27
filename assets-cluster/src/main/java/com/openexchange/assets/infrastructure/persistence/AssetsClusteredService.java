// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import com.openexchange.assets.infrastructure.generated.EgressChannelsDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import com.openexchange.assets.infrastructure.publisher.SessionEgressQueue;
import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Aeron {@link ClusteredService} for the Assets Engine — the boundary that turns the deterministic
 * {@link AssetsEngine} into a replicated cluster service. Thin: ingress is decoded by
 * {@link AssetsSbeDemuxer} and applied to the engine (on every replica, deterministically); egress is
 * emitted by {@link AssetsEventPublisher} to client sessions (leader only); snapshots go through the
 * unit-tested {@link BalanceSnapshotCodec}.
 *
 * <p><b>Phase 0 status:</b> the ingress path and the snapshot byte format are exercised by the
 * determinism + codec test suites. The Aeron snapshot <em>transport</em> (offer/poll/reassemble) and
 * live leader egress are exercised when the node is first booted (Phase 2 shadow mode) — that is where
 * the two-cluster-on-one-box capacity is validated on appropriate hardware. Money egress is reliable
 * (never shed): {@link #enqueueEgress} queues rather than drops.</p>
 *
 * <p><b>Egress shape.</b> Apply writes encoded frames into a per-session {@link SessionEgressQueue} —
 * request-scoped frames (acks, rejects, query replies) into the ORIGIN session's queue only, shared
 * streams into every subscriber's (see {@link #enqueueEgress} for the routing and its delivery
 * contract); {@link #drainEgress()} offers them in bulk at the end of every {@code onSessionMessage}.
 * Under load
 * that is opportunistic batching (each drain sends whatever accumulated, so batches form by
 * themselves) — and since schema v5 the drain also COALESCES consecutive same-type high-rate events
 * into one batch frame (see {@link SessionEgressQueue}), so a SettleBatch worth of ingress leaves as a
 * handful of offers instead of hundreds; at low rate the queue holds one command's events and latency
 * is unchanged. A fixed-id
 * cluster timer drains as well, purely as the idle-path liveness belt for "a consumer unblocked but no
 * further ingress arrived" — it is never the normal path, which is what keeps this from repeating the
 * matching engine's 20 ms flush-timer latency mistake. Offers cannot be moved off this thread: Aeron's
 * {@code ClusteredServiceAgent.offer} rejects calls made from {@code doBackgroundWork},
 * {@code onStart}, {@code onRoleChange} and {@code onTerminate}.</p>
 */
public final class AssetsClusteredService implements ClusteredService {

    // RCS S1: every replicated-state mutation goes through the applier (engine, projector, demuxer, envelope). RC1 S7:
    // a CoreConfig cross-check failure stops this node (A-9 rule), through the same seam as the apply fence; failFast
    // is read lazily, so a test's recorder swapped in later is the one that runs.
    private final AssetsLogApplier applier = new AssetsLogApplier(() -> this.failFast.run());
    // Read-only aliases of the applier's instances (declared after it: field order matters).
    private final AssetsEngine engine = applier.engine();
    private final AssetsSbeDemuxer demuxer = applier.demuxer();

    {
        // RC1 S3 (K6): the replicated registration decides; these are its leader-local transport effects.
        applier.setTransportHooks(new AssetsSbeDemuxer.TransportHooks() {
            public void onSessionRegistered(final long sessionId, final int channelMask, final boolean resumable) {
                if (!isLeader || cluster == null) {
                    return;
                }
                refreshSessionsIfNeeded();
                final SessionEgressQueue queue = egressBySessionId.get(sessionId);
                if (queue != null) {
                    queue.subscribe(channelMask);
                    if (resumable) {
                        queue.markResumable();
                    }
                }
            }

            public void onSessionRejected(final long sessionId) {
                if (!isLeader || cluster == null) {
                    return;
                }
                // Through consensus, so followers observe the close deterministically via the log.
                final ClientSession session = cluster.getClientSession(sessionId);
                if (session != null && !session.isClosing()) {
                    try {
                        session.close();
                    } catch (final RuntimeException ignored) {
                        // already closing
                    }
                }
            }
        });
    }

    private Cluster cluster;
    private IdleStrategy idleStrategy;
    private boolean isLeader;

    /**
     * Idle-path drain timer. A CONSTANT id, recognised by value in {@link #onTimerEvent}, so every node
     * identifies it deterministically after any recover/replay. This is the match#25/#26 lesson: a
     * counter-allocated id desynced across switchovers and silently killed the flush chain. Rescheduling
     * the same id is idempotent, so at most one drain timer exists cluster-wide.
     */
    /**
     * Timer ids are per LEADERSHIP TERM: {@code BASE + leadershipTermId} (RC1 S5). Aeron 1.53's consensus module counts,
     * per correlationId, every TimerEvent a node consumes from the log while no timer with that id is pending
     * ({@code onReplayTimerEvent}: followers do this for every expiry, the leader only during replay/catch-up) and
     * silently swallows that many later {@code scheduleTimer} calls for the id ({@code onScheduleTimer}); the map is
     * never pruned. A fixed id is therefore poisoned on a node that was a follower for a while: after it becomes
     * leader its first re-arm is swallowed, {@code *Armed} stays true and the periodic timer is dead. A fresh id per
     * term starts at count 0 on the new leader, the same id is safe within the term (the live leader never feeds the
     * counter), and the map grows by one key per term, not per tick. Anything at or above a base is that timer kind.
     */
    private static final long EGRESS_DRAIN_TIMER_BASE = AssetsLogApplier.EGRESS_DRAIN_TIMER_BASE;
    static final long SWEEP_TIMER_BASE = AssetsLogApplier.SWEEP_TIMER_BASE;
    /** The ids term 0 would use; kept for the in-process service test. */
    static final long EGRESS_DRAIN_TIMER_ID = EGRESS_DRAIN_TIMER_BASE;
    static final long SWEEP_TIMER_ID = SWEEP_TIMER_BASE;
    /** Current leadership term (from onNewLeadershipTermEvent, also seen during replay); -1 until known. */
    private long leadershipTermId = -1L;

    static boolean isSweepTimerId(final long correlationId) {
        return AssetsLogApplier.isSweepTimerId(correlationId);
    }

    static boolean isDrainTimerId(final long correlationId) {
        return AssetsLogApplier.isDrainTimerId(correlationId);
    }

    /** Terms are spaced by {@link #TIMER_ID_TERM_STRIDE} so consecutive terms never use adjacent ids (control runs). */
    static final long TIMER_ID_TERM_STRIDE = AssetsLogApplier.TIMER_ID_TERM_STRIDE;

    private long termTimerId(final long base) {
        return base + Math.max(0L, leadershipTermId) * TIMER_ID_TERM_STRIDE;
    }
    private static final long EGRESS_DRAIN_BACKSTOP_MS = 10;
    /** RC1 S2 (K3-D): the ONE deadline sweep timer; constant id, re-armed every SWEEP_PERIOD_MS while the index is non-empty. */

    static final long SWEEP_PERIOD_MS = 100;
    private boolean sweepArmed;
    private volatile long sweepTimerFires;

    // Per-session egress queues, plus a cached array so the money path iterates without allocating
    // (cluster.clientSessions() returns a JDK unmodifiable wrapper whose iterator() allocates).
    private final Long2ObjectHashMap<SessionEgressQueue> egressBySessionId = new Long2ObjectHashMap<>();
    private SessionEgressQueue[] egressQueues = new SessionEgressQueue[0];
    /** Peeks ingress frames for the transport-level messages handled or observed at this boundary. */
    private final MessageHeaderDecoder peekHeaderDecoder = new MessageHeaderDecoder();
    private final SubscribeDecoder subscribeDecoder = new SubscribeDecoder();
    /**
     * The session whose command is currently being dispatched — the ORIGIN that request-scoped egress
     * (see {@link AssetsEventPublisher.Routing}) is delivered to. Set around {@code demuxer.dispatch}
     * in {@link #onSessionMessage}, null everywhere else: no other entry point (timer, snapshot load)
     * emits request-scoped frames, and {@link #enqueueToOrigin} asserts that. Leader-local transport
     * state like the queues themselves — never replicated, never snapshotted.
     */
    private ClientSession originSession;
    /**
     * True while the dispatched command is a read-only query ({@link AssetsSbeDemuxer#isQuery}): a
     * query emits only reply frames, so the publisher routes even its {@code BalanceUpdate} emissions
     * (a balance-snapshot reply streams entries through that method) to the origin.
     */
    private boolean originIsQuery;
    private boolean sessionsDirty = true;
    private boolean drainTimerArmed;
    private volatile long drainTimerFires;
    private volatile long resumableSessionsClosed;
    /** Review FIX-2: every RC2 S8 3.13 discard close on this node (written on the service thread only). */
    private volatile long discardClosesTotal;

    // Live egress instrumentation, published as Aeron counters so AeronStat (and therefore every bench
    // run) can read them without touching this process. Updated only from the drain timer, never from
    // the money path. Null when the driver refused them: diagnostics must never stop a node booting.
    private static final int EGRESS_COUNTER_TYPE_ID = 1_000_101;
    private static final long EGRESS_REPORT_INTERVAL_MS = 1_000;
    private boolean countersAttempted;
    private Counter egressPendingBytes;
    private Counter egressBackPressured;
    private Counter egressPeakPendingBytes;
    private long lastEgressReportMs;

    /**
     * Wire this node's money-journal sink onto the engine. Must be called before the container launches
     * so the deterministic service thread can journal from the very first command, replayed or live.
     * Wiring the sink does not arm journaling: that is replicated state, restored from the snapshot or
     * set by a SetMoneyJournal command through the log.
     */
    public void setMoneyJournalSink(com.openexchange.assets.domain.MoneyJournalSink journalSink) {
        applier.setMoneyJournalSink(journalSink);
    }

    /**
     * This node's balance-feed conflation store, or null when the feed is off (the default). Written
     * by this service thread on every LIVE balance update — on followers too, so the store is always
     * current on every node — and published by the feed's own agent thread while this node leads.
     * Node-local transport state like the egress queues: never replicated, never snapshotted. Must be
     * set before the container launches, like the journal sink.
     */
    private com.openexchange.assets.infrastructure.feed.BalanceConflationStore balanceFeed;

    public void setBalanceFeed(com.openexchange.assets.infrastructure.feed.BalanceConflationStore store) {
        this.balanceFeed = store;
    }

    // What this node answers about ITSELF, for whatever supervises it. The
    // money ledger has no external supervisor in the open deployment, so the
    // orchestrator's two questions are answered here: kill me (/health), and
    // may you move on to the next member (/ready). The second one is the one
    // that costs quorum when it lies, so it is deliberately pessimistic.
    private final com.openexchange.cluster.NodeReadiness readiness =
            new com.openexchange.cluster.NodeReadiness();
    private com.openexchange.cluster.NodeEndpoint nodeEndpoint;
    private com.openexchange.cluster.ConsensusReadiness consensusReadiness;
    // Per-message service-thread cost (apply vs drain), per-template cost and the last snapshot's
    // size/timings, served as /metrics by the node endpoint. Plain writes on this thread only; see
    // AssetsNodeMetrics for the publish/acquire visibility contract.
    private final com.openexchange.assets.infrastructure.metrics.AssetsNodeMetrics nodeMetrics =
            new com.openexchange.assets.infrastructure.metrics.AssetsNodeMetrics();
    // Aeron can reenter doBackgroundWork via idle() before the callback returns.
    // Its logPosition is already the new event's position at that point.
    private int applicationCallbackDepth;
    private boolean applicationCallbackFailed;

    /**
     * The fail-fast exit for an apply failure (RC1 S1, port of the ME's 23090c6 fence). The default is
     * {@link Runtime#halt(int)}, NOT {@link System#exit(int)}: every caller runs on the cluster SERVICE
     * thread, and this process's shutdown hooks (the Aeron teardown chain) await that very thread, so
     * exit() from here deadlocks into a zombie (observed live on the ME, 2026-08-23: alive, HEALTHY,
     * processing nothing, TERM-immune). halt() skips the hooks; skipping graceful teardown is correct
     * because a node whose apply failed must not reach peers anyway, recovery is restart + replay.
     * Callers log their evidence BEFORE invoking the seam. Package-private so tests swap a recorder in.
     */
    Runnable failFast = AssetsClusteredService::failFastHalt;

    /** Production default of {@link #failFast}; static so the forked-JVM test runs the exact path. */
    static void failFastHalt() {
        Runtime.getRuntime().halt(1);
    }

    /** Wired before launch; the context owns the same-process consensus evidence. */
    public void readinessConsensus(final io.aeron.cluster.ConsensusModule.Context context) {
        consensusReadiness = new com.openexchange.cluster.ConsensusReadiness(context, readiness);
    }

    // What makes this cluster snapshot itself with no admin gateway running.
    // The AE is the engine this was missing on: nothing ever triggered its
    // snapshots, so its log grew from the day it shipped until /dev/shm was full
    // and the money path stopped for seventeen hours (2026-07-25).
    private final com.openexchange.cluster.SnapshotCadence snapshotCadence =
            com.openexchange.cluster.SnapshotCadence.fromEnv();
    // ...and what turns the snapshot into reclaimed disk on THIS node.
    private com.openexchange.cluster.SnapshotLogPruner logPruner;
    // Lazily connected archive client used only to confirm the snapshot recording landed on disk
    // before onTakeSnapshot returns (see awaitSnapshotRecorded). Cloned context: never shares lock
    // state with the cluster's own archive client. Closed in onTerminate.
    private AeronArchive snapshotArchive;
    /** Bounded wait for a wedged snapshot publication (all chunks share one deadline). */
    private static final long SNAPSHOT_OFFER_DEADLINE_MS = 30_000;
    /** Bounded wait for the archive to ingest the published snapshot bytes. */
    private static final long SNAPSHOT_RECORD_DEADLINE_MS = 10_000;

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnStart(cluster, snapshotImage);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        if (consensusReadiness != null) { consensusReadiness.start(cluster.aeron()); }
        this.idleStrategy = cluster.idleStrategy();
        this.isLeader = cluster.role() == Cluster.Role.LEADER;
        nodeMetrics.setRole(cluster.role().code());
        applier.setEventSink(new AssetsEventPublisher(this::enqueueEgress, () -> originIsQuery,
                balanceFeed != null ? balanceFeed::onBalanceUpdate
                        : AssetsEventPublisher.LiveBalanceTap.NONE));
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
            seedBalanceFeedFromSnapshot();
        }
        startNodeEndpoint();
        startDurability(cluster);
        // Started, not ready: the snapshot is loaded but the log replay may
        // still be running. Readiness waits for current consensus and application evidence.
        readiness.started();
    }

    /**
     * What this node exposes about itself beyond liveness. Suppliers run on the endpoint thread and
     * read plain longs or Aeron counters; nothing here can block the service thread. The histogram
     * and snapshot series come from {@code nodeMetrics} itself.
     */
    private com.openexchange.assets.infrastructure.metrics.AssetsMetricsBody metricsBody() {
        return new com.openexchange.assets.infrastructure.metrics.AssetsMetricsBody(nodeMetrics)
                .gauge("assets_accounts",
                        "Accounts in the ledger (never shrinks; a rejected command still creates its account)",
                        () -> engine.accountCount())
                .gauge("assets_last_applied_trade_id", "Settlement high-water (dense ME tradeId) applied by this ledger",
                        engine::getLastAppliedTradeId)
                .gauge("assets_consume_position", "ME settlement-journal egressSeq watermark this ledger has applied",
                        engine::getConsumePosition)
                .gauge("assets_journal_seq", "Money journal sequence (last entry written)", engine::getJournalSeq)
                .gauge("assets_money_journal_enabled", "1 if the money journal is armed on this ledger, else 0",
                        () -> engine.isMoneyJournalEnabled() ? 1 : 0)
                .counter("assets_settle_faults_total",
                        "Settle legs that could not draw fully from their order hold (MUST be alerted on)",
                        engine::getSettleFaultCount)
                .counter("assets_suppressed_feed_releases_total",
                        "Feed TerminalReleases suppressed on OMS-managed holds", engine::getSuppressedFeedReleaseCount)
                .counter("assets_exhausted_holds_reaped_total", "Fully drawn holds reaped from accounts",
                        engine::getExhaustedHoldsReaped)
                .counter("assets_drain_timer_fires_total",
                        "Idle-path egress drain belt fires (leader only; a chain that stops advancing is match#25)",
                        () -> drainTimerFires)
                .counter("assets_resumable_sessions_closed_total",
                        "Sessions closed at their egress queue ceiling instead of blocking the engine (every core session is resumable)",
                        () -> resumableSessionsClosed)
                .gauge("assets_sessions", "Client sessions with an egress queue on this node", () -> egressQueues.length)
                .gauge("assets_egress_pending_bytes", "Bytes queued for egress across sessions (-1 until the leader counters exist)",
                        () -> egressPendingBytes == null ? -1 : egressPendingBytes.get())
                .gauge("assets_egress_peak_pending_bytes", "Peak of assets_egress_pending_bytes (-1 until the leader counters exist)",
                        () -> egressPeakPendingBytes == null ? -1 : egressPeakPendingBytes.get())
                .counter("assets_egress_back_pressured_total",
                        "Egress offers that hit back pressure (-1 until the leader counters exist)",
                        () -> egressBackPressured == null ? -1 : egressBackPressured.get())
                .gauge("assets_application_callback_failed",
                        "1 once a cluster callback threw on this node (apply latched, /ready 503, process stopped), else 0",
                        () -> applicationCallbackFailed ? 1 : 0)
                .counter("assets_ingress_schema_rejects_total",
                        "Ingress frames dropped for a schemaId/version outside this build's supported set (before any body decode)",
                        demuxer::schemaRejectCount)
                .counter("assets_ingress_decode_rejects_total",
                        "Ingress frames dropped for frame/group bounds or an invalid enum/field byte (poison input, expected)",
                        demuxer::decodeRejectCount)
                .counter("assets_ingress_apply_errors_total",
                        "Application faults after a decoded command entered the engine; nonzero fences this node until restart",
                        demuxer::applyErrorCount)
                .counter("assets_ingress_unknown_template_total",
                        "Ingress frames whose templateId money-schema does not define", demuxer::unknownTemplateCount)
                .counter("assets_ingress_unhandled_template_total",
                        "Ingress frames whose v6 templateId this build does not serve yet (gateway ahead of the core)",
                        demuxer::unhandledTemplateCount)
                .counter("assets_settle_rejects_total",
                        "Feed settles refused before any mutation (unknown market, bad amounts, overflow); MUST be alerted on",
                        engine::getSettleRejectCount)
                .counter("assets_egress_oversize_drops_total",
                        "Egress frames larger than a session publication's max message length, lost for that session; should stay 0",
                        this::egressOversizeDrops)
                .counter("assets_ingress_unauthorised_total",
                        "Ingress commands refused for session authority (class, user scope, registration); never a fence",
                        demuxer::unauthorisedCount)
                .gauge("assets_sessions_known", "Sessions in the replicated authority table", () -> engine.sessions().size())
                .gauge("assets_order_entry_gate",
                        "1 when the global order-entry gate is OPEN, else 0 (reason in the log and AdmissionState)",
                        () -> engine.gate().state(com.openexchange.assets.application.engine.gate.OrderEntryGate.GLOBAL)
                                == com.openexchange.assets.domain.GateState.OPEN ? 1 : 0)
                .counter("assets_sweep_timer_fires_total",
                        "Deadline sweep timer fires applied on this node (one log entry each; K3-D)", () -> sweepTimerFires)
                .gauge("assets_deadline_index_size", "Entries in the derived deadline index (GTD, replace, requestId TTL)",
                        () -> engine.deadlines().size())
                .gauge("assets_last_log_time_ms", "Monotone log time last applied by this ledger (cluster epoch ms)",
                        engine::getLastLogTime)
                .include(com.openexchange.assets.infrastructure.metrics.OrderEngineMetrics.of(engine.metricsView(), this::egressDiscardCloses));
    }

    /**
     * Arm the snapshot rhythm and the log pruner.
     *
     * <p>Same wiring as the matching engine, deliberately: the code is one copy
     * in cluster-kit and both engines call it the same way. A feature added to
     * one engine's durability and not the other is how the AE ended up with no
     * snapshot trigger at all.</p>
     *
     * <p>The cadence starts from {@code cluster.logPosition()}, which here is the
     * recovery position - the last snapshot's position on a node that recovered
     * from one, 0 at genesis. Starting from 0 on a recovered node would make it
     * believe the whole log accumulated since its last snapshot.</p>
     */
    private void startDurability(final Cluster cluster) {
        try {
            final int clusterId = cluster.context().clusterId();
            snapshotCadence.bind(
                    () -> com.openexchange.cluster.SnapshotCadence.findControlToggle(
                            cluster.aeron(), clusterId),
                    cluster.logPosition(), System.nanoTime());

            if (com.openexchange.cluster.SnapshotCadence.booleanFromEnv(
                    "SNAPSHOT_PRUNE_ENABLED", "snapshot.prune.enabled", true)) {
                logPruner = new com.openexchange.cluster.SnapshotLogPruner(
                        cluster.context().clusterDir(),
                        cluster.context().archiveContext().clone(),
                        cluster.aeron().countersReader(),
                        clusterId,
                        readiness::ready)
                        // Same as the matching engine: computed on the leader after
                        // every snapshot and logged, not yet applied to the purge.
                        .reportWatermark(
                                com.openexchange.cluster.ClusterMemberPositions.from(
                                        cluster.aeron(), cluster.context()),
                                () -> isLeader);
                logPruner.start();
            } else {
                System.out.println("[PRUNE] disabled by SNAPSHOT_PRUNE_ENABLED=false — "
                        + "this node's cluster log will grow until something else purges it");
            }
        } catch (Exception e) {
            // Loud, and NOT fatal: a node that cannot arm its own durability must
            // still serve the ledger. But this is precisely the failure that was
            // invisible for seventeen hours, so it is reported as what it is.
            System.err.println("ASSETS CRITICAL: snapshot cadence/log pruner did not start — "
                    + "this cluster will not snapshot or reclaim disk on its own: " + e);
            e.printStackTrace();
        }
    }

    /**
     * The probe endpoint. A fixed port by default rather than one derived from
     * the member id, because the member id is still -1 this early (see
     * {@code allocateEgressCounters}) and a probe on the wrong port reads as a
     * dead node.
     */
    private void startNodeEndpoint() {
        try {
            // Probe/metrics port: honor ASSETS_NODE_PORT if injected (the process
            // manager sets it authoritatively), else fall back to 9700 + nodeId so
            // co-located nodes never collide. Base is 9700, not 9600: 9600 is the
            // Settlement Bridge's metrics port on the same box. cluster.memberId() is
            // -1 this early in onStart, so read the id the way AeronCluster does.
            // Empty env is treated as unset.
            String nodeStr = System.getenv("CLUSTER_NODE");
            if (nodeStr == null || nodeStr.isEmpty()) {
                nodeStr = System.getProperty("node.id", "0");
            }
            int nodeId;
            try {
                nodeId = Integer.parseInt(nodeStr.trim());
            } catch (NumberFormatException nfe) {
                nodeId = 0;
            }
            nodeMetrics.setMemberId(nodeId);
            // Env first (process manager), then the system property (in-process tests pick a free
            // port without touching the environment), else the per-node default.
            final String env = System.getenv("ASSETS_NODE_PORT");
            final String prop = System.getProperty("assets.node.port");
            final int port;
            if (env != null && !env.isEmpty()) {
                port = Integer.parseInt(env);
            } else if (prop != null && !prop.isEmpty()) {
                port = Integer.parseInt(prop);
            } else {
                port = 9700 + nodeId;
            }
            nodeEndpoint = new com.openexchange.cluster.NodeEndpoint(readiness, metricsBody()::render);
            nodeEndpoint.start(port);
        } catch (Exception e) {
            // A probe endpoint must never be the reason the ledger does not start.
            System.err.println("NODE: failed to start probe endpoint: " + e.getMessage());
        }
    }

    /**
     * Called every duty cycle on every member, busy or idle: the only signal
     * that separates a quiet market from a wedged agent thread. Nothing is
     * offered from here, deliberately; see the note on {@code doBackgroundWork}
     * above.
     */
    @Override
    public int doBackgroundWork(final long nowNs) {
        if (applicationCallbackDepth != 0 || applicationCallbackFailed) {
            readiness.tick();
            if (applicationCallbackFailed) { readiness.unavailable("application-callback-failed"); }
            return 0; // Never publish a partially applied event as completed.
        }
        if (consensusReadiness == null) {
            readiness.tick();
            readiness.roleChanged(cluster.role());
            readiness.unavailable("consensus-not-configured");
        } else {
            consensusReadiness.poll(cluster, nowNs);
        }
        // At most once a second, and only on the leader: is the cluster due a
        // snapshot? Everything past that gate is a long compare.
        return snapshotCadence.tick(nowNs, isLeader, cluster.logPosition());
    }

    /**
     * Allocated on first use, not in {@code onStart}: the member id is still -1 that early, and a
     * counter labelled "member -1" is a counter nobody can attribute in a three-node run.
     */
    private void allocateEgressCounters() {
        if (countersAttempted) {
            return;
        }
        countersAttempted = true;
        try {
            final int memberId = cluster.memberId();
            egressPendingBytes = cluster.aeron().addCounter(
                    EGRESS_COUNTER_TYPE_ID, "assets egress pending bytes, member " + memberId);
            egressBackPressured = cluster.aeron().addCounter(
                    EGRESS_COUNTER_TYPE_ID, "assets egress back-pressured frames, member " + memberId);
            egressPeakPendingBytes = cluster.aeron().addCounter(
                    EGRESS_COUNTER_TYPE_ID, "assets egress peak pending bytes, member " + memberId);
        } catch (final RuntimeException ex) {
            System.err.println("assets egress counters unavailable, continuing without them: " + ex);
        }
    }

    private void updateEgressCounters() {
        allocateEgressCounters();
        if (egressPendingBytes == null) {
            return;
        }
        long pending = 0;
        long backPressured = 0;
        long peak = 0;
        for (int i = 0; i < egressQueues.length; i++) {
            final SessionEgressQueue queue = egressQueues[i];
            pending += queue.pendingBytes();
            backPressured += queue.backPressureCount();
            peak = Math.max(peak, queue.peakPendingBytes());
        }
        egressPendingBytes.setRelease(pending);
        egressBackPressured.setRelease(backPressured);
        egressPeakPendingBytes.setRelease(peak);
    }

    /**
     * Name the consumer that is falling behind. Aggregate counters say egress is the limiter; they do
     * not say <em>whose</em> egress, and the answer decides where the next fix goes (batching versus
     * per-session filtering). The response channel carries the consumer's endpoint, which is the only
     * identifier that means anything from outside. Off the money path, at most one line per second, and
     * silent while every queue is empty.
     */
    private void reportEgressPressure(long nowMs) {
        if (nowMs - lastEgressReportMs < EGRESS_REPORT_INTERVAL_MS) {
            return;
        }
        boolean anyPending = false;
        for (int i = 0; i < egressQueues.length; i++) {
            if (!egressQueues[i].isEmpty()) {
                anyPending = true;
                break;
            }
        }
        if (!anyPending) {
            return;
        }
        lastEgressReportMs = nowMs;
        final StringBuilder sb = new StringBuilder(96).append("EGRESS PRESSURE:");
        for (int i = 0; i < egressQueues.length; i++) {
            final SessionEgressQueue queue = egressQueues[i];
            sb.append(" [session=").append(queue.sessionId())
                    .append(" via=").append(queue.session().responseChannel())
                    .append(" pending=").append(queue.pendingBytes())
                    .append("B backPressured=").append(queue.backPressureCount())
                    .append(']');
        }
        System.err.println(sb);
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnSessionOpen(session, timestamp);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnSessionOpen(ClientSession session, long timestamp) {
        if (session != null) {
            // The principal the authenticator minted rides the SessionOpen log event: every member parses the
            // same bytes into the same authority (K6). Null only in in-process unit tests.
            applier.onSessionOpen(session.id(), cluster == null ? -1L : cluster.logPosition(), timestamp,
                    session.encodedPrincipal());
        }
        sessionsDirty = true;
        drainEgress();
        // Arm here as well as in onSessionMessage: a leader that has a session but has not yet been
        // asked anything should already have its idle-path belt running.
        armDrainTimerIfNeeded();
        armSweepIfNeeded();
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnSessionClose(session, timestamp, closeReason);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        applier.onSessionClose(session.id(), cluster == null ? -1L : cluster.logPosition(), timestamp, closeReason);
        egressBySessionId.remove(session.id());
        sessionsDirty = true;
        drainEgress();
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer,
                                 int offset, int length, Header header) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnSessionMessage(session, timestamp, buffer, offset, length, header);
        } catch (RuntimeException | Error failure) {
            // The demuxer already classified this: decode poison never gets here (dropped + counted), so
            // anything that does is an APPLICATION FAULT after a possible mutation. Close readiness in
            // THIS callback, log the evidence (hooks will not run), stop the process, and rethrow so
            // Aeron's own accounting sees it too. The 'latch and continue' this replaces left a node
            // applying with /ready 503 and its snapshot cadence stopped. Recovery is restart + replay.
            failApplicationCallback();
            System.err.println("FATAL command application failure: logPosition=" + header.position()
                    + " sessionId=" + (session == null ? -1 : session.id())
                    + " templateId=" + peekTemplateId(buffer, offset, length)
                    + " type=" + failure.getClass().getName() + " message=" + failure.getMessage());
            failFast.run();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void ensureApplicationHealthy() {
        if (applicationCallbackFailed) {
            throw new IllegalStateException("Application callback already failed; restart required");
        }
    }

    /** Latch AND close readiness now, not on the next background tick. */
    private void failApplicationCallback() {
        applicationCallbackFailed = true;
        readiness.unavailable("application-callback-failed");
    }

    private int peekTemplateId(DirectBuffer buffer, int offset, int length) {
        return length >= MessageHeaderDecoder.ENCODED_LENGTH && offset >= 0
                && length <= buffer.capacity() - offset
                ? peekHeaderDecoder.wrap(buffer, offset).templateId() : -1;
    }

    private void applyOnSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer,
                                 int offset, int length, Header header) {
        // Subscribe is a TRANSPORT concern, not money state: it changes who receives egress, never what
        // the ledger does. It is handled here and deliberately never reaches the engine, so the engine
        // stays a pure deterministic state machine and the subscription never enters a snapshot.
        // Service-thread cost, apply and drain timed separately (RC0 measurement input). Diagnostic
        // nanoTime only: nothing here feeds replicated state. Sampled per ASSETS_APPLY_SAMPLE_SHIFT
        // (default: every message).
        boolean sampled = false;
        int templateId = -1;
        long dispatchStartNs = 0L;
        long dispatchEndNs = 0L;
        if (applier.isTransportSubscribe(buffer, offset, length)) {
            subscribeTransport(session, buffer, offset, length);
        } else {
            // The origin context: while the engine applies this command, request-scoped egress goes to
            // THIS session (and, for a query, so does every emission — see isQueryIngress). Cleared in
            // a finally so a throwing apply cannot leak one command's origin onto the next.
            originSession = session;
            originIsQuery = isQueryIngress(buffer, offset, length);
            sampled = nodeMetrics.shouldSample();
            if (sampled) {
                templateId = length >= MessageHeaderDecoder.ENCODED_LENGTH
                        ? peekHeaderDecoder.wrap(buffer, offset).templateId() : -1;
                dispatchStartNs = System.nanoTime();
            }
            try {
                // Deterministic on every replica: decode -> domain command -> engine mutation. The apply
                // only queues its egress; the offers happen in the drain below, off the apply's critical
                // section.
                applier.onSessionMessage(session == null ? -1L : session.id(), header.position(), timestamp,
                        buffer, offset, length);
            } finally {
                originSession = null;
                originIsQuery = false;
            }
            if (sampled) {
                dispatchEndNs = System.nanoTime();
            }
        }
        drainEgress();
        if (sampled) {
            nodeMetrics.recordMessage(templateId, dispatchEndNs - dispatchStartNs,
                    System.nanoTime() - dispatchEndNs, header.position());
        }
        armDrainTimerIfNeeded();
        armSweepIfNeeded();
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnTimerEvent(correlationId, timestamp);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnTimerEvent(long correlationId, long timestamp) {
        final int kind = AssetsLogApplier.timerKind(correlationId);
        final long logPosition = cluster == null ? -1L : cluster.logPosition();
        if (kind == AssetsLogApplier.TIMER_SWEEP) {
            // Replicated: a TimerEvent is a log entry, so every member pops the same entries at the same
            // log time; only the leader re-arms, and only while something is left to sweep.
            sweepArmed = false;
            sweepTimerFires++;
            applier.onTimerEvent(correlationId, logPosition, timestamp);
            drainEgress();
            armSweepIfNeeded();
            return;
        }
        applier.onTimerEvent(correlationId, logPosition, timestamp);
        if (kind == AssetsLogApplier.TIMER_DRAIN) {
            drainTimerArmed = false;
            drainTimerFires++;
            final long drainStartNs = System.nanoTime();
            drainEgress();
            nodeMetrics.recordTimerDrain(System.nanoTime() - drainStartNs);
            updateEgressCounters();
            reportEgressPressure(timestamp);
            armDrainTimerIfNeeded();
        }
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnTakeSnapshot(snapshotPublication);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnTakeSnapshot(ExclusivePublication snapshotPublication) {
        final long startNs = System.nanoTime();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = applier.serialize(buffer);
        final long serializedNs = System.nanoTime();

        // Chunked, like the matching engine's A-2 fix: a single offer() throws
        // IllegalArgumentException the moment the payload exceeds the publication's
        // maxMessageLength (snapshot term / 8: 8 MiB at the 64m default, 128 KiB on a 1m term).
        // Every member snapshots at the same log position, so an oversize single offer fenced ALL
        // nodes at once and the log was never pruned again. Splitting into maxMessageLength-sized
        // messages keeps every offer legal; a payload that fits is still one message, byte-identical
        // to the historical path. The receiver (loadSnapshot) reassembles by raw concatenation.
        final int chunks = offerSnapshotChunked(snapshotPublication, buffer, length, idleStrategy);
        final long offeredNs = System.nanoTime();

        // Do not return until the archive has the bytes on disk: a node killed between the offer
        // and the archive flush would prune a snapshot the recording log already points at and
        // fail recovery with "unknown recording id" (matching engine forensic, 2026-07-02).
        awaitSnapshotRecorded(snapshotPublication);
        final long recordedNs = System.nanoTime();

        System.out.println("[SNAPSHOT] accounts=" + engine.accountCount() + " bytes=" + length
                + " chunks=" + chunks + " maxMessageLength=" + snapshotPublication.maxMessageLength()
                + " serializeMs=" + ((serializedNs - startNs) / 1_000_000)
                + " offerMs=" + ((offeredNs - serializedNs) / 1_000_000)
                + " recordMs=" + ((recordedNs - offeredNs) / 1_000_000)
                + " logPosition=" + cluster.logPosition());
        nodeMetrics.stampSnapshot(System.currentTimeMillis(), length, chunks, serializedNs - startNs,
                offeredNs - serializedNs, recordedNs - offeredNs, cluster.logPosition());

        // The baseline for the next snapshot, on EVERY member and whoever asked for this one.
        snapshotCadence.snapshotTaken(cluster.logPosition(), System.nanoTime());
    }

    /**
     * Offer a serialized snapshot as one or more Aeron messages, each no larger than the
     * publication's {@code maxMessageLength}. Package-private and static so the multi-chunk
     * transport path is unit-testable against a real Aeron publication
     * ({@code SnapshotChunkingTransportTest}). One overall deadline covers every chunk, so a wedged
     * publication fails loudly instead of parking the service thread forever (the old loop retried
     * every negative result with no bound). Chunk count is a node-local I/O detail: each member splits
     * with its own {@code maxMessageLength} and the receiver concatenates, so there is deliberately
     * no cross-node framing.
     *
     * @return the number of messages offered
     */
    static int offerSnapshotChunked(final ExclusivePublication snapshotPublication,
                                    final DirectBuffer buffer, final int length,
                                    final IdleStrategy idleStrategy) {
        final int maxLen = snapshotPublication.maxMessageLength();
        final long deadlineMs = System.currentTimeMillis() + SNAPSHOT_OFFER_DEADLINE_MS;
        int sent = 0;
        int chunks = 0;
        long attempts = 0;
        idleStrategy.reset();
        while (sent < length) {
            final int chunk = Math.min(maxLen, length - sent);
            long result;
            while ((result = snapshotPublication.offer(buffer, sent, chunk)) < 0) {
                if (result == ExclusivePublication.CLOSED
                        || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                    throw new IllegalStateException("failed to offer assets snapshot: result=" + result
                            + " sent=" + sent + "/" + length + " bytes");
                }
                attempts++;
                if (System.currentTimeMillis() > deadlineMs) {
                    throw new IllegalStateException("assets snapshot offer stuck after "
                            + SNAPSHOT_OFFER_DEADLINE_MS + "ms: lastResult=" + result
                            + " attempts=" + attempts + " sent=" + sent + "/" + length + " bytes");
                }
                idleStrategy.idle();
            }
            sent += chunk;
            chunks++;
        }
        return chunks;
    }

    /**
     * Block until the Archive's recording of the snapshot publication has ingested every byte we
     * offered ({@code recordingPosition >= publication.position()}), or fail after a bounded wait.
     */
    private void awaitSnapshotRecorded(final ExclusivePublication snapshotPublication) {
        final long writtenPosition = snapshotPublication.position();
        if (writtenPosition <= 0) {
            return;
        }
        if (snapshotArchive == null) {
            snapshotArchive = AeronArchive.connect(cluster.context().archiveContext().clone());
        }
        final String channel = snapshotPublication.channel();
        final int streamId = snapshotPublication.streamId();
        final int sessionId = snapshotPublication.sessionId();
        final long deadlineMs = System.currentTimeMillis() + SNAPSHOT_RECORD_DEADLINE_MS;

        long recordingId;
        idleStrategy.reset();
        while ((recordingId = snapshotArchive.findLastMatchingRecording(0L, channel, streamId, sessionId))
                == Aeron.NULL_VALUE) {
            if (System.currentTimeMillis() > deadlineMs) {
                throw new IllegalStateException("assets snapshot recording not registered within "
                        + SNAPSHOT_RECORD_DEADLINE_MS + "ms (channel=" + channel + " stream=" + streamId
                        + " session=" + sessionId + ")");
            }
            idleStrategy.idle();
        }
        long observedPosition;
        while ((observedPosition = snapshotArchive.getRecordingPosition(recordingId)) < writtenPosition) {
            if (System.currentTimeMillis() > deadlineMs) {
                throw new IllegalStateException("assets snapshot archive ingestion stalled: recordingId="
                        + recordingId + " written=" + writtenPosition + " recorded=" + observedPosition);
            }
            idleStrategy.idle();
        }
    }

    @Override
    public void onNewLeadershipTermEvent(long leadershipTermId, long logPosition, long timestamp, long termBaseLogPosition,
                                         int leaderMemberId, int logSessionId, TimeUnit timeUnit, int appVersion) {
        // Seen live and during replay (in log order): the latest term names the timer ids this node arms as leader.
        this.leadershipTermId = leadershipTermId;
        // Never fenced at the base: a latched applier refuses every entry, so a term event on a stopped node stays silent.
        if (!applier.failed()) {
            applier.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
        }
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        ensureApplicationHealthy();
        applicationCallbackDepth++;
        try {
            applyOnRoleChange(newRole);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnRoleChange(Cluster.Role newRole) {
        this.isLeader = newRole == Cluster.Role.LEADER;
        nodeMetrics.setRole(newRole.code());
        // The feed publishes only on the leader; a GAIN also queues a full-store sweep, which is the
        // feed's natural baseline for consumers across the failover (the store itself is current on
        // every node because the live tap runs during apply everywhere).
        if (balanceFeed != null) {
            balanceFeed.onRoleChange(isLeader);
        }
        // A live role also means recovery is behind us; CANDIDATE withdraws
        // readiness, which is exactly when a rolling restart has to wait.
        readiness.roleChanged(newRole);
        // A member that is no longer leader drops any snapshot request it was
        // waiting on: the consensus module reads that toggle only while leading.
        snapshotCadence.roleChanged(isLeader);
        // Anything still queued belongs to a leadership this node no longer holds: a demoted node
        // cannot offer, and consumers resynchronise through the snapshot-request messages.
        for (int i = 0; i < egressQueues.length; i++) {
            egressQueues[i].reset();
        }
        // Always re-arm from scratch. Only the leader really schedules (a follower's scheduleTimer is a
        // no-op), and a timer does not survive a snapshot recover-into-leader, so clearing the flag
        // guarantees the next onSessionMessage arms exactly one fresh belt. This is match#26's fix:
        // missing it leaves a leader whose idle-path drain never fires. The deadline sweep follows the
        // same rule (K3-D): clear here, re-arm from the next legal callback while the index is non-empty.
        drainTimerArmed = false;
        sweepArmed = false;
    }

    @Override
    public void onTerminate(Cluster cluster) {
        applicationCallbackDepth++;
        try {
            applyOnTerminate(cluster);
        } catch (RuntimeException | Error failure) {
            failApplicationCallback();
            throw failure;
        } finally {
            applicationCallbackDepth--;
        }
    }

    private void applyOnTerminate(Cluster cluster) {
        readiness.stopping();
        if (consensusReadiness != null) { consensusReadiness.close(); }
        if (logPruner != null) {
            logPruner.stop();
        }
        if (nodeEndpoint != null) {
            nodeEndpoint.stop();
        }
        if (snapshotArchive != null) {
            try {
                snapshotArchive.close();
            } catch (final RuntimeException ignored) {
                // shutting down
            }
            snapshotArchive = null;
        }
        closeQuietly(egressPendingBytes);
        closeQuietly(egressBackPressured);
        closeQuietly(egressPeakPendingBytes);
    }

    private static void closeQuietly(Counter counter) {
        if (counter != null) {
            try {
                counter.close();
            } catch (final RuntimeException ignored) {
                // shutting down; a counter that cannot be released must not mask the real reason
            }
        }
    }

    private void loadSnapshot(Image snapshotImage) {
        final ExpandableArrayBuffer collector = new ExpandableArrayBuffer();
        final MutableInteger length = new MutableInteger(0);
        // The snapshot is one OR MORE Aeron messages (chunked on the way out to stay under
        // maxMessageLength). FragmentAssembler reassembles each message from its MTU fragments; the
        // messages are then ACCUMULATED, in arrival order, into one contiguous payload that is decoded
        // once. A single ExclusivePublication wrote them, so the concatenation is byte-identical to
        // BalanceSnapshotCodec.serialize's output. Decoding the first message alone (the old
        // behaviour) would silently restore a truncated ledger the moment a snapshot spans two chunks.
        final FragmentHandler handler = (buffer, offset, len, header) -> {
            collector.putBytes(length.value, buffer, offset, len);
            length.value += len;
        };
        final io.aeron.FragmentAssembler assembler = new io.aeron.FragmentAssembler(handler);
        idleStrategy.reset();
        int polls = 0;
        while (true) {
            final int fragments = snapshotImage.poll(assembler, 64);
            polls++;
            if (snapshotImage.isEndOfStream()) {
                // Drain anything the image delivered together with end-of-stream.
                while (snapshotImage.poll(assembler, 64) > 0) {
                    polls++;
                }
                break;
            }
            if (fragments == 0 && snapshotImage.isClosed()) {
                // Same guard as Aeron's own ClusteredServiceAgent.loadState: an image that went away
                // without end-of-stream would otherwise park the service thread here forever.
                throw new IllegalStateException("assets snapshot image closed before end-of-stream: "
                        + length.value + " byte(s) received at position " + snapshotImage.position());
            }
            idleStrategy.idle(fragments);
        }
        System.out.println("[SNAPSHOT] reassembled " + length.value + " byte(s) in " + polls
                + " poll(s) from image position " + snapshotImage.position());
        restoreFromImage(collector, length.value);
    }

    /**
     * Restore from a reassembled snapshot image: open the kit envelope (magic, version, length, CRC32C),
     * decode the v4 payload, require it to end exactly where the envelope says, then verify the ledger
     * invariants. Any refusal is logged with its reason and stops the node through {@link #failFast}: a
     * node must not run on a snapshot it could not verify (hardening B1.2 step 5). A bare payload is
     * refused like any other corruption; there is no legacy pass-through (clean genesis).
     */
    void restoreFromImage(final DirectBuffer image, final int imageLength) {
        try {
            // Empty image, envelope, decode, framing and invariants are all checked by the applier.
            applier.restore(image, imageLength);
            final BalanceSnapshotCodec.Decoded decoded = applier.lastRestored();
            System.out.println("[SNAPSHOT] restored orders=" + engine.orders().orders().size() + " accounts=" + decoded.accountCount
                    + " holds=" + decoded.holdCount + " lastAppliedTradeId=" + decoded.lastAppliedTradeId
                    + " consumePosition=" + decoded.consumePosition + " journalSeq=" + decoded.journalSeq
                    + " lastLogTime=" + decoded.lastLogTime + " sections=" + decoded.sectionCount
                    + " deadlines=" + engine.deadlines().size() + " enveloped=true verified=true");
        } catch (final RuntimeException refused) {
            failApplicationCallback();
            System.err.println("FATAL snapshot refused: " + refused.getMessage()
                    + " (imageLength=" + imageLength + "); this node will not serve an unverified ledger");
            failFast.run();
            throw refused;
        }
    }

    /** The snapshot image as offered: the v4 payload written at the envelope's payload offset, then sealed. */
    static int encodeSnapshot(final AssetsEngine engine, final MutableDirectBuffer buffer,
                              final com.openexchange.cluster.SnapshotEnvelope envelope) {
        return AssetsLogApplier.encode(engine, buffer, envelope);
    }

    /** Exposed for tests / diagnostics: deadline sweep timer fires on this node. */
    public long sweepTimerFires() {
        return sweepTimerFires;
    }

    /**
     * Seed the balance-feed store from restored state. The snapshot codec writes balances straight
     * into accounts (no sink events), so a node recovering from a snapshot would otherwise hold a
     * feed store that knows only the keys the post-snapshot replay touches — and its
     * leadership-gain sweep would be a PARTIAL baseline, the silently-wrong kind. Boot-time only,
     * off the money path; replayed commands then overwrite through the live tap.
     */
    private void seedBalanceFeedFromSnapshot() {
        final com.openexchange.assets.infrastructure.feed.BalanceConflationStore feed = balanceFeed;
        if (feed == null) {
            return;
        }
        engine.forEachAccountByUserId(account -> {
            final long userId = account.userId();
            account.forEachNonZeroBalance((assetId, available, locked) ->
                    feed.onBalanceUpdate(userId, assetId, available, locked));
        });
    }

    /**
     * Reliable leader-only egress, called from inside the engine's apply: copy the encoded frame into
     * the destination queue(s) and return. Never offer here, because an inline offer means a
     * back-pressured consumer parks the whole deterministic thread.
     *
     * <p><b>Routing.</b> {@code BROADCAST} frames (live balance updates, settlements) are copied into
     * every subscribed session's queue, as always. {@code ORIGIN} frames (acks, rejects, query replies)
     * go ONLY to the session whose command is being applied — and go there UNCONDITIONALLY: the
     * Subscribe channel mask narrows broadcast streams, never the replies to a session's own commands
     * (you sent the command, you get its answer). Session count is no longer a multiplier on
     * request-scoped egress, and consumers stop client-filtering foreign acks. Routing is entirely
     * transport-side: it decides which QUEUE a frame lands in; the state machine and the emitted bytes
     * are untouched, so replicas stay byte-identical.</p>
     *
     * <p><b>Delivery contract for request-scoped egress: AT-MOST-ONCE, TO THE ORIGIN.</b> An
     * undelivered frame dies with its session — origin gone here, or discarded on a CLOSED session at
     * offer time — exactly as broadcast frames always have (see
     * {@code SessionEgressQueue#offerFrame}). Recovery for a client that lost its session mid-flight
     * is COMMAND RETRY + engine idempotency + authoritative resync (balance/hold snapshot queries),
     * never re-delivery: the replicated log is the source of truth, the ack is a delivery
     * optimization.</p>
     */
    private void enqueueEgress(MutableDirectBuffer buffer, int offset, int length, int channel,
                               AssetsEventPublisher.Routing routing) {
        if (!isLeader || cluster == null) {
            return; // followers replicate state but do not emit client egress
        }
        if (routing == AssetsEventPublisher.Routing.ORIGIN) {
            enqueueToOrigin(buffer, offset, length);
            return;
        }
        refreshSessionsIfNeeded();
        final SessionEgressQueue[] queues = egressQueues;
        for (int i = 0; i < queues.length; i++) {
            final SessionEgressQueue queue = queues[i];
            if (!queue.wants(channel)) {
                continue; // this session did not subscribe to this class of egress
            }
            if (!queue.append(buffer, offset, length)) {
                appendAfterFreeingSpace(queue, buffer, offset, length);
            }
        }
    }

    /**
     * Queue a request-scoped frame for the origin session alone, regardless of its channel mask —
     * there is deliberately no channel here to consult a mask with. Subscription channels exist to
     * narrow BROADCAST streams; they must never let a session accidentally silence the replies to
     * its own commands. Concretely: a session subscribed to only acks and settlements that sends
     * RequestBalanceSnapshot still receives the reply's BalanceUpdate-typed entries and its
     * terminator. See {@link #enqueueEgress} for the at-most-once delivery contract. An origin that
     * is gone (its queue no longer exists) drops the frame — and in-log-order a session cannot die
     * between its command's ingress and this apply anyway, so the null path is defensive.
     */
    private void enqueueToOrigin(MutableDirectBuffer buffer, int offset, int length) {
        final ClientSession origin = originSession;
        if (origin == null) {
            // Only a session-message dispatch emits request-scoped frames; timers and snapshot load
            // emit none. Surface a violation in test JVMs (surefire enables assertions), stay silent
            // on the money path in production — the log is authoritative either way.
            assert false : "request-scoped egress emitted outside a session-message dispatch";
            return;
        }
        refreshSessionsIfNeeded();
        final SessionEgressQueue queue = egressBySessionId.get(origin.id());
        if (queue == null) {
            return;
        }
        if (!queue.append(buffer, offset, length)) {
            appendAfterFreeingSpace(queue, buffer, offset, length);
        }
    }

    /**
     * Peek: is this ingress frame a read-only query command? Mirrors what {@link AssetsSbeDemuxer}
     * will do with the same bytes — a templateId switch, deliberately with no schema check the
     * demuxer does not make — so {@link #originIsQuery} is true precisely while the demuxer is
     * dispatching a query.
     */
    private boolean isQueryIngress(DirectBuffer buffer, int offset, int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return false;
        }
        peekHeaderDecoder.wrap(buffer, offset);
        return AssetsSbeDemuxer.isQuery(peekHeaderDecoder.templateId());
    }

    /**
     * The transport effect of a well-formed Subscribe ({@link AssetsLogApplier#isTransportSubscribe} said so): decode
     * the channel mask and, on the leader, narrow this session's broadcast streams. Never the engine, never a
     * snapshot. A short or foreign-version Subscribe is not classified as one: it goes to the applier, whose demuxer
     * drops and counts it without reading stale bytes past the frame.
     */
    private void subscribeTransport(ClientSession session, DirectBuffer buffer, int offset, int length) {
        subscribeDecoder.wrapAndApplyHeader(buffer, offset, peekHeaderDecoder);
        final EgressChannelsDecoder channels = subscribeDecoder.channels();
        int mask = 0;
        if (channels.acks())        mask |= AssetsEventPublisher.CH_ACKS;
        if (channels.balances())    mask |= AssetsEventPublisher.CH_BALANCES;
        if (channels.settlements()) mask |= AssetsEventPublisher.CH_SETTLEMENTS;
        if (channels.snapshots())   mask |= AssetsEventPublisher.CH_SNAPSHOTS;
        if (isLeader && cluster != null) {
            refreshSessionsIfNeeded();
            final SessionEgressQueue queue = egressBySessionId.get(session.id());
            if (queue != null) {
                queue.subscribe(mask);
            }
        }
    }

    /**
     * The queue hit its growth ceiling: this session's consumer has been unable to keep up for a long
     * time. Every core session owns a resume protocol (RegisterSession requires {@code resumable=true};
     * the C++ gateway resyncs through the log, the forwarder through RequestPendingDispatch, the bridge
     * through QueryFeedPosition), so the session is CLOSED and the engine keeps flowing. Blocking the whole
     * engine on one consumer traded cluster availability for a guarantee no remaining client needs, and
     * it could livelock (an engine parked here rejects ingress, so a consumer waiting on this engine can
     * never drain the queue the engine is blocked on). {@link ClientSession#close()} goes through the
     * consensus module, so followers observe the close deterministically via the log.
     */
    private void appendAfterFreeingSpace(SessionEgressQueue queue, MutableDirectBuffer buffer,
                                         int offset, int length) {
        resumableSessionsClosed++;
        System.err.println("CRITICAL: assets egress queue full for session=" + queue.sessionId()
                + " via=" + queue.session().responseChannel()
                + " (pending=" + queue.pendingBytes() + "B, backPressured=" + queue.backPressureCount()
                + ", resumable=" + queue.isResumable()
                + ") — closing the session instead of blocking the engine; the consumer resyncs on reconnect");
        queue.reset();
        try {
            queue.session().close();
        } catch (final RuntimeException ignored) {
            // already closing: the queue is empty either way and later drains discard on CLOSED
        }
    }

    /**
     * Offer queued frames to every session, stopping per session at the first frame it back-pressures.
     * Refused frames stay queued, in order, for the next drain — that is what replaces the unbounded
     * park the inline path used to do.
     */
    private void drainEgress() {
        if (!isLeader || cluster == null) {
            return;
        }
        refreshSessionsIfNeeded();
        final SessionEgressQueue[] queues = egressQueues;
        for (int i = 0; i < queues.length; i++) {
            queues[i].drainAll();
        }
    }

    /**
     * Rebuild the cached session array when it may have gone stale. The size check is the important
     * part: sessions restored from a snapshot are added by Aeron's {@code ServiceSnapshotLoader}
     * WITHOUT an {@code onSessionOpen} callback, so a registry driven by callbacks alone would never
     * learn about them and those sessions would silently receive no egress. {@code size()} on the
     * session collection is O(1) and allocation-free, unlike its iterator.
     */
    private void refreshSessionsIfNeeded() {
        final Collection<ClientSession> sessions = cluster.clientSessions();
        if (!sessionsDirty && sessions.size() == egressQueues.length) {
            return;
        }
        final SessionEgressQueue[] rebuilt = new SessionEgressQueue[sessions.size()];
        int i = 0;
        for (final ClientSession session : sessions) {
            SessionEgressQueue queue = egressBySessionId.get(session.id());
            if (queue == null || queue.session() != session) {
                queue = new SessionEgressQueue(session, this::countDiscardClose);
                egressBySessionId.put(session.id(), queue);
            }
            rebuilt[i++] = queue;
        }
        if (egressBySessionId.size() > rebuilt.length) {
            // Belt and braces: drop queues whose session vanished without an onSessionClose.
            final Iterator<SessionEgressQueue> it = egressBySessionId.values().iterator();
            while (it.hasNext()) {
                final SessionEgressQueue queue = it.next();
                boolean live = false;
                for (int j = 0; j < rebuilt.length; j++) {
                    if (rebuilt[j] == queue) {
                        live = true;
                        break;
                    }
                }
                if (!live) {
                    it.remove();
                }
            }
        }
        egressQueues = rebuilt;
        sessionsDirty = false;
    }

    /**
     * Arm the deadline sweep (K3-D): one timer, constant id, {@value #SWEEP_PERIOD_MS} ms ahead, leader
     * only, only while the index is non-empty, and only from a callback where scheduling is legal (the
     * same rule as the drain belt). Rescheduling the same id is idempotent, so at most one sweep exists
     * cluster-wide; a sweep armed before a snapshot may also be replayed by the consensus module after a
     * restore, which is harmless because the sweep itself is idempotent (one-shot pops).
     */
    private void armSweepIfNeeded() {
        if (sweepArmed || !isLeader || cluster == null || engine.deadlines().isEmpty()) {
            return;
        }
        final long deadline = cluster.time() + cluster.timeUnit().convert(SWEEP_PERIOD_MS, TimeUnit.MILLISECONDS);
        idleStrategy.reset();
        while (!cluster.scheduleTimer(termTimerId(SWEEP_TIMER_BASE), deadline)) {
            idleStrategy.idle();
        }
        sweepArmed = true;
    }

    /**
     * Arm the idle-path drain belt. Leader only, and only from a callback where scheduling a timer is
     * legal (Aeron allows it from onSessionMessage / onTimerEvent / onSessionOpen / onSessionClose).
     */
    private void armDrainTimerIfNeeded() {
        if (drainTimerArmed || !isLeader || cluster == null) {
            return;
        }
        final long deadline = cluster.time()
                + cluster.timeUnit().convert(EGRESS_DRAIN_BACKSTOP_MS, TimeUnit.MILLISECONDS);
        idleStrategy.reset();
        while (!cluster.scheduleTimer(termTimerId(EGRESS_DRAIN_TIMER_BASE), deadline)) {
            idleStrategy.idle();
        }
        drainTimerArmed = true;
    }

    /**
     * RC2 S8 3.13 with review FIX-2: a monotone service-level total. Each queue reports its discard close here at the
     * moment it closes the session, so the count survives the queue's retirement at onSessionClose (a sum over the live
     * queues fell back to 0 within a duty cycle) and never decreases.
     */
    private long egressDiscardCloses() {
        return discardClosesTotal;
    }

    private void countDiscardClose() {
        discardClosesTotal++;
    }

    /** Sum over live session queues (a closed session's drops leave with its queue). */
    private long egressOversizeDrops() {
        long total = 0L;
        final SessionEgressQueue[] queues = egressQueues;
        for (int i = 0; i < queues.length; i++) {
            total += queues[i].oversizeDrops();
        }
        return total;
    }

    /** Exposed for tests / diagnostics. */
    public AssetsEngine engine() {
        return engine;
    }

    /** Exposed for tests / diagnostics: the ingress fence counters live on the demuxer. */
    public AssetsSbeDemuxer demuxer() {
        return demuxer;
    }

    /** The log applier this service wraps (RCS S1); package-private for tests and later RCS slices. */
    AssetsLogApplier applier() {
        return applier;
    }

    /**
     * How many times the idle-path drain belt has fired. Exposed for tests / diagnostics: a chain that
     * stops advancing is the match#25 failure mode, so it is worth being able to assert on it.
     */
    public long drainTimerFires() {
        return drainTimerFires;
    }

    /**
     * How many times a RESUMABLE session was closed at its queue ceiling instead of blocking the
     * engine. Exposed for tests / diagnostics: this is the leader-availability decision the failover
     * livelock fix rests on, so it is worth being able to assert on it.
     */
    public long resumableSessionsClosed() {
        return resumableSessionsClosed;
    }
}
