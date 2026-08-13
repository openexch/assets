// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.application.projection.SettlementProjector;
import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import com.openexchange.assets.infrastructure.generated.EgressChannelsDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import com.openexchange.assets.infrastructure.publisher.SessionEgressQueue;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
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

    private final AssetsEngine engine = new AssetsEngine();
    // The projector owns the feed-forward translation (Settle/TerminalRelease -> money mutation +
    // consumePosition advance). It wraps the same engine instance for the life of the service:
    // loadSnapshot restores INTO that instance, so the projector needs no re-wiring on recovery.
    private final SettlementProjector projector = new SettlementProjector(engine);
    private final AssetsSbeDemuxer demuxer = new AssetsSbeDemuxer(engine, projector);

    private Cluster cluster;
    private IdleStrategy idleStrategy;
    private boolean isLeader;

    /**
     * Idle-path drain timer. A CONSTANT id, recognised by value in {@link #onTimerEvent}, so every node
     * identifies it deterministically after any recover/replay. This is the match#25/#26 lesson: a
     * counter-allocated id desynced across switchovers and silently killed the flush chain. Rescheduling
     * the same id is idempotent, so at most one drain timer exists cluster-wide.
     */
    private static final long EGRESS_DRAIN_TIMER_ID = 1_000_000_000_000L;
    private static final long EGRESS_DRAIN_BACKSTOP_MS = 10;

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
    private long egressStallCount;
    private volatile long resumableSessionsClosed;

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
        engine.setMoneyJournalSink(journalSink);
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
    // cluster-kit#15: the last role this duty cycle OBSERVED via cluster.role().
    // Only feeds readiness in doBackgroundWork; isLeader and the rest of the
    // role bookkeeping stay driven by the onRoleChange callback.
    private Cluster.Role lastObservedRole;

    // What makes this cluster snapshot itself with no admin gateway running.
    // The AE is the engine this was missing on: nothing ever triggered its
    // snapshots, so its log grew from the day it shipped until /dev/shm was full
    // and the money path stopped for seventeen hours (2026-07-25).
    private final com.openexchange.cluster.SnapshotCadence snapshotCadence =
            com.openexchange.cluster.SnapshotCadence.fromEnv();
    // ...and what turns the snapshot into reclaimed disk on THIS node.
    private com.openexchange.cluster.SnapshotLogPruner logPruner;

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
        this.isLeader = cluster.role() == Cluster.Role.LEADER;
        engine.setEventSink(new AssetsEventPublisher(this::enqueueEgress, () -> originIsQuery,
                balanceFeed != null ? balanceFeed::onBalanceUpdate
                        : AssetsEventPublisher.LiveBalanceTap.NONE));
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
            seedBalanceFeedFromSnapshot();
        }
        startNodeEndpoint();
        startDurability(cluster);
        // Started, not ready: the snapshot is loaded but the log replay may
        // still be running. Readiness waits for the first live role change.
        readiness.started();
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
            final String env = System.getenv("ASSETS_NODE_PORT");
            final int port;
            if (env != null && !env.isEmpty()) {
                port = Integer.parseInt(env);
            } else {
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
                port = 9700 + nodeId;
            }
            nodeEndpoint = new com.openexchange.cluster.NodeEndpoint(readiness, null);
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
        readiness.tick();
        // cluster-kit#15: feed readiness by POLLING the role, never by trusting
        // onRoleChange alone. Aeron fires that callback only on role TRANSITIONS,
        // so a node that boots straight into FOLLOWER and stays there never gets
        // it: /ready answered "catching up" forever and SnapshotLogPruner never
        // pruned on that node (observed live 2026-08-12). An enum compare per
        // cycle is free; the callback keeps its other side effects (isLeader,
        // cadence, drain timer) and readiness.roleChanged is idempotent.
        final Cluster.Role observedRole = cluster.role();
        if (observedRole != lastObservedRole) {
            lastObservedRole = observedRole;
            readiness.roleChanged(observedRole);
            if (observedRole == Cluster.Role.LEADER || observedRole == Cluster.Role.FOLLOWER) {
                System.out.println("READINESS: role " + observedRole
                        + " observed via duty-cycle poll (cluster-kit#15)");
            }
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
        sessionsDirty = true;
        drainEgress();
        // Arm here as well as in onSessionMessage: a leader that has a session but has not yet been
        // asked anything should already have its idle-path belt running.
        armDrainTimerIfNeeded();
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        egressBySessionId.remove(session.id());
        sessionsDirty = true;
        drainEgress();
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer,
                                 int offset, int length, Header header) {
        // Subscribe is a TRANSPORT concern, not money state: it changes who receives egress, never what
        // the ledger does. It is handled here and deliberately never reaches the engine, so the engine
        // stays a pure deterministic state machine and the subscription never enters a snapshot.
        if (!applyIfSubscribe(session, buffer, offset, length)) {
            // Peeked, never consumed: a QueryFeedPosition marks its sender as resumable and still
            // flows to the engine, which answers it with a FeedPositionReport.
            markIfFeedConsumer(session, buffer, offset, length);
            // The origin context: while the engine applies this command, request-scoped egress goes to
            // THIS session (and, for a query, so does every emission — see isQueryIngress). Cleared in
            // a finally so a throwing apply cannot leak one command's origin onto the next.
            originSession = session;
            originIsQuery = isQueryIngress(buffer, offset, length);
            try {
                // Deterministic on every replica: decode -> domain command -> engine mutation. The apply
                // only queues its egress; the offers happen in the drain below, off the apply's critical
                // section.
                demuxer.dispatch(buffer, offset, length, timestamp);
            } finally {
                originSession = null;
                originIsQuery = false;
            }
        }
        drainEgress();
        armDrainTimerIfNeeded();
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        // Holds have no TTL yet (auto-expiry is a later phase); the only timer is the egress drain belt.
        if (correlationId == EGRESS_DRAIN_TIMER_ID) {
            drainTimerArmed = false;
            drainTimerFires++;
            drainEgress();
            updateEgressCounters();
            reportEgressPressure(timestamp);
            armDrainTimerIfNeeded();
        }
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(engine, buffer);
        idleStrategy.reset();
        while (true) {
            final long result = snapshotPublication.offer(buffer, 0, length);
            if (result > 0) {
                // The baseline for the next snapshot, on EVERY member and whoever
                // asked for this one.
                snapshotCadence.snapshotTaken(cluster.logPosition(), System.nanoTime());
                return;
            }
            if (result == ExclusivePublication.CLOSED
                    || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("failed to offer assets snapshot: " + result);
            }
            idleStrategy.idle();
        }
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        this.isLeader = newRole == Cluster.Role.LEADER;
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
        // missing it leaves a leader whose idle-path drain never fires.
        drainTimerArmed = false;
    }

    @Override
    public void onTerminate(Cluster cluster) {
        readiness.stopping();
        if (logPruner != null) {
            logPruner.stop();
        }
        if (nodeEndpoint != null) {
            nodeEndpoint.stop();
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
        // A snapshot is one logical message (possibly fragmented); reassemble it, then decode.
        final FragmentHandler handler = (buffer, offset, len, header) -> {
            collector.putBytes(0, buffer, offset, len);
            length.value = len;
        };
        final io.aeron.FragmentAssembler assembler = new io.aeron.FragmentAssembler(handler);
        idleStrategy.reset();
        while (true) {
            final int fragments = snapshotImage.poll(assembler, 1);
            if (length.value > 0) {
                break;
            }
            if (snapshotImage.isEndOfStream()) {
                break;
            }
            idleStrategy.idle(fragments);
        }
        if (length.value > 0) {
            BalanceSnapshotCodec.deserialize(collector, 0, length.value, engine);
        }
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
     * Handle a Subscribe message if that is what this is.
     *
     * @return true when the message was a Subscribe and must NOT be dispatched to the engine
     */
    private boolean applyIfSubscribe(ClientSession session, DirectBuffer buffer, int offset, int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return false;
        }
        peekHeaderDecoder.wrap(buffer, offset);
        if (peekHeaderDecoder.templateId() != SubscribeDecoder.TEMPLATE_ID
                || peekHeaderDecoder.schemaId() != SubscribeDecoder.SCHEMA_ID) {
            return false;
        }
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
        return true;
    }

    /**
     * Mark the session's queue resumable if this message is a QueryFeedPosition.
     *
     * <p>QueryFeedPosition is the first step of a consumer's RESUME protocol: the bridge sends it at
     * every epoch start and rebuilds its skip-filter from the FeedPositionReport answer, so a sender
     * has proven it can lose this session and resynchronise. That proof is what licenses
     * {@link #appendAfterFreeingSpace} to close the session instead of blocking the engine when its
     * queue hits the ceiling. The mark is a transport concern like Subscribe — leader-local, never
     * snapshotted — but unlike Subscribe the message itself is FOR the engine, which answers it, so it
     * is peeked here and still dispatched.</p>
     */
    private void markIfFeedConsumer(ClientSession session, DirectBuffer buffer, int offset, int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        peekHeaderDecoder.wrap(buffer, offset);
        if (peekHeaderDecoder.templateId() != QueryFeedPositionDecoder.TEMPLATE_ID
                || peekHeaderDecoder.schemaId() != QueryFeedPositionDecoder.SCHEMA_ID) {
            return;
        }
        if (isLeader && cluster != null) {
            refreshSessionsIfNeeded();
            final SessionEgressQueue queue = egressBySessionId.get(session.id());
            if (queue != null) {
                queue.markResumable();
            }
        }
    }

    /**
     * The queue hit its growth ceiling: this session's consumer has been unable to keep up for a long
     * time, and what happens next depends on whether that consumer can survive losing its session.
     *
     * <p><b>Resumable consumer (it sent QueryFeedPosition): close the session, keep the engine.</b>
     * A consumer with a resume protocol — the bridge: {@code SessionClosedException} -> epoch restart
     * -> QueryFeedPosition/FeedPositionReport -> skip-filter, with AE idempotency underneath — loses
     * NOTHING when its session dies: it replays from positions. Blocking the whole engine for it
     * would trade cluster availability for a guarantee it does not need — and that trade can
     * livelock: an engine parked here rejects ingress, so a consumer that is itself waiting on this
     * engine can never drain the very queue the engine is blocked on. {@link ClientSession#close()}
     * goes through the consensus module, so followers observe the close deterministically via the
     * log.</p>
     *
     * <p><b>Everything else: block on that one session until it frees space,</b> exactly as the old
     * inline path blocked — money egress to a consumer without a resume protocol (OMS, load
     * generators) is never shed. Only that session's producer stalls, and only in this far-edge case;
     * the other sessions keep their queued frames.</p>
     */
    private void appendAfterFreeingSpace(SessionEgressQueue queue, MutableDirectBuffer buffer,
                                         int offset, int length) {
        if (queue.isResumable()) {
            resumableSessionsClosed++;
            System.err.println("CRITICAL: assets egress queue full for RESUMABLE session=" + queue.sessionId()
                    + " via=" + queue.session().responseChannel()
                    + " (pending=" + queue.pendingBytes() + "B, backPressured=" + queue.backPressureCount()
                    + ") — closing the session instead of blocking the engine; the consumer resyncs on reconnect");
            queue.reset();
            try {
                queue.session().close();
            } catch (final RuntimeException ignored) {
                // already closing: the queue is empty either way and later drains discard on CLOSED
            }
            return; // engine keeps flowing; the un-queued frame is safe — resync replays from positions
        }
        final long n = ++egressStallCount;
        if (n == 1 || n % 1000 == 0) {
            System.err.println("CRITICAL: assets egress queue full for session=" + queue.sessionId()
                    + " via=" + queue.session().responseChannel()
                    + " (pending=" + queue.pendingBytes() + "B, backPressured="
                    + queue.backPressureCount() + ", stalls=" + n
                    + ") — blocking the engine until the consumer drains; nothing is dropped");
        }
        idleStrategy.reset();
        while (!queue.append(buffer, offset, length)) {
            if (queue.drain() > 0) {
                idleStrategy.reset();
            } else {
                idleStrategy.idle();
            }
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
                queue = new SessionEgressQueue(session);
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
        while (!cluster.scheduleTimer(EGRESS_DRAIN_TIMER_ID, deadline)) {
            idleStrategy.idle();
        }
        drainTimerArmed = true;
    }

    /** Exposed for tests / diagnostics. */
    public AssetsEngine engine() {
        return engine;
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
