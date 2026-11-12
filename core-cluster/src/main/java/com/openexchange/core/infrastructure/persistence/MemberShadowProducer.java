// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.ConsensusReadiness;
import com.openexchange.cluster.NodeReadiness;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ProducerDesignation;
import com.openexchange.cluster.shadow.ProducerWindowGuard;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowProducerSettings;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.shadow.ShadowWriterThread;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The follower producer of a member: one designated cluster follower per term writes an ALIGNED shadow of its own
 * engine image at exactly P_cm, serialized inside its own {@code onTakeSnapshot} after the stub was offered, into its
 * own archive. Nothing is serialized anywhere else: the service duty cycle polls the log before its background work,
 * so only the callback sees the engine as it is when the ClusterAction frame is applied.
 *
 * <p>Designation: the member whose id follows the leader's in the static members table, wrapping
 * ({@link ProducerDesignation}). The leader id of the current term comes from the leader's own {@code CommitPosition}
 * message ({@link ConsensusReadiness#leader()}), cross-checked against the log's {@code NewLeadershipTermEvent} when
 * this process saw that frame. Designation decides only whether THIS node serializes a copy of its own image into its
 * own archive: no peer reads it and no replicated value depends on it (a copy may be missing on one node after any
 * skip, busy or abort). A node restarted mid-term never sees the term's frame again, so the wire is the primary
 * source: a restarted designated follower produces at the next P_cm without an election.</p>
 *
 * <p>The guard is a conjunction read at each ClusterAction on the service thread, first false leg names the skip, every
 * skip is counted and none touches readiness or the snapshot: shadow mode and a stub written, no skip window, readiness
 * ready (as the last readiness poll outside the consensus module's SNAPSHOT state found it: during the action itself
 * the module is in SNAPSHOT and readiness reads consensus-inactive on every node by construction), role FOLLOWER, leader known for this term, wire and log agree, designated, writer armed, writer idle, the
 * client liveness bound, the log window budget, and the image fitting the buffer (an overflow inside the serialize is
 * caught here: the callback returns normally, the member never dies for a shadow).</p>
 *
 * <p>The service thread pays the serialize only; the self-check, the offer, the recording and the registration run on
 * the writer thread ({@code shadow-writer-F<memberId>}) with its own archive client. A write in flight when this node
 * becomes LEADER is aborted and the writer purges its partial recording.</p>
 */
final class MemberShadowProducer {

    /** Why an ALIGNED was not produced at a ClusterAction ({@code shadow_producer_skipped_total{reason}}). */
    enum Skip {
        NOT_SHADOW_MODE("not_shadow_mode"),
        FULL_IMAGE("full_image"),
        PRODUCER_NONE("producer_none"),
        SKIP_WINDOW("skip_window"),
        NOT_READY("not_ready"),
        LEADER("leader"),
        LEADER_UNKNOWN("leader_unknown"),
        LEADER_MISMATCH("leader_mismatch"),
        NOT_DESIGNATED("not_designated"),
        UNARMED("unarmed"),
        BUSY("busy"),
        LIVENESS_BOUND("liveness_bound"),
        WINDOW_BUDGET("window_budget"),
        CAPACITY("capacity");

        final String label;

        Skip(final String label) {
            this.label = label;
        }
    }

    /** What the housekeeping armed after the ADOPT: the writer, its thread and the image buffer. */
    record Armed(ShadowWriter writer, ShadowWriterThread thread, ShadowImageBuffer image, long firstProducerSeq) {
    }

    /** The service-side state the guard reads; every supplier runs on the service thread. */
    record Host(SnapshotMode mode, ShadowProducerSettings settings, NodeReadiness readiness, BooleanSupplier ready,
                Supplier<ConsensusReadiness> consensus, BooleanSupplier isLeader, LongSupplier leadershipTermId,
                LongSupplier termBaseLogPosition, BooleanSupplier skipWindowActive,
                Supplier<MemberShadowHousekeeping> housekeeping, Supplier<ShadowRestore.Faults> faults,
                long alarmWindowMs) {
    }

    private final Host host;
    private final AtomicLongArray skipped = new AtomicLongArray(Skip.values().length);
    private final AtomicLong written = new AtomicLong();
    private final AtomicLong serialized = new AtomicLong();
    private final AtomicLong serializeFailed = new AtomicLong();
    private final AtomicLong deferredWindow = new AtomicLong();
    private final AtomicLong abortedLeader = new AtomicLong();
    private final AtomicLong leaderMismatch = new AtomicLong();
    private final AtomicLong serializedOutsideCallback = new AtomicLong();
    private volatile long serializeMsLast = -1;
    private volatile long lastPosition = -1;
    private volatile int designatedGauge;
    private volatile String lastSkip = "none";
    /** True from a role change to LEADER that asked the writer to abort until the writer's result arrives. */
    private volatile boolean abortRequestedForLeader;
    private volatile long abortRequestedNs = -1;
    private volatile long abortLatencyNsLast = -1;

    private ProducerDesignation designation;
    private String designationMembers;
    private ProducerWindowGuard guard;
    private long logTermLeaderTerm = -1;
    private int logTermLeaderId = -1;
    private long mismatchLineTerm = -1;
    private boolean capacityLinePrinted;
    private final long startNs = System.nanoTime();
    private long lastDesignationNs;
    private boolean designationSampled;
    private long designatedSinceNs = -1;
    private volatile long lastWrittenNs = -1;
    private long lastQuietLineNs = -1;

    MemberShadowProducer(final Host host) {
        this.host = Objects.requireNonNull(host, "host");
    }

    // ------------------------------------------------------------------------------------------------ inputs

    /** The log frame's leader id for the term (the cross-check); seen live and during replay, in log order. */
    void onNewLeadershipTermEvent(final long leadershipTermId, final int leaderMemberId) {
        logTermLeaderTerm = leadershipTermId;
        logTermLeaderId = leaderMemberId;
    }

    /** The log term leader as this process saw it: {@code {term, id}}, {@code {-1, -1}} until a frame was seen. */
    long[] logTermLeader() {
        return new long[] {logTermLeaderTerm, logTermLeaderId};
    }

    /** The members table (static, identical on every node), from the consensus context; null without one. */
    void members(final String clusterMembers) {
        if (clusterMembers != null && !clusterMembers.equals(designationMembers)) {
            designation = ProducerDesignation.ofClusterMembers(clusterMembers);
            designationMembers = clusterMembers;
        }
    }

    /** The log channel's receiver window, the guard's {@code W}; built once. */
    void receiverWindow(final long windowBytes) {
        final ShadowProducerSettings s = host.settings();
        guard = new ProducerWindowGuard(windowBytes, s.windowBudgetPct(), s.rateWindowMs(), s.serializeEstimateMs());
    }

    long receiverWindow() {
        return guard == null ? -1 : guard.windowBytes();
    }

    /**
     * The service's background work, once per duty cycle at depth 0: the rate sample (the guard keeps at most one a
     * second) and the designation gauge (at most every 10 ms), plus the producer-side diagnostic line.
     */
    void onBackgroundWork(final long nowNs, final long logPosition, final int selfMemberId) {
        if (host.mode() != SnapshotMode.SHADOW || !host.settings().enabled()) {
            return;
        }
        if (guard != null) {
            guard.sample(nowNs, logPosition);
        }
        if (designationSampled && nowNs - lastDesignationNs < TimeUnit.MILLISECONDS.toNanos(10)) {
            return;
        }
        designationSampled = true;
        lastDesignationNs = nowNs;
        final int d = designationNow(selfMemberId);
        designatedGauge = d;
        if (d == 1 && designatedSinceNs < 0) {
            designatedSinceNs = nowNs;
        }
        final long windowNs = TimeUnit.MILLISECONDS.toNanos(host.alarmWindowMs());
        final long since = Math.max(designatedSinceNs, lastWrittenNs);
        if (designatedSinceNs >= 0 && nowNs - since >= windowNs
                && (lastQuietLineNs < 0 || nowNs - lastQuietLineNs >= windowNs)) {
            lastQuietLineNs = nowNs;
            System.err.println("[SHADOW-PRODUCER] CRITICAL: designated follower wrote no ALIGNED for "
                    + TimeUnit.NANOSECONDS.toMinutes(nowNs - since) + " min: last skip " + lastSkip);
        }
        if (d != 1) {
            designatedSinceNs = -1;
        }
    }

    /** {@code shadow_producer_designated}: 1 designated and FOLLOWER, 0 not, -1 leader unknown. */
    private int designationNow(final int self) {
        if (host.isLeader().getAsBoolean()) {
            return 0;
        }
        final int leader = leaderId();
        if (leader < 0) {
            return -1;
        }
        return designation != null && designation.isDesignated(self, leader) ? 1 : 0;
    }

    /**
     * The current term's leader id from the commit wire, or -1 when the wire has none for the service's current term.
     * The negative-control seams: {@code leaderFromLogOnly} reads the log frame instead (the hole a mid-term restart
     * opens), {@code wireLeaderOverride} replaces the wire id (a mismatch).
     */
    private int leaderId() {
        final ShadowRestore.Faults f = host.faults().get();
        final long term = host.leadershipTermId().getAsLong();
        if (term < 0) {
            return -1;
        }
        if (f.leaderFromLogOnly) {
            return logTermLeaderTerm == term ? logTermLeaderId : -1;
        }
        final ConsensusReadiness cr = host.consensus().get();
        final ConsensusReadiness.LeaderCheckpoint cp = cr == null ? null : cr.leader();
        if (cp == null || cp.term() != term || cp.leaderMemberId() < 0) {
            return -1;
        }
        return f.wireLeaderOverride >= 0 ? f.wireLeaderOverride : cp.leaderMemberId();
    }

    /** The current leader's archive control endpoint from the commit wire (the puller's skip), or null. */
    String leaderArchiveEndpoint() {
        final ConsensusReadiness cr = host.consensus().get();
        final ConsensusReadiness.LeaderCheckpoint cp = cr == null ? null : cr.leader();
        final ProducerDesignation d = designation;
        return cp == null || d == null || cp.leaderMemberId() < 0 ? null : d.archiveEndpoint(cp.leaderMemberId());
    }

    // ------------------------------------------------------------------------------------------------ the action

    /**
     * Inside {@code onTakeSnapshot}, after the stub was offered: the guard, then the serialize and the hand-off.
     * Never throws a {@code RuntimeException} (a producer failure is not a snapshot failure); an {@code Error}
     * propagates.
     *
     * @return {@code aligned} or {@code skipped:<reason>} for the snapshot line
     */
    String onClusterAction(final long pCm, final boolean stub, final boolean inWindow, final int selfMemberId,
                           final long clusterTime, final long livenessTimeoutNs, final int depth,
                           final CoreLogApplier applier, final CoreShadowIdentity identity) {
        final ShadowRestore.Faults f = host.faults().get();
        if (host.mode() != SnapshotMode.SHADOW) {
            skipped.incrementAndGet(Skip.NOT_SHADOW_MODE.ordinal());
            return "off";
        }
        if (!stub) {
            return skip(Skip.FULL_IMAGE, pCm, "");
        }
        if (!host.settings().enabled()) {
            return skip(Skip.PRODUCER_NONE, pCm, "");
        }
        if ((inWindow || host.skipWindowActive().getAsBoolean()) && !f.alignedGuardOff) {
            return skip(Skip.SKIP_WINDOW, pCm, "");
        }
        if ((!host.ready().getAsBoolean() || f.notReadyInjected) && !f.readyGuardOff) {
            return skip(Skip.NOT_READY, pCm, host.readiness().describe());
        }
        if (host.isLeader().getAsBoolean() && !f.producerOnLeader) {
            return skip(Skip.LEADER, pCm, "");
        }
        final long term = host.leadershipTermId().getAsLong();
        // negative control only: the role leg off, the leader names itself and takes the designation
        final boolean leaderControl = f.producerOnLeader && host.isLeader().getAsBoolean();
        final int leader = leaderControl ? selfMemberId : leaderId();
        if (leader < 0) {
            return skip(Skip.LEADER_UNKNOWN, pCm, "term " + term);
        }
        if (!f.leaderFromLogOnly && logTermLeaderTerm == term && logTermLeaderId >= 0 && logTermLeaderId != leader) {
            leaderMismatch.incrementAndGet();
            if (mismatchLineTerm != term) {
                mismatchLineTerm = term;
                System.err.println("[SHADOW-PRODUCER] CRITICAL: leader id disagrees: commit wire says " + leader
                        + ", term event says " + logTermLeaderId + " for term " + term + ": not producing");
            }
            return skip(Skip.LEADER_MISMATCH, pCm, "wire " + leader + " log " + logTermLeaderId);
        }
        if (!f.designationOff && !leaderControl
                && (designation == null || !designation.isDesignated(selfMemberId, leader))) {
            return skip(Skip.NOT_DESIGNATED, pCm, "leader " + leader + " designated "
                    + (designation == null ? -1 : designation.designated(leader)));
        }
        final MemberShadowHousekeeping hk = host.housekeeping().get();
        final Armed armed = hk == null ? null : hk.armedProducer();
        if (armed == null || guard == null) {
            final boolean late = System.nanoTime() - startNs > TimeUnit.MILLISECONDS.toNanos(host.alarmWindowMs());
            if (late) {
                System.err.println("[SHADOW-PRODUCER] CRITICAL: the designated follower's writer is not armed at P_cm="
                        + pCm + " (no ADOPT seen, or the housekeeping is not running)");
            }
            return skip(Skip.UNARMED, pCm, guard == null ? "no receiver window" : "writer not armed");
        }
        if (!armed.thread().isIdle()) {
            return skip(Skip.BUSY, pCm, "");
        }
        final long nowNs = System.nanoTime();
        final long tLast = guard.lastSerializeNs();
        if (!f.livenessBoundOff && ProducerWindowGuard.livenessBound(tLast, livenessTimeoutNs)) {
            System.err.println("[SHADOW-PRODUCER] CRITICAL: skipped the ALIGNED at P_cm=" + pCm + ": T_last="
                    + TimeUnit.NANOSECONDS.toMillis(tLast) + " ms is above half the client liveness timeout "
                    + TimeUnit.NANOSECONDS.toMillis(livenessTimeoutNs) + " ms (a longer serialize kills the member)");
            return skip(Skip.LIVENESS_BOUND, pCm, "T_last=" + TimeUnit.NANOSECONDS.toMillis(tLast) + "ms liveness="
                    + TimeUnit.NANOSECONDS.toMillis(livenessTimeoutNs) + "ms");
        }
        if (!f.windowGuardOff && ProducerWindowGuard.defer(guard.rateBytesPerSecond(nowNs), tLast,
                guard.windowBytes(), guard.budgetPct())) {
            deferredWindow.incrementAndGet();
            return skip(Skip.WINDOW_BUDGET, pCm, guard.describe(nowNs));
        }
        final long termBase = host.termBaseLogPosition().getAsLong();
        final ShadowImageBuffer image = armed.image();
        final long t0 = System.nanoTime();
        final int n;
        try {
            n = applier.serialize(image.payloadView());
            if (n > image.payloadCapacity()) {
                throw new IndexOutOfBoundsException("image " + n + " B above the buffer " + image.payloadCapacity()
                        + " B");
            }
            if (f.producerSerializeDelayMs > 0) {
                sleep(f.producerSerializeDelayMs); // test seam only: a slow serialize on the service thread
            }
        } catch (final IndexOutOfBoundsException overflow) {
            if (f.capacityGuardOff) {
                throw overflow; // negative control: the catch removed, the callback fails
            }
            return capacity(pCm, image, overflow);
        } catch (final RuntimeException failed) {
            if (overflowIn(failed)) {
                if (f.capacityGuardOff) {
                    throw failed;
                }
                return capacity(pCm, image, failed);
            }
            serializeFailed.incrementAndGet();
            System.err.println("[SHADOW-PRODUCER] CRITICAL: the ALIGNED serialize at P_cm=" + pCm + " failed: "
                    + failed + " (the cluster snapshot is unaffected: the stub was offered before)");
            return "skipped:serialize_failed";
        }
        final long tNs = System.nanoTime() - t0;
        image.setPayloadLength(n);
        guard.serialized(tNs);
        serializeMsLast = TimeUnit.NANOSECONDS.toMillis(tNs);
        serialized.incrementAndGet();
        if (depth == 0) {
            serializedOutsideCallback.incrementAndGet();
        }
        final ShadowWriterThread.Request request = ShadowWriterThread.Request.of(image, CoreLogApplier.ENGINE_ID_CORE,
                ShadowKind.ALIGNED, new LogCursor(pCm, clusterTime, term, Math.max(-1L, termBase)), identity);
        if (!armed.thread().offer(request)) {
            return skip(Skip.BUSY, pCm, "the writer refused the hand-off");
        }
        lastSkip = "none";
        return "aligned";
    }

    private String capacity(final long pCm, final ShadowImageBuffer image, final Throwable overflow) {
        if (!capacityLinePrinted) {
            capacityLinePrinted = true;
            System.err.println("[SHADOW-PRODUCER] CRITICAL: the engine image does not fit SHADOW_PRODUCER_BUFFER_MB ("
                    + image.payloadCapacity() + " B) at P_cm=" + pCm + ": " + overflow + " (raise the setting; the"
                    + " stub snapshot is intact)");
        }
        return skip(Skip.CAPACITY, pCm, "buffer " + image.payloadCapacity() + " B");
    }

    /** An {@code IndexOutOfBoundsException} anywhere in the cause chain (the codec has no size function). */
    static boolean overflowIn(final Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof IndexOutOfBoundsException) {
                return true;
            }
        }
        return false;
    }

    private String skip(final Skip reason, final long pCm, final String detail) {
        skipped.incrementAndGet(reason.ordinal());
        lastSkip = reason.label;
        System.out.println("[SHADOW-PRODUCER] skipped the ALIGNED at P_cm=" + pCm + ": " + reason.label
                + (detail.isEmpty() ? "" : " (" + detail + ")"));
        return "skipped:" + reason.label;
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------------------------------------ role, result

    /** On the transition to LEADER: abort a write in flight (its partial recording is purged by the writer). */
    void onRoleChange(final boolean leader) {
        if (!leader || host.faults().get().leaderFinishesWrite) {
            return;
        }
        final MemberShadowHousekeeping hk = host.housekeeping().get();
        final Armed armed = hk == null ? null : hk.armedProducer();
        if (armed != null && !armed.thread().isIdle()) {
            abortRequestedForLeader = true;
            abortRequestedNs = System.nanoTime();
            armed.writer().requestAbort();
            System.out.println("[SHADOW-PRODUCER] this node became LEADER with an ALIGNED write in flight: abort"
                    + " requested (the partial recording is purged)");
        }
    }

    /** The writer thread's result listener. */
    void onWriteResult(final ShadowWriterThread.Request request, final ShadowWriter.WriteResult result) {
        final boolean abortForLeader = abortRequestedForLeader;
        abortRequestedForLeader = false;
        if (result.ok()) {
            written.incrementAndGet();
            lastPosition = request.at().logPosition();
            lastWrittenNs = System.nanoTime();
            System.out.println("[SHADOW-PRODUCER] wrote the ALIGNED at P_cm=" + request.at().logPosition() + ": "
                    + result);
            return;
        }
        if (abortForLeader && result.reason() == ShadowWriter.Reason.ABORTED) {
            abortedLeader.incrementAndGet();
            abortLatencyNsLast = abortRequestedNs < 0 ? -1 : System.nanoTime() - abortRequestedNs;
            System.out.println("[SHADOW-PRODUCER] the ALIGNED at P_cm=" + request.at().logPosition() + " was aborted on"
                    + " becoming LEADER after " + TimeUnit.NANOSECONDS.toMillis(abortLatencyNsLast) + " ms: " + result);
            return;
        }
        if (result.reason() == ShadowWriter.Reason.NOT_MONOTONE) {
            System.out.println("[SHADOW-PRODUCER] the ALIGNED at P_cm=" + request.at().logPosition() + " was already"
                    + " written by this producer: " + result);
            return;
        }
        System.err.println("[SHADOW-PRODUCER] CRITICAL: the ALIGNED write at P_cm=" + request.at().logPosition()
                + " failed: " + result);
    }

    // ------------------------------------------------------------------------------------------------ metrics

    long skipped(final Skip reason) {
        return skipped.get(reason.ordinal());
    }

    long written() {
        return written.get();
    }

    long serialized() {
        return serialized.get();
    }

    long serializedOutsideCallback() {
        return serializedOutsideCallback.get();
    }

    long abortedLeader() {
        return abortedLeader.get();
    }

    long abortLatencyNsLast() {
        return abortLatencyNsLast;
    }

    long deferredWindow() {
        return deferredWindow.get();
    }

    int designated() {
        return designatedGauge;
    }

    void metrics(final com.openexchange.core.infrastructure.metrics.CoreMetricsBody body) {
        body.gauge("shadow_producer_designated", "1 while this node is the designated follower producer, 0 while not,"
                + " -1 while the current term's leader is unknown", () -> designatedGauge);
        body.counter("shadow_producer_written_total", "ALIGNED shadows this member wrote at P_cm", written::get);
        body.counter("shadow_producer_serialize_total", "ALIGNED serializes inside onTakeSnapshot", serialized::get);
        body.gauge("shadow_producer_serialize_ms_last", "The last ALIGNED serialize on the service thread, ms (-1:"
                + " none)", () -> serializeMsLast);
        body.gauge("shadow_producer_last_position", "P_cm of the last ALIGNED this member wrote (-1: none)",
                () -> lastPosition);
        final Skip[] reasons = Skip.values();
        final String[] names = new String[reasons.length];
        final LongSupplier[] values = new LongSupplier[reasons.length];
        for (int i = 0; i < reasons.length; i++) {
            final Skip r = reasons[i];
            names[i] = r.label;
            values[i] = () -> skipped.get(r.ordinal());
        }
        body.labelledGauge("shadow_producer_skipped_total", "ClusterActions without an ALIGNED, by the first false"
                + " guard leg (cumulative)", "reason", names, values);
        body.counter("shadow_producer_deferred_window_total", "ALIGNED skipped by the log window budget",
                deferredWindow::get);
        body.counter("shadow_producer_aborted_leader_total", "ALIGNED writes aborted on becoming LEADER",
                abortedLeader::get);
        body.counter("shadow_producer_leader_mismatch_total", "ClusterActions where the commit wire and the term event"
                + " named different leaders", leaderMismatch::get);
        body.counter("shadow_producer_serialize_failed_total", "ALIGNED serializes that threw (not a snapshot failure)",
                serializeFailed::get);
    }
}
