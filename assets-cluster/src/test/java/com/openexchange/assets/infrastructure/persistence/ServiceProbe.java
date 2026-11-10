// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.cluster.NodeReadiness;
import com.openexchange.cluster.SnapshotEnvelope;
import org.agrona.ExpandableArrayBuffer;

import java.lang.reflect.Field;

/**
 * RCS S4 W0: the package-private reads the cluster-level restore tests need from a live {@link AssetsClusteredService}
 * (its snapshot image, readiness, term id, halt seam). Public test helper; the service itself is not changed.
 */
public final class ServiceProbe {

    private ServiceProbe() {
    }

    /** The service's snapshot bytes exactly as {@code onTakeSnapshot} would write them (kit envelope + v5 payload). */
    public static byte[] image(final AssetsClusteredService service) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
        final int n = AssetsClusteredService.encodeSnapshot(service.engine(), buffer, new SnapshotEnvelope());
        final byte[] out = new byte[n];
        buffer.getBytes(0, out);
        return out;
    }

    private static Object field(final AssetsClusteredService service, final String name) {
        try {
            final Field f = AssetsClusteredService.class.getDeclaredField(name);
            f.setAccessible(true);
            return f.get(service);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError("cannot read AssetsClusteredService." + name, e);
        }
    }

    /** {@code /ready} as the node endpoint computes it. */
    public static boolean ready(final AssetsClusteredService service) {
        return ((NodeReadiness)field(service, "readiness")).ready();
    }

    /** The service's {@code isLeader} flag (set from the role in {@code onStart} and {@code onRoleChange}). */
    public static boolean isLeader(final AssetsClusteredService service) {
        return (Boolean)field(service, "isLeader");
    }

    public static String readiness(final AssetsClusteredService service) {
        return ((NodeReadiness)field(service, "readiness")).describe();
    }

    /** The service's current leadership term id ({@code -1} until a term event, ACS:132). */
    public static long leadershipTermId(final AssetsClusteredService service) {
        return (Long)field(service, "leadershipTermId");
    }

    /** The id the service's last sweep arm passed to {@code scheduleTimer} ({@code -1} before any arm; D5). */
    public static long lastArmedSweepTimerId(final AssetsClusteredService service) {
        return (Long)field(service, "lastArmedSweepTimerId");
    }

    /** The sweep id this service's current term must arm ({@code SWEEP_TIMER_BASE + term * stride}, RC1 S5). */
    public static long expectedSweepTimerId(final AssetsClusteredService service) {
        return AssetsClusteredService.SWEEP_TIMER_BASE
                + Math.max(0L, leadershipTermId(service)) * AssetsClusteredService.TIMER_ID_TERM_STRIDE;
    }

    /** Replaces the halt seam so a refusal is recorded instead of killing the surefire fork. */
    public static void swapFailFast(final AssetsClusteredService service, final Runnable failFast) {
        service.failFast = failFast;
    }

    // ------------------------------------------------------------------------------------------ RCS S4 W3

    private static RcsCounters counters(final AssetsClusteredService service) {
        return (RcsCounters)field(service, "rcsCounters");
    }

    /** An S4 counter by its metric name ({@code rcs_restore_path}, {@code rcs_virtual_genesis_total}, ...). */
    public static long rcs(final AssetsClusteredService service, final String metricName) {
        for (final RcsCounters.C c : RcsCounters.C.values()) {
            if (c.metricName().equals(metricName)) {
                return counters(service).get(c);
            }
        }
        throw new IllegalArgumentException("no RCS counter " + metricName);
    }

    /** {@code rcs_restore_candidates_refused_total{reason}}. */
    public static long refused(final AssetsClusteredService service, final String reason) {
        return counters(service).refused(RcsCounters.Refusal.valueOf(reason));
    }

    /** {@code rcs_restore_conflict_total{EQUAL|SKIP|TAIL}}. */
    public static long conflicts(final AssetsClusteredService service, final String at) {
        return counters(service).conflicts(RcsCounters.ConflictAt.valueOf(at));
    }

    public static boolean skipWindowActive(final AssetsClusteredService service) {
        return service.skipWindowActive();
    }

    /**
     * Switches the named negative-control seams of {@code ShadowRestore.Faults} on (field names, e.g.
     * {@code tailIterationOff}); called before the container starts, so the next {@code onStart} sees them.
     */
    public static void faults(final AssetsClusteredService service, final String... names) {
        final ShadowRestore.Faults f = ShadowRestore.Faults.testing();
        for (final String name : names) {
            try {
                final Field flag = ShadowRestore.Faults.class.getDeclaredField(name);
                flag.setAccessible(true);
                flag.setBoolean(f, true);
            } catch (final ReflectiveOperationException e) {
                throw new AssertionError("no fault seam " + name, e);
            }
        }
        service.shadowFaults = f;
    }

    /** 11.6 runtime second net: the pruner gets this watermark instead of the ShadowFloor. */
    public static void prunerWatermarkOverride(final AssetsClusteredService service,
                                               final java.util.function.LongSupplier watermark) {
        service.prunerWatermarkOverride = watermark;
    }

    /** The engine's replicated money-journal seq ({@code AssetsShadowIdentity.journalSeq}; read at rest only). */
    public static long journalSeq(final AssetsClusteredService service) {
        return new AssetsShadowIdentity(service.engine()).journalSeq();
    }

    /** D4: whether this node's housekeeping currently holds its archive's one purge owner ({@code ShadowRetention}). */
    public static boolean ownsPurge(final AssetsClusteredService service) {
        final MemberShadowHousekeeping hk = (MemberShadowHousekeeping)field(service, "shadowHousekeeping");
        return hk != null && hk.ownsRetention();
    }

    /**
     * 11.2 egress oracle (FIX-5): from now on every frame this member's publisher emits (before the leader gate) is
     * copied into the returned list, with its channel and routing; {@link #untapEgress} stops it.
     */
    public static java.util.List<String> tapEgress(final AssetsClusteredService service) {
        final java.util.List<String> frames = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        service.egressTap = (buffer, offset, length, channel, routing) -> {
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b, 0, length);
            frames.add(channel + "/" + routing + "/" + java.util.HexFormat.of().formatHex(b));
        };
        return frames;
    }

    public static void untapEgress(final AssetsClusteredService service) {
        service.egressTap = null;
    }

    /** The node's {@code ShadowFloor} (the pruner's watermark), or null when the floor is not wired. */
    public static com.openexchange.cluster.shadow.ShadowFloor shadowFloor(final AssetsClusteredService service) {
        final MemberShadowHousekeeping hk = (MemberShadowHousekeeping)field(service, "shadowHousekeeping");
        return hk == null ? null : hk.floor();
    }

    /** Purge passes the node's {@code SnapshotLogPruner} ran (-1 when there is no pruner). */
    public static long prunesRun(final AssetsClusteredService service) {
        final com.openexchange.cluster.SnapshotLogPruner p =
                (com.openexchange.cluster.SnapshotLogPruner)field(service, "logPruner");
        return p == null ? -1 : p.prunesRun();
    }

    /** {@code {termId, base, recordingId, nextBase}} of every valid term entry, as the restore reads them (6.1). */
    public static java.util.List<long[]> restoreTerms(final java.io.File clusterDir) {
        final java.util.List<long[]> out = new java.util.ArrayList<>();
        for (final ShadowRestore.Term t : ShadowRestore.readTerms(clusterDir)) {
            out.add(new long[] {t.termId(), t.base(), t.recordingId(), t.nextBase()});
        }
        return out;
    }

    /** The 6.4 single-recording precondition over the term entries of {@code clusterDir}: null or the refusal text. */
    public static String singleRecording(final java.io.File clusterDir, final long lo, final long hi) {
        return ShadowRestore.singleRecording(ShadowRestore.readTerms(clusterDir), lo, hi);
    }

    /** The 7.1 precondition over {@code clusterDir}'s term entries and a log start: null or why not. */
    public static String virtualGenesisPrecondition(final java.io.File clusterDir, final long logStart, final long pCm) {
        return ShadowRestore.virtualGenesisPrecondition(ShadowRestore.readTerms(clusterDir), logStart, pCm);
    }

    // ------------------------------------------------------------------------------------------ RCS S4 W4

    /** Whether the replicated money-journal switch is on in this member's engine. */
    public static boolean journalEnabled(final AssetsClusteredService service) {
        return service.engine().isMoneyJournalEnabled();
    }

    /**
     * Review F8, contract 6.4: {@code ShadowRestore.Faults.afterTailFrames(n, hook)}; the next {@code onStart}'s tail
     * runs {@code hook} on the service thread after its {@code n}-th applied frame.
     */
    public static void afterTailFrames(final AssetsClusteredService service, final long n, final Runnable hook) {
        service.shadowFaults = ShadowRestore.Faults.afterTailFrames(n, hook);
    }

    /** Sessions with an egress queue on this service ({@code egressBySessionId.size()}). */
    public static int egressQueues(final AssetsClusteredService service) {
        final Object map = field(service, "egressBySessionId");
        return ((org.agrona.collections.Long2ObjectHashMap<?>)map).size();
    }

    /** 11.13 Q9 drill: {@code hook} runs on the service thread right after each {@code onTakeSnapshot} returns. */
    public static void afterTakeSnapshot(final AssetsClusteredService service, final Runnable hook) {
        final ShadowRestore.Faults f = service.shadowFaults == ShadowRestore.Faults.NONE
                ? ShadowRestore.Faults.testing() : service.shadowFaults;
        f.afterTakeSnapshot = hook;
        service.shadowFaults = f;
    }

    /** 11.2 100k leg: {@code observer} sees every applied tail frame (the egress-queue and heap sampler). */
    public static void tailObserver(final AssetsClusteredService service, final java.util.function.LongConsumer observer) {
        final ShadowRestore.Faults f = service.shadowFaults == ShadowRestore.Faults.NONE
                ? ShadowRestore.Faults.testing() : service.shadowFaults;
        f.tailFrameObserver = observer;
        service.shadowFaults = f;
    }

    /** The node's snapshot cadence (the standby counters and the byte baseline). */
    public static com.openexchange.cluster.SnapshotCadence cadence(final AssetsClusteredService service) {
        return (com.openexchange.cluster.SnapshotCadence)field(service, "snapshotCadence");
    }

    /** {@code assets_backup_queries_denied_total} of this node (0 without ClusterConfig's table). */
    public static long backupQueriesDenied(final AssetsClusteredService service) {
        final Object table = field(service, "sessionAuthorisation");
        return table == null ? 0 : ((com.openexchange.cluster.auth.ClassAuthorisationService)table)
                .deniedCount(com.openexchange.cluster.auth.ClassAuthorisationService.BACKUP_QUERY);
    }
}
