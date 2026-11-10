// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.journal.MoneyJournalTail;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowFloor;
import com.openexchange.cluster.shadow.ShadowHousekeeping;
import com.openexchange.cluster.shadow.ShadowRetention;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.shadow.StubPeek;
import io.aeron.Aeron;
import io.aeron.cluster.ConsensusModule;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * 9.1-9.3, the member side of the floor, wired in BOTH {@code SNAPSHOT_MODE}s (D2): the {@link ShadowFloor} the
 * pruner purges behind, its precondition (the newest group's service payload kind, read here with {@link StubPeek}:
 * a FULL image answers P_cm, today's pruning; a stub answers the 9.1 table; unreadable answers 0), and the wake hook
 * ({@code SnapshotLogPruner.onWake}) that runs the kit's {@link ShadowHousekeeping} (catalog scan, paced deep reads,
 * {@link ShadowRetention}, the journal tail behind {@code skipSafe}, read only when the floor will ask for it) on the
 * PRUNER thread. Everything archive-facing is
 * built lazily on that thread, with its own archive client, so nothing here touches the service thread.
 *
 * <p>Fail closed: until the catalog exists (no ADOPT yet: a catalog with {@code genesisId 0} would not mark another
 * genesis FOREIGN, so it must not feed a floor; or the one-purge-owner rule refused, C9) the floor has no scan and
 * answers UNKNOWN, which purges nothing. A hook failure is reported by the pruner as CRITICAL and retried next wake.
 * The floor's suppliers read P_cm from {@code RecordingLog} (read side) and the log start from the archive on every
 * call, never from memory (C4); the payload kind is cached by recording id (a stopped recording's bytes never
 * change).</p>
 *
 * <p>Ownership (C9, D4): {@link #close} (from {@code onTerminate}) releases the archive's one purge owner. The pruner
 * thread may be inside {@link #run} at that moment ({@code SnapshotLogPruner.stop} does not join), so both take one
 * lock: a close that finds the hook running marks it closed and the hook releases on its way out; a closed
 * housekeeping never builds again. A same-JVM restart of the member therefore re-registers.</p>
 */
final class MemberShadowHousekeeping implements Runnable, AutoCloseable {

    private final File clusterDir;
    private final AeronArchive.Context archiveContext;
    private final int clusterId;
    private final SnapshotModeSettings settings;
    private final SnapshotModeCounters snapshotModeCounters;
    private final ShadowCounters shadowCounters;
    private final ShadowVerdicts verdicts;
    private final ShadowHousekeeping.Counters counters;
    private final LongSupplier genesisId;
    private final org.agrona.ErrorHandler fatalErrors;
    private final NanoClock clock = SystemNanoClock.INSTANCE;
    private final ShadowFloor floor;

    private static final long NEVER = Long.MIN_VALUE;

    private volatile MoneyJournalTail.Result journal = new MoneyJournalTail.Unknown("not read yet");
    private long lastJournalReadNs = NEVER;
    private AeronArchive archive;
    private ShadowRetention retention;
    private ShadowHousekeeping housekeeping;
    private String lastRefusal;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean closed;
    private long peekedRecordingId = Long.MIN_VALUE;
    private ShadowFloor.NewestImage peekedKind = ShadowFloor.NewestImage.UNKNOWN;

    /**
     * @param genesisId the engine's recorded genesis nonce as the service last published it (0 before ADOPT)
     */
    MemberShadowHousekeeping(final File clusterDir, final AeronArchive.Context archiveContext, final int clusterId,
                             final SnapshotModeSettings settings, final SnapshotModeCounters snapshotModeCounters,
                             final ShadowCounters shadowCounters, final ShadowVerdicts verdicts,
                             final ShadowHousekeeping.Counters counters, final LongSupplier genesisId,
                             final org.agrona.ErrorHandler fatalErrors) {
        this.clusterDir = Objects.requireNonNull(clusterDir, "clusterDir");
        this.archiveContext = Objects.requireNonNull(archiveContext, "archiveContext");
        this.clusterId = clusterId;
        this.settings = Objects.requireNonNull(settings, "settings");
        this.snapshotModeCounters = Objects.requireNonNull(snapshotModeCounters, "snapshotModeCounters");
        this.shadowCounters = Objects.requireNonNull(shadowCounters, "shadowCounters");
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.genesisId = Objects.requireNonNull(genesisId, "genesisId");
        this.fatalErrors = Objects.requireNonNull(fatalErrors, "fatalErrors");
        this.floor = new ShadowFloor(this::latestSnapshotPosition, this::newestImage,
                h -> ShadowRestore.skipSafe(h, journal), settings.floorMargin(), this::logStartPosition, counters);
    }

    /** The watermark supplier the pruner must hold (4.1 runtime net: {@code watermarkIs(floor())}). */
    ShadowFloor floor() {
        return floor;
    }

    ShadowHousekeeping.Counters counters() {
        return counters;
    }

    /** The pruner's wake hook (9.3). */
    @Override
    public void run() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            runLocked();
        } finally {
            if (closed) {
                release();
            }
            lock.unlock();
        }
    }

    private void runLocked() {
        try {
            if (archive == null) {
                archive = AeronArchive.connect(archiveContext.clone());
            }
            if (housekeeping == null && !build()) {
                return;
            }
            housekeeping.poll();
        } catch (final RuntimeException failed) {
            forwardFatal(failed);
            // a lost archive client is reconnected next wake; the floor keeps its last scan (or none: UNKNOWN)
            CloseHelper.quietClose(archive);
            archive = null;
            CloseHelper.quietClose(retention); // releases the purge owner even when build() failed half way
            retention = null;
            housekeeping = null;
            throw failed;
        }
    }

    private boolean build() {
        final long genesis = genesisId.getAsLong();
        if (genesis <= 0) {
            refusedOnce("[SHADOW-HK] no recorded genesis yet (before ADOPT): the floor stays UNKNOWN, nothing is"
                    + " purged below a stub");
            return false;
        }
        final long timeoutNs = TimeUnit.MILLISECONDS.toNanos(settings.loadTimeoutMs());
        final ShadowCatalog catalog = new ShadowCatalog(archive.context().aeron(), archive,
                CoreLogApplier.ENGINE_ID_CORE, clusterId, genesis, verdicts, ShadowCatalog.DEFAULT_LIST_BATCH,
                clock, timeoutNs, shadowCounters);
        try {
            retention = new ShadowRetention(catalog, floor, settings.retainN(), counters);
        } catch (final IllegalStateException secondOwner) {
            refusedOnce("[SHADOW-HK] CRITICAL: " + secondOwner.getMessage() + "; the floor stays UNKNOWN on this node");
            return false;
        }
        housekeeping = new ShadowHousekeeping(catalog, retention, floor, clock, settings.scanPeriodMs(),
                settings.deepReadPaceMs(), this::refreshJournal, counters);
        System.out.println("[SHADOW-HK] housekeeping armed: archive=" + catalog.archiveId() + " genesisId=" + genesis
                + " retainN=" + settings.retainN() + " margin=" + settings.floorMargin().text() + " scanPeriodMs="
                + settings.scanPeriodMs() + " deepReadPaceMs=" + settings.deepReadPaceMs());
        return true;
    }

    private void refusedOnce(final String line) {
        if (!line.equals(lastRefusal)) {
            lastRefusal = line;
            System.out.println(line);
        }
    }

    /**
     * The housekeeping's refresh step (9.3 (3)): the journal tail is read only when the floor will consult
     * {@code skipSafe}, that is the newest group is a STUB and a VERIFIED journal-enabled shadow sits above P_cm, and
     * then at most once per {@code SHADOW_SCAN_PERIOD_MS} (up to five replay sessions per journal
     * recording per read, the tmpfs churn paces). Otherwise the floor answers before any {@code skipSafe} (FULL
     * -> P_cm, no shadow above P_cm -> the table never asks), and nothing is replayed. A stale tail is a lower bound
     * of the durable seq (it only grows), so it can refuse a safe SKIP candidate for one period, never accept an
     * unsafe one.
     */
    private void refreshJournal() {
        final ShadowCatalog.Scan scan = housekeeping == null ? null : housekeeping.scan();
        final long pCm = latestSnapshotPosition();
        ShadowFloor.NewestImage kind;
        try {
            kind = scan == null || pCm < 0 ? ShadowFloor.NewestImage.UNKNOWN : newestImage();
        } catch (final RuntimeException unreadable) {
            kind = ShadowFloor.NewestImage.UNKNOWN; // the floor answers 0 on its own read; nothing to refresh for
        }
        if (scan == null || !needsJournalTail(kind, pCm, scan.verified())) {
            snapshotModeCounters.increment(SnapshotModeCounters.C.JOURNAL_TAIL_READS_SKIPPED_TOTAL);
            return;
        }
        final long now = clock.nanoTime();
        if (lastJournalReadNs != NEVER
                && now - lastJournalReadNs < TimeUnit.MILLISECONDS.toNanos(settings.scanPeriodMs())) {
            return;
        }
        lastJournalReadNs = now;
        snapshotModeCounters.increment(SnapshotModeCounters.C.JOURNAL_TAIL_READS_TOTAL);
        final MoneyJournalTail.Result r = new MoneyJournalTail(archive.context().aeron(), archive, clock)
                .read(TimeUnit.MILLISECONDS.toNanos(settings.loadTimeoutMs()));
        journal = r;
        ShadowRestore.publishJournalTail(r, snapshotModeCounters);
    }

    /**
     * Whether the floor's table will ask {@code skipSafe} with the journal tail: the newest group is a STUB and some
     * VERIFIED shadow above P_cm has journaling on (a journal-off shadow is skip-safe without the tail, 8.1).
     */
    static boolean needsJournalTail(final ShadowFloor.NewestImage kind, final long pCm,
                                    final java.util.List<com.openexchange.cluster.shadow.LogicalShadow> verified) {
        if (kind != ShadowFloor.NewestImage.STUB || pCm < 0) {
            return false;
        }
        for (final com.openexchange.cluster.shadow.LogicalShadow v : verified) {
            if (v.logPosition() > pCm && v.representative().header().journalEnabledFlag()) {
                return true;
            }
        }
        return false;
    }

    /** P_cm: the newest valid snapshot position in {@code RecordingLog} (read side), or -1. */
    private long latestSnapshotPosition() {
        long latest = -1;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid) {
                    latest = Math.max(latest, e.logPosition);
                }
            }
        }
        return latest;
    }

    /** The local log recording's start position from the archive; throws when it cannot be read (floor: -1). */
    private long logStartPosition() {
        try {
            return readLogStartPosition();
        } catch (final RuntimeException failed) {
            forwardFatal(failed);
            throw failed;
        }
    }

    private long readLogStartPosition() {
        final AeronArchive a = archive;
        if (a == null) {
            throw new IllegalStateException("no archive client yet");
        }
        final long logRecordingId;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            logRecordingId = log.findLastTermRecordingId();
        }
        if (logRecordingId < 0) {
            throw new IllegalStateException("no log recording in " + clusterDir);
        }
        return a.getStartPosition(logRecordingId);
    }

    /**
     * This thread shares the container's Aeron client, so it may be the first to see the driver die. A FATAL Aeron
     * error (driver timeout or loss) goes to the container's error handler, which is the engine lifecycle's: it must
     * halt the node exactly as when the service thread sees it first (EDLPT W9b), never be swallowed as a failed wake.
     */
    private void forwardFatal(final RuntimeException failed) {
        if (failed instanceof io.aeron.exceptions.DriverTimeoutException
                || failed instanceof io.aeron.exceptions.AeronException ae
                && ae.category() == io.aeron.exceptions.AeronException.Category.FATAL) {
            fatalErrors.onError(failed);
        }
    }

    /** Whether this node currently holds its archive's purge owner (D4 restart row). */
    boolean ownsRetention() {
        return retention != null;
    }

    /**
     * The newest snapshot group's service payload kind, the floor's precondition (D2). Runs on the pruner thread
     * inside the purge pass, after this hook connected the archive client.
     */
    private ShadowFloor.NewestImage newestImage() {
        try {
            return readNewestImage();
        } catch (final RuntimeException failed) {
            forwardFatal(failed);
            throw failed;
        }
    }

    private ShadowFloor.NewestImage readNewestImage() {
        long newest = -1;
        long serviceRecordingId = -1;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type != RecordingLog.ENTRY_TYPE_SNAPSHOT || !e.isValid) {
                    continue;
                }
                if (e.logPosition > newest) {
                    newest = e.logPosition;
                    serviceRecordingId = -1;
                }
                if (e.logPosition == newest && e.serviceId != ConsensusModule.Configuration.SERVICE_ID) {
                    serviceRecordingId = e.recordingId;
                }
            }
        }
        if (serviceRecordingId < 0) {
            return unreadable("no service snapshot entry at P_cm=" + newest);
        }
        if (serviceRecordingId == peekedRecordingId) {
            return peekedKind;
        }
        final AeronArchive a = archive;
        if (a == null) {
            return unreadable("no archive client yet");
        }
        final long[] range = {-1, -1};
        final int found = a.listRecording(serviceRecordingId,
                (c, s, r, st, sp, startPosition, stopPosition, i, t, se, m, sid, str, ch, sc, src) -> {
                    range[0] = startPosition;
                    range[1] = stopPosition;
                });
        if (found != 1 || range[1] == AeronArchive.NULL_POSITION) {
            return unreadable("service snapshot recording " + serviceRecordingId + " is "
                    + (found != 1 ? "missing" : "not stopped"));
        }
        final Aeron aeron = a.context().aeron();
        final StubPeek.Kind kind;
        try {
            kind = StubPeek.peek(aeron, a, clock, serviceRecordingId, range[0], range[1],
                    clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.loadTimeoutMs()));
        } catch (final RuntimeException failed) {
            forwardFatal(failed);
            return unreadable("peek of service snapshot recording " + serviceRecordingId + " failed: " + failed);
        }
        peekedRecordingId = serviceRecordingId;
        if (kind == StubPeek.Kind.UNKNOWN) {
            // a stopped recording never changes: cache UNKNOWN too, so the floor keeps purging nothing without a
            // replay per wake
            peekedKind = ShadowFloor.NewestImage.UNKNOWN;
            return unreadable("service snapshot recording " + serviceRecordingId + " holds no service message");
        }
        peekedKind = kind == StubPeek.Kind.STUB ? ShadowFloor.NewestImage.STUB : ShadowFloor.NewestImage.FULL;
        System.out.println("[shadow restore] floor precondition: newest group at P_cm=" + newest + " (service rec="
                + serviceRecordingId + ") is " + peekedKind + (peekedKind == ShadowFloor.NewestImage.FULL
                ? ": the floor is P_cm (today's pruning)" : ": the 9.1 table applies"));
        return peekedKind;
    }

    private ShadowFloor.NewestImage unreadable(final String why) {
        refusedOnce("[SHADOW-FLOOR] CRITICAL: the newest group's payload kind is unreadable (" + why
                + "): purge nothing");
        return ShadowFloor.NewestImage.UNKNOWN;
    }

    @Override
    public void close() {
        closed = true;
        if (lock.tryLock()) {
            try {
                release();
            } finally {
                lock.unlock();
            }
        }
        // else the pruner thread is inside run(): it releases on its way out (closed is set)
    }

    private void release() {
        CloseHelper.quietClose(retention);
        retention = null;
        housekeeping = null;
        CloseHelper.quietClose(archive);
        archive = null;
    }
}
