// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.journal.MoneyJournalTail;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowFloor;
import com.openexchange.cluster.shadow.ShadowHousekeeping;
import com.openexchange.cluster.shadow.ShadowPullSettings;
import com.openexchange.cluster.shadow.ShadowPuller;
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
 *
 * <p>The puller (with {@code SHADOW_PULL_SOURCES} set): one {@link ShadowPuller} thread
 * ({@code shadow-puller-core-<memberId>}) started by the first wake after the ADOPT (the genesis it filters on is
 * the recorded one) and closed with the housekeeping (from {@code onTerminate}). It reads the service's role through
 * {@code leaderNow} only: while this node leads, it lists and pulls nothing and aborts what is in flight. A pulled
 * copy reaches the floor and retention only after its local deep read (the next catalog scan sees it). A member
 * passes the puller the current leader's archive endpoint ({@link #leaderArchiveEndpoint}): a discovery pass skips it,
 * so nobody replicates from the leader's archive while it leads.</p>
 *
 * <p>The follower producer's writer ({@link #producer}, shadow mode only): armed once by the first wake after the ADOPT
 * that has a catalog scan, with this producer's identity {@code F<memberId>} and the replica's seeding rule (the seq
 * over header-bearing copies and header-less TOO_SHORT aliases of this producer, the position over its usable copies
 * only), its own archive client and one image buffer; published through a volatile the service reads in its guard. After
 * every scan the kit's {@link CrashedWriteSweep} purges this producer's crashed writes from before this process
 * (through retention), and under {@code two-producers} the floor-side {@link WitnessAlarm} raises its CRITICAL line on
 * every member when the newest ALIGNED position has had fewer than two producers for longer than its window.
 * {@link #close} closes the writer thread (aborting a write in flight) before the archive clients.</p>
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
    private final ShadowPullSettings pullSettings;
    private final String responseHost;
    private final java.util.function.BooleanSupplier leaderNow;
    private final String pullerThreadName;
    private volatile ShadowPuller puller;

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

    /** Builds the member's writer (production: the kit's public constructor; a test seam swaps a faulty one in). */
    @FunctionalInterface
    interface WriterFactory {
        com.openexchange.cluster.shadow.ShadowWriter create(Aeron aeron, AeronArchive archive,
                                                            com.openexchange.cluster.shadow.ShadowProducer producer,
                                                            com.openexchange.cluster.shadow.ShadowSelfCheck selfCheck,
                                                            String ipcTermLength, NanoClock clock, long connectTimeoutNs,
                                                            long offerTimeoutNs, long recordTimeoutNs,
                                                            long initialProducerSeq, long initialLastWrittenPosition,
                                                            ShadowCounters counters);

        WriterFactory PRODUCTION = com.openexchange.cluster.shadow.ShadowWriter::new;
    }

    /** The producer wiring; null: this member arms no writer (off mode, {@code SHADOW_PRODUCER=none}, unit tests). */
    record ProducerWiring(int memberId, int members,
                          com.openexchange.cluster.shadow.ShadowWriterThread.ShadowWriteListener listener,
                          WriterFactory writerFactory, boolean sweepOff, boolean witnessAlarmOff) {
    }

    private volatile ProducerWiring producerWiring;
    private volatile java.util.function.Supplier<String> leaderArchiveEndpoint = () -> null;
    private volatile MemberShadowProducer.Armed armed;
    private AeronArchive writerArchive;
    private com.openexchange.cluster.shadow.CrashedWriteSweep sweep;
    private volatile com.openexchange.cluster.shadow.WitnessAlarm witnessAlarm;
    private ShadowFloor.NewestImage peekedKind = ShadowFloor.NewestImage.UNKNOWN;

    /**
     * @param genesisId the engine's recorded genesis nonce as the service last published it (0 before ADOPT)
     */
    MemberShadowHousekeeping(final File clusterDir, final AeronArchive.Context archiveContext, final int clusterId,
                             final SnapshotModeSettings settings, final SnapshotModeCounters snapshotModeCounters,
                             final ShadowCounters shadowCounters, final ShadowVerdicts verdicts,
                             final ShadowHousekeeping.Counters counters, final LongSupplier genesisId,
                             final org.agrona.ErrorHandler fatalErrors) {
        this(clusterDir, archiveContext, clusterId, settings, snapshotModeCounters, shadowCounters, verdicts, counters,
                genesisId, fatalErrors, ShadowPullSettings.disabled(), "localhost", () -> false, "shadow-puller-core");
    }

    /**
     * @param pullSettings     the {@code SHADOW_PULL_*} rows checked against this node's own archive endpoint
     * @param responseHost     this node's host (the listing clients' response channel when no endpoint is set)
     * @param leaderNow        the service's role: true while LEADER
     * @param pullerThreadName the puller thread's name
     */
    MemberShadowHousekeeping(final File clusterDir, final AeronArchive.Context archiveContext, final int clusterId,
                             final SnapshotModeSettings settings, final SnapshotModeCounters snapshotModeCounters,
                             final ShadowCounters shadowCounters, final ShadowVerdicts verdicts,
                             final ShadowHousekeeping.Counters counters, final LongSupplier genesisId,
                             final org.agrona.ErrorHandler fatalErrors, final ShadowPullSettings pullSettings,
                             final String responseHost, final java.util.function.BooleanSupplier leaderNow,
                             final String pullerThreadName) {
        this.pullSettings = Objects.requireNonNull(pullSettings, "pullSettings");
        this.responseHost = Objects.requireNonNull(responseHost, "responseHost");
        this.leaderNow = Objects.requireNonNull(leaderNow, "leaderNow");
        this.pullerThreadName = Objects.requireNonNull(pullerThreadName, "pullerThreadName");
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
                h -> ShadowRestore.skipSafe(h, journal), settings.floorMargin(), settings.anchorRule(),
                this::logStartPosition, counters);
    }

    /** Wires the follower producer (before the pruner starts); shadow mode with {@code SHADOW_PRODUCER} enabled only. */
    MemberShadowHousekeeping producer(final ProducerWiring wiring) {
        this.producerWiring = wiring;
        if (wiring != null && settings.mode() == SnapshotMode.SHADOW
                && settings.anchorRule() == ShadowFloor.AnchorRule.TWO_PRODUCERS && !wiring.witnessAlarmOff()) {
            witnessAlarm = new com.openexchange.cluster.shadow.WitnessAlarm(
                    TimeUnit.MILLISECONDS.toNanos(settings.witnessAlarmMs()));
        }
        return this;
    }

    /** The current leader's archive endpoint for the puller's skip (null: none known). */
    MemberShadowHousekeeping leaderArchiveEndpoint(final java.util.function.Supplier<String> endpoint) {
        this.leaderArchiveEndpoint = Objects.requireNonNull(endpoint, "endpoint");
        return this;
    }

    /** The armed writer, its thread and image buffer; null until armed. Read by the service thread. */
    MemberShadowProducer.Armed armedProducer() {
        return armed;
    }

    /** {@code shadow_floor_witness_missing{kind}}: 1 while the alarm holds and the kind is missing. */
    int witnessMissing(final com.openexchange.cluster.shadow.ProducerKind kind) {
        final com.openexchange.cluster.shadow.WitnessAlarm a = witnessAlarm;
        return a == null ? 0 : a.missing(kind);
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
            startPullerOnce();
            if (housekeeping == null && !build()) {
                return;
            }
            housekeeping.poll();
            afterScan(housekeeping.scan());
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

    /** The puller starts once, after the ADOPT (it filters candidates on the recorded genesis). */
    private void startPullerOnce() {
        if (puller != null || !pullSettings.enabled() || genesisId.getAsLong() <= 0) {
            return;
        }
        final ShadowPuller p = new ShadowPuller(archive.context().aeron(), archiveContext, CoreLogApplier.ENGINE_ID_CORE,
                clusterId, genesisId, leaderNow, responseHost, pullSettings, verdicts, shadowCounters, clock,
                TimeUnit.MILLISECONDS.toNanos(settings.loadTimeoutMs()), pullerThreadName);
        p.deferredSource(() -> leaderArchiveEndpoint.get());
        puller = p;
        p.start();
        System.out.println("[SHADOW-PULL] puller started: sources=" + pullSettings.sources() + " periodMs="
                + pullSettings.periodMs() + " kinds=" + pullSettings.kinds() + " genesisId=" + genesisId.getAsLong());
    }

    /** After every catalog poll: arm the writer once, sweep this producer's crashed writes, the witness alarm. */
    private void afterScan(final ShadowCatalog.Scan scan) {
        final ProducerWiring w = producerWiring;
        if (w == null || scan == null || !scan.known()) {
            return;
        }
        // a one-member cluster designates nobody: no writer, no image buffer
        if (armed == null && w.members() >= 2 && settings.mode() == SnapshotMode.SHADOW
                && settings.producerSettings().enabled()) {
            arm(w, scan);
        }
        final MemberShadowProducer.Armed a = armed;
        if (a != null && sweep != null && !w.sweepOff()) {
            sweep.sweep(scan, a.firstProducerSeq(), retention);
        }
        final com.openexchange.cluster.shadow.WitnessAlarm alarm = witnessAlarm;
        if (alarm != null && leaderNow.getAsBoolean()) {
            // a leader neither pulls nor produces, so its own archive never holds a fresh two-producer anchor (and it
            // never purges below P_cm on one): the followers' floors carry the alarm; leading starts a fresh window
            if (alarm.holding()) {
                witnessAlarm = new com.openexchange.cluster.shadow.WitnessAlarm(
                        TimeUnit.MILLISECONDS.toNanos(settings.witnessAlarmMs()));
            }
        } else if (alarm != null) {
            final com.openexchange.cluster.shadow.WitnessAlarm.Result r = alarm.observe(clock.nanoTime(), scan);
            if (r.criticalLine() != null) {
                System.err.println("[SHADOW-FLOOR] CRITICAL: " + r.criticalLine());
            }
        }
    }

    /**
     * Arms the writer with this producer's seq and position from the local catalog (the replica's rule): the seq over
     * header-bearing copies AND header-less TOO_SHORT aliases (else a restarted writer reuses a crashed seq), the
     * position over usable copies only (a CORRUPT or CONFLICT copy at P must not block a re-write at P).
     */
    private void arm(final ProducerWiring w, final ShadowCatalog.Scan scan) {
        final com.openexchange.cluster.shadow.ShadowProducer producer = new com.openexchange.cluster.shadow.ShadowProducer(
                clusterId, com.openexchange.cluster.shadow.ProducerKind.FOLLOWER, w.memberId());
        long maxSeq = 0;
        long maxPos = -1;
        for (final com.openexchange.cluster.shadow.ShadowEntry e : scan.all()) {
            final com.openexchange.cluster.shadow.ShadowHeader h = e.header();
            if (h != null && h.producerKind() == producer.kind().code() && h.producerId() == producer.producerId()) {
                maxSeq = Math.max(maxSeq, h.producerSeq());
                if (e.state().usable()) {
                    maxPos = Math.max(maxPos, h.logPosition());
                }
            } else if (h == null && e.alias() != null && e.alias().producerKind() == producer.kind().code()
                    && e.alias().producerId() == producer.producerId()) {
                maxSeq = Math.max(maxSeq, e.alias().producerSeq());
            }
        }
        final com.openexchange.cluster.shadow.ShadowProducerSettings ps = settings.producerSettings();
        final com.openexchange.cluster.shadow.ShadowImageBuffer image = new com.openexchange.cluster.shadow.ShadowImageBuffer(
                new org.agrona.concurrent.UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(
                        ps.bufferBytes() + com.openexchange.cluster.shadow.ShadowHeader.LENGTH)));
        writerArchive = AeronArchive.connect(archiveContext.clone());
        final com.openexchange.cluster.shadow.ShadowWriter writer = (w.writerFactory() == null
                ? WriterFactory.PRODUCTION : w.writerFactory()).create(archive.context().aeron(), writerArchive, producer,
                new com.openexchange.core.replica.ApplierSelfCheck("[SHADOW-PRODUCER]"), ps.termLength(), clock,
                TimeUnit.MILLISECONDS.toNanos(ps.connectTimeoutMs()), TimeUnit.MILLISECONDS.toNanos(ps.offerTimeoutMs()),
                TimeUnit.MILLISECONDS.toNanos(ps.recordTimeoutMs()), maxSeq + 1, maxPos, shadowCounters);
        final com.openexchange.cluster.shadow.ShadowWriterThread thread =
                new com.openexchange.cluster.shadow.ShadowWriterThread(writer, w.listener(), this::onWriterError,
                        shadowCounters);
        sweep = new com.openexchange.cluster.shadow.CrashedWriteSweep(producer, "[SHADOW-HK]", System.out::println,
                shadowCounters);
        armed = new MemberShadowProducer.Armed(writer, thread, image, maxSeq + 1);
        System.out.println("[SHADOW-PRODUCER] writer armed: producer=F" + w.memberId() + " nextSeq=" + (maxSeq + 1)
                + " lastWritten=" + maxPos + " bufferMb=" + ps.bufferMb() + " termLength=" + ps.termLength());
    }

    /** The writer thread's errors: an {@code Error} reaches the member's halt path, anything else is one line. */
    private void onWriterError(final Throwable t) {
        System.err.println("[SHADOW-PRODUCER] CRITICAL: shadow writer thread error: " + t);
        if (t instanceof Error) {
            fatalErrors.onError(t);
        }
    }

    /** The running puller, null before the ADOPT or without sources. */
    ShadowPuller puller() {
        return puller;
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
                + " retainN=" + settings.retainN() + " margin=" + settings.floorMargin().text() + " anchorRule="
                + settings.anchorRule().text() + " scanPeriodMs="
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
        final ShadowPuller p = puller;
        if (p != null) {
            CloseHelper.quietClose(p); // stops its thread, aborts and purges what is in flight
        }
        final MemberShadowProducer.Armed a = armed;
        if (a != null) {
            CloseHelper.quietClose(a.thread()); // aborts a write in flight (its cleanup purges), joins
        }
        CloseHelper.quietClose(writerArchive);
        writerArchive = null;
        CloseHelper.quietClose(retention);
        retention = null;
        housekeeping = null;
        CloseHelper.quietClose(archive);
        archive = null;
    }
}
