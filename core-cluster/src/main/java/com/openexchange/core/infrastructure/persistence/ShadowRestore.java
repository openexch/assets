// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.domain.CoreEventSink;
import com.openexchange.core.infrastructure.journal.MoneyJournalTail;
import com.openexchange.core.infrastructure.publisher.CoreEventPublisher;
import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.ClusterLogReader;
import com.openexchange.cluster.log.ClusterLogReaderStoppedException;
import com.openexchange.cluster.log.SkipWindow;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowAlias;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowImageLoader;
import com.openexchange.cluster.shadow.ShadowStub;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * The member's restore from a {@link ShadowStub} inside {@code onStart}. Selection over ONE scan of
 * the local catalog, in the D12 order, the first candidate that passes EVERY pre-engine check is the one restored, and
 * there is never a second {@code restore} in this process (the engine is final, option (a)):
 *
 * <ol>
 *   <li><b>EQUAL</b> {@code equalAt(P_cm)}: CONFLICT is CRITICAL and means "no candidate"; a loader or identity
 *       refusal is counted and falls through.</li>
 *   <li><b>SKIP</b> {@code newestAtOrAbove(P_cm + 1)}: CONFLICT, not skip-safe (8.1), or a refusal abandons SKIP for
 *       this boot.</li>
 *   <li><b>TAIL</b> downwards from P_cm over {@code newestBelow(pos, logStart)}: CONFLICT and
 *       refusals continue below; the single-recording precondition is a pre-engine check.</li>
 *   <li><b>Virtual genesis</b> (section 7) when the 7.1 precondition holds, loud.</li>
 *   <li>Otherwise {@link Halt} with the 7.3 text (G1).</li>
 * </ol>
 *
 * <p>Pre-engine checks of a candidate: {@link ShadowImageLoader#load} into a buffer of {@code header.payloadLength}
 * bytes (header, trailer, digest), {@link ShadowIdentity#check} under STRICT, skip-safe for SKIP, one recording for
 * TAIL. Then {@code applier.restore}, {@code applier.stampLogPosition(P_s)}, the post-restore cross-check
 * ({@link ShadowIdentity#matches}) and the W1 witness {@code lastLogTime <= header.clusterTime} (6.3; one-directional,
 * see there). A failure after {@code restore} halts.</p>
 *
 * <p>The TAIL replay (6.4) runs here, inside {@code onStart} before the service acks (hard rule 12.12): a
 * session-scoped replay of {@code (P_s, P_cm]} from the local log recording, read by {@link ClusterLogReader} into an
 * adapter that calls the SAME applier entries as the live wrappers with the reader's frame-end positions,
 * updates the service's term id on every term event (G28), and discards the events the applier emits through a
 * counting {@link TailEgressSink} (rule 12.15: nothing is published from {@code onStart}). Every wait has a deadline
 * from a setting (rule 12.10); a watchdog covers an apply that blocks inside the money-journal ring (rule 12.14).</p>
 *
 * <p>Never writes {@code RecordingLog}, never schedules, never publishes, never purges.</p>
 */
final class ShadowRestore {

    /** A halt of the restore protocol: the service prints it, fences and runs {@code failFast} (G1). */
    static final class Halt extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Halt(final String message) {
            super(message);
        }

        Halt(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /** The restore paths (section 6, 7). */
    enum Path { EQUAL, SKIP, TAIL, VIRTUAL_GENESIS }

    /**
     * What the restore did. {@code window} is non-null for SKIP only; {@code leadershipTermId} is the service's term id
     * after the restore (the stub's on every path, checked against the tail on TAIL and virtual genesis).
     */
    record Outcome(Path path, long imagePosition, long logPosition, long leadershipTermId, SkipWindow window,
                   ShadowHeader header, long recordingId) {
    }

    /**
     * Test seams (review F8 and the 11.x negative controls; contract 11 preamble: "each test's control switches one
     * rule off through a package-private seam"). {@link #afterTailFrames} pauses the tail after {@code n} applied frames
     * by running {@code hook} on the service thread (11.14 kills the driver while it blocks). Every other seam switches
     * exactly one rule off so its test can show the RED it guards. Production uses {@link #NONE}, which is never
     * modified ({@link #testing()} hands out a fresh instance).
     */
    static final class Faults {
        static final Faults NONE = new Faults();

        private long tailFrames = -1;
        private Runnable tailHook = () -> { };
        /** 11.3 / 11.5 control: the 7.1 precondition answers "holds" even on a pruned log. */
        boolean virtualGenesisPreconditionFaked;
        /** 11.3 control (review F4): rule 3 stops at the first TAIL position it cannot use instead of going down. */
        boolean tailIterationOff;
        /** 11.3: the engine refuses the image AFTER {@code applier.restore} was entered (a digest-valid producer bug). */
        boolean failAfterRestore;
        /** 11.3 control: a failure after restore tries the restore again instead of halting. */
        boolean secondAttemptAllowed;
        /** 11.3 control (8.1): every SKIP candidate is treated as skip-safe. */
        boolean skipSafeOff;
        /** 11.11 control (G28): the tail adapter does not update the service's term id on a term event. */
        boolean tailTermUpdateOff;
        /** 11.4 control: {@code onTakeSnapshot} ignores the skip window (the window clause of 3.3 off). */
        boolean windowClauseOff;
        /** 11.10 control: open, close and timer entries are stamped with the engine's previous position. */
        boolean rf3StampOff;
        /** 11.10 control (K3 shape): timer ids are built from term 0 whatever the service's term id. */
        boolean timerIdsFromTermZero;
        /**
         * 11.13 Q9 drill (M Q9): runs on the service thread right after {@code onTakeSnapshot} returned, before Aeron's
         * own {@code awaitRecordingComplete}; the drill kills the member there.
         */
        Runnable afterTakeSnapshot;
        /**
         * 11.2 100k leg (review F3): called on the service thread after every applied tail frame with the frame count
         * (the egress-queue and heap sampler).
         */
        java.util.function.LongConsumer tailFrameObserver;

        static Faults afterTailFrames(final long n, final Runnable hook) {
            if (n < 0) {
                throw new IllegalArgumentException("n must be >= 0: " + n);
            }
            final Faults f = new Faults();
            f.tailFrames = n;
            f.tailHook = Objects.requireNonNull(hook, "hook");
            return f;
        }

        /** A fresh, modifiable instance for a negative control. */
        static Faults testing() {
            return new Faults();
        }

        void onTailFrame(final long applied) {
            if (tailFrameObserver != null) {
                tailFrameObserver.accept(applied);
            }
            if (applied == tailFrames) {
                tailHook.run();
            }
        }
    }

    /**
     * The tail's egress: events the applier emits while the tail runs are counted and dropped,
     * exactly what a follower does with CM-replayed frames; nothing reaches {@code egressBySessionId} or a publication.
     */
    static final class TailEgressSink implements CoreEventPublisher.Egress {
        private long discarded;
        private long bytes;

        @Override
        public void emit(final MutableDirectBuffer buffer, final int offset, final int length, final int channel,
                         final CoreEventPublisher.Routing routing) {
            discarded++;
            bytes += length;
        }

        long discarded() {
            return discarded;
        }

        long bytes() {
            return bytes;
        }
    }

    /** What the restore needs from the cluster, all read at {@code onStart} (log-derived or node-local I/O). */
    record Inputs(File clusterDir, AeronArchive.Context archiveContext, int clusterId, long recoveryPosition) {
    }

    /** A loaded candidate that passed every pre-engine check. */
    private record Loaded(LogicalShadow shadow, ShadowEntry entry, ShadowImageLoader.LoadedShadow image) {
    }

    /** A valid term entry: its term, base, recording, and the next term's base (or MAX for the last). */
    record Term(long termId, long base, long recordingId, long nextBase) {
    }

    static final int TAIL_FRAGMENT_LIMIT = 64;
    private static final long WATCHDOG_GRACE_NS = TimeUnit.SECONDS.toNanos(1);

    private final Inputs in;
    private final CoreLogApplier applier;
    private final SnapshotModeSettings settings;
    private final SnapshotModeCounters counters;
    private final ShadowCounters shadowCounters;
    private final ShadowVerdicts verdicts;
    private final Faults faults;
    private final LongConsumer termSink;
    private final CoreEventSink liveSink;
    private final Consumer<String> watchdogHalt;
    private final NanoClock clock = SystemNanoClock.INSTANCE;

    private boolean restoreAttempted;
    private volatile long tailAt = -1;

    /**
     * @param termSink     sets the SERVICE's leadership term id (G28; the applier knows nothing of terms)
     * @param liveSink     the event sink the service runs with; restored after the tail
     * @param watchdogHalt logs and stops the node from the watchdog thread when an apply blocks past the tail deadline
     */
    ShadowRestore(final Inputs in, final CoreLogApplier applier, final SnapshotModeSettings settings,
                  final SnapshotModeCounters counters, final ShadowCounters shadowCounters, final ShadowVerdicts verdicts,
                  final Faults faults, final LongConsumer termSink, final CoreEventSink liveSink,
                  final Consumer<String> watchdogHalt) {
        this.in = Objects.requireNonNull(in, "in");
        this.applier = Objects.requireNonNull(applier, "applier");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.shadowCounters = Objects.requireNonNull(shadowCounters, "shadowCounters");
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.faults = Objects.requireNonNull(faults, "faults");
        this.termSink = Objects.requireNonNull(termSink, "termSink");
        this.liveSink = Objects.requireNonNull(liveSink, "liveSink");
        this.watchdogHalt = Objects.requireNonNull(watchdogHalt, "watchdogHalt");
    }

    // ------------------------------------------------------------------------------------------- stub (3.3)

    /** The kit envelope the stub is sealed in; a fresh instance per call, no shared state between threads. */
    static com.openexchange.cluster.SnapshotEnvelope envelope() {
        return new com.openexchange.cluster.SnapshotEnvelope();
    }

    /** What the opened envelope holds: a stub, or the payload whose layout tag the codec reads. */
    record Peek(int payloadOffset, int payloadLength, boolean stub, long tag) {
        /** Anything that is neither a stub nor v5 is refused with today's codec wording plus the tag. */
        void requireFullImage() {
            if (tag != BalanceSnapshotCodec.LAYOUT_V5_TAG) {
                throw new IllegalStateException("balance snapshot layout tag " + tag + " refused: this build reads only"
                        + " v5 (" + BalanceSnapshotCodec.LAYOUT_V5_TAG + ") or a shadow stub (" + ShadowStub.STUB_TAG
                        + ")");
            }
        }
    }

    /** 3.3: the envelope first (magic, version, length, CRC32C), then the first payload long. */
    static Peek peek(final DirectBuffer image, final int length) {
        final int payloadLength = envelope().open(image, length);
        final int offset = com.openexchange.cluster.SnapshotEnvelope.payloadOffset();
        final long tag = payloadLength >= Long.BYTES ? image.getLong(offset) : Long.MIN_VALUE;
        return new Peek(offset, payloadLength, ShadowStub.isStubPayload(image, offset, payloadLength), tag);
    }

    /** The stub inside an opened envelope, with the 3.3 consistency checks; every refusal is a {@link Halt}. */
    static ShadowStub decodeStub(final DirectBuffer image, final int payloadOffset, final int payloadLength,
                                 final int clusterId, final long recoveryPosition) {
        final ShadowStub.Decoded d = ShadowStub.decode(image, payloadOffset, payloadLength,
                CoreLogApplier.ENGINE_ID_CORE, clusterId);
        if (!d.ok()) {
            throw new Halt("FATAL shadow restore: stub refused: " + d.refusal());
        }
        final ShadowStub stub = d.stub();
        if (stub.logPosition() != recoveryPosition) {
            throw new Halt("FATAL shadow restore: stub P_cm " + stub.logPosition() + " != recovery position " + recoveryPosition);
        }
        return stub;
    }

    // ------------------------------------------------------------------------------------------- the protocol

    Outcome run(final ShadowStub stub) {
        final long pCm = stub.logPosition();
        final long startNs = clock.nanoTime();
        final List<Term> terms = readTerms(in.clusterDir());
        if (terms.isEmpty()) {
            throw new Halt("FATAL shadow restore: stub at P_cm=" + pCm + " but " + in.clusterDir()
                    + " holds no valid term entry: no local log to restore from");
        }
        final Term lastTerm = terms.get(terms.size() - 1);
        final long logRecordingId = lastTerm.recordingId();
        final Map<Long, Long> termBases = new HashMap<>();
        for (final Term t : terms) {
            termBases.put(t.termId(), t.base());
        }
        final ShadowIdentity.LocalIdentity local = new ShadowIdentity.LocalIdentity(CoreLogApplier.ENGINE_ID_CORE,
                in.clusterId(), stub.genesisId(), termId -> termBases.getOrDefault(termId, -1L));

        AeronArchive archive = null;
        try {
            archive = AeronArchive.connect(in.archiveContext().clone());
            final Aeron aeron = archive.context().aeron();
            final long logStart = archive.getStartPosition(logRecordingId);
            final long loadTimeoutNs = TimeUnit.MILLISECONDS.toNanos(settings.loadTimeoutMs());
            final ShadowCatalog catalog = new ShadowCatalog(aeron, archive, CoreLogApplier.ENGINE_ID_CORE,
                    in.clusterId(), stub.genesisId(), verdicts, ShadowCatalog.DEFAULT_LIST_BATCH, clock, loadTimeoutNs,
                    shadowCounters);
            final ShadowImageLoader loader = new ShadowImageLoader(aeron, archive, verdicts, clock, loadTimeoutNs,
                    shadowCounters);
            final ShadowCatalog.Scan scan = catalog.scan();
            counters.set(SnapshotModeCounters.C.RESTORE_CANDIDATES_SCANNED, scan.all().size());
            System.out.println("[shadow restore] restore from stub P_cm=" + pCm + " term=" + stub.leadershipTermId()
                    + " genesisId=" + stub.genesisId() + ": " + scan.all().size() + " shadow recording(s), usable="
                    + scan.usable().size() + " conflicts=" + scan.conflicts().size() + " errors=" + scan.errors()
                    + " logRecording=" + logRecordingId + " logStart=" + logStart);

            // 1. EQUAL
            String equal = "none";
            final ShadowCatalog.Answer eq = scan.equalAt(pCm);
            Loaded chosen = null;
            Path path = null;
            if (eq.kind() == ShadowCatalog.Answer.Kind.CONFLICT) {
                conflict(SnapshotModeCounters.ConflictAt.EQUAL, eq.shadow());
                equal = "conflict";
            } else if (eq.kind() == ShadowCatalog.Answer.Kind.FOUND) {
                chosen = check(loader, eq.shadow(), local, "EQUAL");
                equal = chosen == null ? "refused" : "found";
                path = chosen == null ? null : Path.EQUAL;
            }

            // 2. SKIP
            String skip = "none";
            if (chosen == null) {
                final ShadowCatalog.Answer sk = scan.newestAtOrAbove(pCm + 1);
                if (sk.kind() == ShadowCatalog.Answer.Kind.CONFLICT) {
                    conflict(SnapshotModeCounters.ConflictAt.SKIP, sk.shadow());
                    skip = "conflict";
                } else if (sk.kind() == ShadowCatalog.Answer.Kind.FOUND) {
                    final ShadowHeader h = sk.shadow().representative().header();
                    final MoneyJournalTail.Result tail = h.journalEnabledFlag()
                            ? readJournalTail(aeron, archive) : null;
                    if (!faults.skipSafeOff && !skipSafe(h, tail)) {
                        counters.increment(SnapshotModeCounters.C.SKIP_UNSAFE_TOTAL);
                        System.out.println("[shadow restore] SKIP candidate P_s=" + sk.shadow().logPosition()
                                + " is not skip-safe on this node (journalSeq=" + h.journalSeq() + ", local journal "
                                + tail + "): SKIP abandoned for this boot");
                        skip = "unsafe";
                    } else {
                        chosen = check(loader, sk.shadow(), local, "SKIP");
                        skip = chosen == null ? "refused" : "found";
                        path = chosen == null ? null : Path.SKIP;
                    }
                }
            }

            // 3. TAIL, iterating downwards (review F4)
            final List<String> tailNotes = new ArrayList<>();
            if (chosen == null) {
                long pos = pCm;
                while (true) {
                    final ShadowCatalog.Answer t = scan.newestBelow(pos, logStart);
                    if (t.kind() == ShadowCatalog.Answer.Kind.EMPTY) {
                        break;
                    }
                    final long ps = t.shadow().logPosition();
                    if (t.kind() == ShadowCatalog.Answer.Kind.CONFLICT) {
                        conflict(SnapshotModeCounters.ConflictAt.TAIL, t.shadow());
                        tailNotes.add("conflict@" + ps);
                        if (faults.tailIterationOff) {
                            break;
                        }
                        pos = ps;
                        continue;
                    }
                    final String multi = singleRecording(terms, ps, pCm);
                    if (multi != null) {
                        counters.refuse(SnapshotModeCounters.Refusal.MULTI_RECORDING);
                        System.out.println("[shadow restore] TAIL candidate P_s=" + ps + " refused: " + multi);
                        tailNotes.add("multi-recording@" + ps);
                        if (faults.tailIterationOff) {
                            break;
                        }
                        pos = ps;
                        continue;
                    }
                    final Loaded l = check(loader, t.shadow(), local, "TAIL");
                    if (l == null) {
                        tailNotes.add("refused@" + ps);
                        if (faults.tailIterationOff) {
                            break;
                        }
                        pos = ps;
                        continue;
                    }
                    chosen = l;
                    path = Path.TAIL;
                    break;
                }
            }

            if (chosen != null) {
                // the tail replays the ONE recording the precondition named (6.4), not merely the newest term's
                final long tailRecording = path == Path.TAIL
                        ? rangeRecording(terms, chosen.shadow().logPosition(), pCm) : logRecordingId;
                return restoreFrom(chosen, path, stub, tailRecording, aeron, archive, startNs);
            }

            // 4. virtual genesis (7.1), else 5. halt (7.3)
            final String precondition = faults.virtualGenesisPreconditionFaked ? null
                    : virtualGenesisPrecondition(terms, logStart, pCm);
            if (precondition == null) {
                return virtualGenesis(stub, rangeRecording(terms, 0, pCm), aeron, archive, startNs);
            }
            throw new Halt("FATAL shadow restore: stub at P_cm=" + pCm + " and no restore path: shadows usable="
                    + scan.usable().size() + " (EQUAL " + equal + ", SKIP " + skip + ", TAIL "
                    + (tailNotes.isEmpty() ? "none" : String.join(" ", tailNotes)) + " above log start " + logStart
                    + "), log starts at " + logStart + (logStart > 0 ? " > 0" : "") + " (virtual genesis refused: "
                    + precondition + "). Runbook: (1) ClusterTool invalidate-latest-snapshot until the newest valid"
                    + " group is a full image whose log is retained, then boot; (2) or restore this node's cluster dir"
                    + " from a peer; (3) or pull a shadow with P_s >= " + logStart + " once a shadow puller exists.");
        } finally {
            CloseHelper.quietClose(archive);
        }
    }

    // ------------------------------------------------------------------------------------------- candidates

    private void conflict(final SnapshotModeCounters.ConflictAt at, final LogicalShadow s) {
        counters.conflict(at);
        System.err.println("[shadow restore] CRITICAL: shadow CONFLICT at P=" + s.logPosition() + " (" + at + "): " + s
                + "; a conflict never resolves by picking, the position has no candidate");
    }

    /** The pre-engine checks of one candidate (6.2): loader, then STRICT identity. Null when refused (counted). */
    private Loaded check(final ShadowImageLoader loader, final LogicalShadow s, final ShadowIdentity.LocalIdentity local,
                         final String rule) {
        final ShadowEntry entry = s.representative();
        final ShadowHeader h = entry.header();
        final UnsafeBuffer into;
        try {
            if (h.payloadLength() > Integer.MAX_VALUE - 8) {
                // a Java array cannot hold it: the counted TOO_LARGE skip, not a NegativeArraySizeException
                throw new OutOfMemoryError("payloadLength " + h.payloadLength() + " exceeds the largest array");
            }
            into = new UnsafeBuffer(new byte[(int)Math.max(1, h.payloadLength())]);
        } catch (final OutOfMemoryError tooLarge) {
            counters.refuse(SnapshotModeCounters.Refusal.TOO_LARGE);
            System.out.println("[shadow restore] " + rule + " candidate P_s=" + s.logPosition() + " refused TOO_LARGE: header"
                    + " payloadLength=" + h.payloadLength() + " cannot be allocated");
            return null;
        }
        final ShadowImageLoader.LoadResult r = loader.load(entry, into);
        if (!r.ok()) {
            counters.refuse(switch (r.kind()) {
                case CORRUPT -> SnapshotModeCounters.Refusal.CORRUPT;
                case SCAN_ERROR -> SnapshotModeCounters.Refusal.SCAN_ERROR;
                case REFUSED_TOO_LARGE -> SnapshotModeCounters.Refusal.TOO_LARGE;
                default -> SnapshotModeCounters.Refusal.REFUSED;
            });
            System.out.println("[shadow restore] " + rule + " candidate P_s=" + s.logPosition() + " rec=" + entry.recordingId()
                    + " refused by the loader: " + r.kind() + " " + r.reason());
            return null;
        }
        final ShadowIdentity.Outcome identity = ShadowIdentity.check(r.loaded().header(), local);
        if (!ShadowIdentity.accepted(identity, ShadowIdentity.Policy.STRICT)) {
            counters.refuse(SnapshotModeCounters.Refusal.valueOf(identity.name()));
            System.out.println("[shadow restore] " + rule + " candidate P_s=" + s.logPosition() + " rec=" + entry.recordingId()
                    + " refused by STRICT identity: " + identity);
            return null;
        }
        return new Loaded(s, entry, r.loaded());
    }

    /** 8.1: {@code !S.journalEnabled || localJournalDurableSeq >= S.journalSeq}; an unreadable journal is not safe. */
    static boolean skipSafe(final ShadowHeader h, final MoneyJournalTail.Result localJournal) {
        if (!h.journalEnabledFlag()) {
            return true;
        }
        return localJournal instanceof MoneyJournalTail.Known k && k.seq() >= h.journalSeq();
    }

    private MoneyJournalTail.Result readJournalTail(final Aeron aeron, final AeronArchive archive) {
        final MoneyJournalTail.Result r = new MoneyJournalTail(aeron, archive, clock)
                .read(TimeUnit.MILLISECONDS.toNanos(settings.loadTimeoutMs()));
        publishJournalTail(r, counters);
        return r;
    }

    static void publishJournalTail(final MoneyJournalTail.Result r, final SnapshotModeCounters counters) {
        if (r instanceof MoneyJournalTail.Known k) {
            counters.set(SnapshotModeCounters.C.JOURNAL_TAIL_SEQ, k.seq());
            if (k.active() > 0) {
                counters.add(SnapshotModeCounters.C.JOURNAL_TAIL_ACTIVE_TOTAL, k.active());
            }
        } else if (r instanceof MoneyJournalTail.Unknown u) {
            System.out.println("[shadow restore] money journal tail unknown: " + u.reason() + " (SKIP is not taken)");
        }
    }

    // ------------------------------------------------------------------------------------------- restore

    private Outcome restoreFrom(final Loaded l, final Path path, final ShadowStub stub, final long logRecordingId,
                                final Aeron aeron, final AeronArchive archive, final long startNs) {
        final ShadowHeader h = l.image().header();
        final long ps = h.logPosition();
        oneAttempt();
        try {
            applier.restore(l.image().payload(), l.image().length());
            if (faults.failAfterRestore) {
                throw new IllegalStateException("test fault: the engine refused the image after restore was entered");
            }
        } catch (final com.openexchange.cluster.auth.PrincipalLessSessionException principalLess) {
            throw principalLess;
        } catch (final RuntimeException refused) {
            if (faults.secondAttemptAllowed && !(refused instanceof Halt)) {
                return restoreFrom(l, path, stub, logRecordingId, aeron, archive, startNs);
            }
            throw new Halt("FATAL shadow restore: " + path + " restore of shadow P_s=" + ps + " rec=" + l.entry().recordingId()
                    + " failed after the engine was touched (digest-valid but undecodable: a producer bug; no second"
                    + " attempt): " + refused.getMessage(), refused);
        }
        applier.stampLogPosition(ps);
        final String mismatch = ShadowIdentity.mismatch(h, new CoreShadowIdentity(applier.engine()));
        if (mismatch != null) {
            shadowCounters.increment(ShadowCounters.Counter.IDENTITY_CROSSCHECK_FAILURES_TOTAL);
            throw new Halt("FATAL shadow restore: IDENTITY_CROSS_CHECK_FAILED after restoring shadow P_s=" + ps + ": " + mismatch);
        }
        final long lastLogTime = applier.engine().getLastLogTime();
        if (lastLogTime > h.clusterTime()) {
            counters.increment(SnapshotModeCounters.C.WITNESS_FAILED_TOTAL);
            throw new Halt("FATAL shadow restore: shadow P_s witness failed: lastLogTime=" + lastLogTime + " > header.clusterTime="
                    + h.clusterTime());
        }
        final long pCm = stub.logPosition();
        SkipWindow window = null;
        long term = stub.leadershipTermId();
        switch (path) {
            case EQUAL -> termSink.accept(term);
            case SKIP -> {
                termSink.accept(term);
                window = new SkipWindow(ps);
            }
            case TAIL -> term = tail(ps, pCm, logRecordingId, h.leadershipTermId(), stub, aeron, archive);
            default -> throw new IllegalStateException(path.toString());
        }
        final long ms = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - startNs);
        counters.set(SnapshotModeCounters.C.RESTORE_PATH, switch (path) {
            case EQUAL -> SnapshotModeCounters.PATH_EQUAL;
            case SKIP -> SnapshotModeCounters.PATH_SKIP;
            default -> SnapshotModeCounters.PATH_TAIL;
        });
        System.out.println("[SNAPSHOT] restored path=" + path + " P_s=" + ps + " P_cm=" + pCm + " rec="
                + l.entry().recordingId() + " producer=" + l.entry().producer() + " term=" + term + " orders="
                + applier.engine().orders().orders().size() + " accounts=" + applier.engine().accountCount()
                + " journalSeq=" + applier.engine().getJournalSeq() + " ms=" + ms);
        return new Outcome(path, ps, pCm, term, window, h, l.entry().recordingId());
    }

    private Outcome virtualGenesis(final ShadowStub stub, final long logRecordingId, final Aeron aeron,
                                   final AeronArchive archive, final long startNs) {
        final long pCm = stub.logPosition();
        oneAttempt();
        System.err.println("shadow restore VIRTUAL GENESIS: no shadow for stub at P_cm=" + pCm + ", replaying the complete local log"
                + " from 0 (" + pCm + " bytes); /ready will be late");
        counters.increment(SnapshotModeCounters.C.VIRTUAL_GENESIS_TOTAL);
        applier.genesis();
        applier.stampLogPosition(0);
        final long term = tail(0, pCm, logRecordingId, -1L, stub, aeron, archive);
        counters.set(SnapshotModeCounters.C.RESTORE_PATH, SnapshotModeCounters.PATH_VIRTUAL_GENESIS);
        System.out.println("[SNAPSHOT] restored path=" + Path.VIRTUAL_GENESIS + " P_s=0 P_cm=" + pCm + " term=" + term
                + " orders=" + applier.engine().orders().orders().size() + " accounts="
                + applier.engine().accountCount() + " ms=" + TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - startNs));
        return new Outcome(Path.VIRTUAL_GENESIS, 0, pCm, term, null, null, -1);
    }

    private void oneAttempt() {
        if (restoreAttempted) {
            throw new Halt("FATAL shadow restore: two restore attempts in one process");
        }
        restoreAttempted = true;
    }

    // ------------------------------------------------------------------------------------------- tail (6.4)

    /**
     * Replays {@code (from, to]} of {@code recordingId} through the applier; returns the service term id afterwards,
     * which must equal the stub's (G28).
     */
    private long tail(final long from, final long to, final long recordingId, final long termAtStart,
                      final ShadowStub stub, final Aeron aeron, final AeronArchive archive) {
        final long[] term = {termAtStart};
        termSink.accept(termAtStart);
        final TailEgressSink sink = new TailEgressSink();
        final long[] frames = {0};
        final ClusterLogListener adapter = new ClusterLogListener() {
            @Override
            public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                         final DirectBuffer buffer, final int offset, final int length) {
                applier.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
                afterEntry(logPosition);
            }

            @Override
            public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
                applier.onTimerEvent(correlationId, logPosition, timestamp);
                afterEntry(logPosition);
            }

            @Override
            public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                      final long timestamp, final int responseStreamId, final String responseChannel,
                                      final byte[] encodedPrincipal) {
                applier.onSessionOpen(clusterSessionId, logPosition, timestamp, encodedPrincipal);
                afterEntry(logPosition);
            }

            @Override
            public void onSessionClose(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                       final long timestamp, final CloseReason closeReason) {
                applier.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
                afterEntry(logPosition);
            }

            @Override
            public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                           final ClusterAction action, final int flags) {
                tailAt = logPosition; // a member never snapshots from inside onStart; the action reaches no engine
                return false;
            }

            @Override
            public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                                 final long timestamp, final long termBaseLogPosition,
                                                 final int leaderMemberId, final int logSessionId,
                                                 final TimeUnit timeUnit, final int appVersion) {
                // G28: the live service sets its term id from every term event (ACS onNewLeadershipTermEvent)
                if (!faults.tailTermUpdateOff) {
                    term[0] = leadershipTermId;
                    termSink.accept(leadershipTermId);
                }
                if (!applier.failed()) {
                    applier.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
                }
                tailAt = logPosition;
            }

            private void afterEntry(final long logPosition) {
                // the live wrappers' post-entry step is drainEgress(); here the sink already dropped the events
                tailAt = logPosition;
                frames[0]++;
                faults.onTailFrame(frames[0]);
            }
        };

        applier.setEventSink(new CoreEventPublisher(sink, () -> false, CoreEventPublisher.LiveBalanceTap.NONE));
        final long startNs = clock.nanoTime();
        final long timeoutNs = TimeUnit.MILLISECONDS.toNanos(settings.tailReplayTimeoutMs());
        final long deadlineNs = startNs + timeoutNs;
        tailAt = from;
        final Thread watchdog = startWatchdog(deadlineNs);
        final long replaySessionId;
        try {
            replaySessionId = archive.startReplay(recordingId, from, to - from, ShadowAlias.REPLAY_CHANNEL,
                    ShadowAlias.REPLAY_STREAM_ID);
        } catch (final RuntimeException refused) {
            watchdog.interrupt();
            applier.setEventSink(liveSink);
            throw new Halt("FATAL shadow restore: tail replay of recording " + recordingId + " [" + from + "," + to
                    + "] refused by the archive: " + refused, refused);
        }
        Subscription subscription = null;
        try {
            final int sessionId = (int)replaySessionId;
            subscription = aeron.addSubscription(ChannelUri.addSessionId(ShadowAlias.REPLAY_CHANNEL, sessionId),
                    ShadowAlias.REPLAY_STREAM_ID);
            final IdleStrategy idle = new BackoffIdleStrategy(10, 20, 1_000, 1_000_000);
            Image image;
            while ((image = subscription.imageBySessionId(sessionId)) == null) {
                timedOut(deadlineNs);
                idle.idle();
            }
            if (image.joinPosition() != from) {
                throw new Halt("FATAL shadow restore: tail replay of recording " + recordingId + " joined at "
                        + image.joinPosition() + ", expected " + from);
            }
            final ClusterLogReader reader = new ClusterLogReader(adapter, TAIL_FRAGMENT_LIMIT, from);
            idle.reset();
            while (reader.position() < to) {
                final int n;
                try {
                    n = reader.poll(image, to);
                } catch (final ClusterLogReaderStoppedException stopped) {
                    final Throwable cause = stopped.stop().cause();
                    if (cause instanceof com.openexchange.cluster.auth.PrincipalLessSessionException principalLess) {
                        throw principalLess;
                    }
                    throw new Halt("FATAL shadow restore: tail reader stopped: " + stopped.stop(), stopped);
                }
                if (n == 0) {
                    if (reader.position() < to && (image.isEndOfStream() || image.isClosed())) {
                        throw new Halt("FATAL shadow restore: tail replay ended at " + reader.position() + " before P_cm=" + to
                                + " (recording " + recordingId + ")");
                    }
                    timedOut(deadlineNs);
                }
                idle.idle(n);
            }
            if (reader.position() != to || reader.midMessage()) {
                throw new Halt("FATAL shadow restore: tail ended at " + reader.position() + (reader.midMessage() ? " mid-message"
                        : "") + ", expected P_cm=" + to);
            }
        } finally {
            watchdog.interrupt();
            try {
                archive.stopReplay(replaySessionId);
            } catch (final RuntimeException ignore) {
                // the replay already ended (an unknown session is OK to the archive)
            }
            CloseHelper.quietClose(subscription);
            applier.setEventSink(liveSink);
            counters.add(SnapshotModeCounters.C.TAIL_FRAMES_TOTAL, frames[0]);
            counters.add(SnapshotModeCounters.C.TAIL_EVENTS_DISCARDED_TOTAL, sink.discarded());
            counters.set(SnapshotModeCounters.C.TAIL_MS, TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - startNs));
        }
        System.out.println("[shadow restore] tail (" + from + "," + to + "] of recording " + recordingId + ": " + frames[0]
                + " frame(s), " + sink.discarded() + " event(s) discarded (" + sink.bytes() + " B), term " + termAtStart
                + " -> " + term[0]);
        if (term[0] != stub.leadershipTermId()) {
            throw new Halt("FATAL shadow restore: tail ended at term " + term[0] + ", stub says " + stub.leadershipTermId());
        }
        return term[0];
    }

    private void timedOut(final long deadlineNs) {
        if (clock.nanoTime() - deadlineNs > 0) {
            throw new Halt(timeoutText());
        }
    }

    private String timeoutText() {
        return "FATAL shadow restore: tail replay timed out at " + tailAt + " (journal ring full?) after "
                + settings.tailReplayTimeoutMs() + " ms (SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS)";
    }

    /**
     * Rule 12.14: an apply that blocks inside the money-journal ring never returns to the loop's deadline check, so a
     * daemon thread stops the node at the same deadline.
     */
    private Thread startWatchdog(final long deadlineNs) {
        // a grace second after the loop's own deadline, so a loop that is not blocked always reports first
        final long watchdogDeadlineNs = deadlineNs + WATCHDOG_GRACE_NS;
        final Thread t = new Thread(() -> {
            try {
                long remaining;
                while ((remaining = watchdogDeadlineNs - clock.nanoTime()) > 0) {
                    TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
                }
                watchdogHalt.accept(timeoutText());
            } catch (final InterruptedException done) {
                Thread.currentThread().interrupt();
            }
        }, "rcs-tail-watchdog");
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ------------------------------------------------------------------------------------------- RecordingLog (read)

    /** Valid term entries by term id (entry order is never relied on), each with the next term's base. */
    static List<Term> readTerms(final File clusterDir) {
        final List<RecordingLog.Entry> entries = new ArrayList<>();
        if (new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            try (RecordingLog log = new RecordingLog(clusterDir, false)) {
                for (final RecordingLog.Entry e : log.entries()) {
                    if (e.type == RecordingLog.ENTRY_TYPE_TERM && e.isValid) {
                        entries.add(e);
                    }
                }
            }
        }
        entries.sort(Comparator.comparingLong(e -> e.leadershipTermId));
        final List<Term> out = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            final RecordingLog.Entry e = entries.get(i);
            final long next = i + 1 < entries.size() ? entries.get(i + 1).termBaseLogPosition : Long.MAX_VALUE;
            out.add(new Term(e.leadershipTermId, e.termBaseLogPosition, e.recordingId, next));
        }
        return out;
    }

    /**
     * Review F2: every term whose frames can end in {@code (lo, hi]} (base {@code < hi} and next base {@code > lo})
     * names ONE recording. Null when it holds; else the refusal text.
     */
    static String singleRecording(final List<Term> terms, final long lo, final long hi) {
        final Set<Long> ids = new LinkedHashSet<>();
        for (final Term t : terms) {
            if (t.base() < hi && t.nextBase() > lo) {
                ids.add(t.recordingId());
            }
        }
        if (ids.size() == 1) {
            return null;
        }
        if (ids.isEmpty()) {
            return "tail range (" + lo + "," + hi + "] has no term entry: unsupported";
        }
        final StringBuilder sb = new StringBuilder();
        for (final long id : ids) {
            sb.append(sb.length() == 0 ? "" : ",").append(id);
        }
        return "tail range (" + lo + "," + hi + "] spans recordings " + sb + ": unsupported";
    }

    /** The one recording of {@code (lo, hi]} when {@link #singleRecording} holds, else -1. */
    static long rangeRecording(final List<Term> terms, final long lo, final long hi) {
        long id = -1;
        for (final Term t : terms) {
            if (t.base() < hi && t.nextBase() > lo) {
                if (id != -1 && id != t.recordingId()) {
                    return -1;
                }
                id = t.recordingId();
            }
        }
        return id;
    }

    /** 7.1: null when virtual genesis may run; else why not. */
    static String virtualGenesisPrecondition(final List<Term> terms, final long logStart, final long pCm) {
        if (logStart != 0) {
            return "log recording starts at " + logStart;
        }
        if (terms.isEmpty() || terms.get(0).base() != 0) {
            return "oldest term entry starts at " + (terms.isEmpty() ? "none" : Long.toString(terms.get(0).base()));
        }
        return singleRecording(terms, 0, pCm);
    }
}
