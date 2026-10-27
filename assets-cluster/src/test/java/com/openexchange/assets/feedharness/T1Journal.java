// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * RC2 S9h 7.2: {@code t1-journal}, the ME writer side of the T1 harness. One JVM hosts K journal archives, each with its
 * own embedded MediaDriver (SHARED) and Archive: control {@code aeron:udp?endpoint=localhost:<portBase + 100 k>} stream
 * 4010, local control 4011, the journal on stream 4001 recorded from {@code aeron:ipc?term-length=64k} (the IPC MTU, 1408,
 * is the production journal MTU), 1 MiB segments. Control lines arrive on stdin; each answers one line, {@code OK ...} or
 * {@code ERR ...}. Every write waits until the recording position covers it before answering OK.
 *
 * <p>Records are addressed by journalSeq when the LOADed journal is dense from 1 (a story's), else by their 1-based
 * position in the file (an E1 member stream, whose seqs repeat across recordings and epochs).</p>
 */
public final class T1Journal {

    public static final int CONTROL_STREAM = 4010;
    public static final int LOCAL_CONTROL_STREAM = 4011;
    public static final int JOURNAL_STREAM = 4001;
    public static final String JOURNAL_CHANNEL = "aeron:ipc?term-length=64k";
    static final int HOLD_STREAM = 9_999;

    private T1Journal() {
    }

    static int main(final String[] args, final InputStream in, final PrintStream out, final PrintStream err) throws Exception {
        if (args.length != 1) {
            throw new FeedHarnessMain.Usage("t1-journal (control lines on stdin; T1_JOURNAL_DIR names the work dir)");
        }
        final String dir = System.getenv("T1_JOURNAL_DIR");
        final Path root = dir != null ? Path.of(dir) : Files.createTempDirectory("t1-journal");
        try (Host host = new Host(root)) {
            final BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                final String reply = host.handle(line.trim());
                out.println(reply);
                out.flush();
                if (reply.equals("OK bye")) {
                    return FeedHarnessMain.OK;
                }
            }
        }
        return FeedHarnessMain.OK;
    }

    /** One journal archive (one ME member's journal). */
    static final class Source implements AutoCloseable {
        final int index;
        final File dir;
        final int controlPort;
        final int maxReplays;
        MediaDriver driver;
        Archive archive;
        AeronArchive client;
        ExclusivePublication pub;
        long recordingId = -1;
        int termLength = 64 * 1024;
        final TreeSet<Long> held = new TreeSet<>();
        final Map<Long, String> corrupt = new TreeMap<>();
        final TreeMap<Long, Long> epochFrom = new TreeMap<>();
        long lastWritten;
        final List<long[]> replays = new ArrayList<>();
        final List<Subscription> holdSubs = new ArrayList<>();

        Source(final int index, final File dir, final int controlPort, final int maxReplays) {
            this.index = index;
            this.dir = dir;
            this.controlPort = controlPort;
            this.maxReplays = maxReplays;
            launch();
        }

        String driverDir() {
            return new File(dir, "driver").getAbsolutePath();
        }

        String controlChannel() {
            return "aeron:udp?endpoint=localhost:" + controlPort;
        }

        void launch() {
            driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(driverDir()).dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED).sharedIdleStrategy(new BackoffIdleStrategy()));
            final Archive.Context ac = new Archive.Context().aeronDirectoryName(driverDir())
                    .archiveDir(new File(dir, "archive")).controlChannel(controlChannel()).controlStreamId(CONTROL_STREAM)
                    .localControlStreamId(LOCAL_CONTROL_STREAM).replicationChannel("aeron:udp?endpoint=localhost:0")
                    .recordingEventsEnabled(false).threadingMode(ArchiveThreadingMode.SHARED).fileSyncLevel(0)
                    .segmentFileLength(1024 * 1024).archiveId(1_000 + index);
            if (maxReplays > 0) {
                ac.maxConcurrentReplays(maxReplays);
            }
            archive = Archive.launch(ac);
            client = AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(driverDir())
                    .controlRequestChannel(controlChannel()).controlRequestStreamId(CONTROL_STREAM)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
        }

        boolean alive() {
            return client != null;
        }

        void startRecording(final int term) {
            stopRecording();
            pub = client.addRecordedExclusivePublication(term == 64 * 1024 ? JOURNAL_CHANNEL
                    : "aeron:ipc?term-length=" + term, JOURNAL_STREAM);
            final CountersReader counters = client.context().aeron().countersReader();
            final long deadline = System.currentTimeMillis() + 10_000;
            int id;
            while ((id = RecordingPos.findCounterIdBySession(counters, pub.sessionId(), client.archiveId()))
                    == CountersReader.NULL_COUNTER_ID) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("source " + index + ": the recording did not start");
                }
                Thread.onSpinWait();
            }
            recordingId = RecordingPos.getRecordingId(counters, id);
        }

        void stopRecording() {
            if (pub != null) {
                awaitRecorded();
                client.stopRecording(pub);
                CloseHelper.quietClose(pub);
                pub = null;
            }
        }

        void awaitRecorded() {
            final long target = pub.position();
            final long deadline = System.currentTimeMillis() + 10_000;
            while (client.getRecordingPosition(recordingId) < target) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("source " + index + ": recording position stuck below " + target);
                }
                Thread.onSpinWait();
            }
        }

        void write(final byte[] bytes) {
            if (pub == null) {
                startRecording(termLength);
            }
            final UnsafeBuffer b = new UnsafeBuffer(bytes);
            final long deadline = System.currentTimeMillis() + 10_000;
            while (pub.offer(b, 0, bytes.length) < 0) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("source " + index + ": offer refused");
                }
                Thread.onSpinWait();
            }
            awaitRecorded();
        }

        @Override
        public void close() {
            for (final long[] r : replays) {
                try {
                    client.stopReplay(r[0]);
                } catch (RuntimeException ignored) {
                    // the replay may have ended
                }
            }
            replays.clear();
            holdSubs.forEach(CloseHelper::quietClose);
            holdSubs.clear();
            if (pub != null) {
                CloseHelper.quietClose(pub);
                pub = null;
            }
            CloseHelper.quietCloseAll(client, archive, driver);
            client = null;
            archive = null;
            driver = null;
        }
    }

    /** The control host: all sources and the loaded journal. */
    public static final class Host implements AutoCloseable {
        private final Path root;
        private final List<Source> sources = new ArrayList<>();
        private List<byte[]> journal = new ArrayList<>();
        private boolean dense;
        private final JournalRecords j = new JournalRecords();

        public Host(final Path root) {
            this.root = root;
        }

        public Source source(final int k) {
            return sources.get(k);
        }

        public int sources() {
            return sources.size();
        }

        /** One control line; returns the reply line. */
        public String handle(final String line) {
            final String[] a = line.split("\\s+");
            try {
                switch (a[0]) {
                    case "SOURCES": return sourcesVerb(a);
                    case "LOAD": return load(a);
                    case "WRITE": return writeVerb(a);
                    case "RECORD": return record(a);
                    case "STOP": return each(a[1], s -> s.stopRecording(), "stopped");
                    case "EMPTY": return each(a[1], s -> {
                        s.startRecording(s.termLength);
                        s.stopRecording();
                    }, "empty recording");
                    case "RESTART": return restart(a);
                    case "HOLE": return each(a[1], s -> s.held.add(Long.parseLong(a[2])), "held " + a[2]);
                    case "FILL": return fill(a);
                    case "PAD": return each(a[1], s -> {
                        if (s.pub == null) {
                            s.startRecording(s.termLength);
                        }
                        while (s.pub.appendPadding(Integer.parseInt(a[2])) < 0) {
                            Thread.onSpinWait();
                        }
                        s.awaitRecorded();
                    }, "padded " + a[2]);
                    case "PURGE": return each(a[1], s -> s.client.purgeSegments(s.recordingId, Long.parseLong(a[2])),
                            "purged below " + a[2]);
                    case "CORRUPT": return each(a[1], s -> {
                        if (!List.of("schema3", "version2", "block", "length", "head0", "oversize").contains(a[3])) {
                            throw new IllegalArgumentException("unknown mode " + a[3]);
                        }
                        s.corrupt.put(Long.parseLong(a[2]), a[3]);
                    }, "corrupt " + a[2] + " " + a[3]);
                    case "REGENESIS": return regenesis(a);
                    case "EPOCH": return each(a[1], s -> s.epochFrom.put(Long.parseLong(a[2]), Long.decode(a[3])),
                            "epoch from " + a[2]);
                    case "KILL": return each(a[1], Source::close, "killed");
                    case "REVIVE": return each(a[1], Source::launch, "revived");
                    case "HOLD_REPLAY": return each(a[1], this::holdReplay, "replay held");
                    case "RELEASE_REPLAY": return each(a[1], s -> {
                        for (final long[] r : s.replays) {
                            s.client.stopReplay(r[0]);
                        }
                        s.replays.clear();
                        s.holdSubs.forEach(CloseHelper::quietClose);
                        s.holdSubs.clear();
                    }, "replays released");
                    case "LIST": return list(a);
                    case "QUIT":
                        close();
                        return "OK bye";
                    default:
                        return "ERR unknown verb " + a[0];
                }
            } catch (RuntimeException e) {
                return "ERR " + a[0] + ": " + e;
            }
        }

        private interface Action {
            void on(Source s);
        }

        private List<Source> pick(final String which) {
            if (which.equals("all")) {
                return sources;
            }
            return List.of(sources.get(Integer.parseInt(which)));
        }

        private String each(final String which, final Action action, final String what) {
            for (final Source s : pick(which)) {
                action.on(s);
            }
            return "OK " + what;
        }

        private String sourcesVerb(final String[] a) {
            final int k = Integer.parseInt(a[1]);
            final int base = Integer.parseInt(a[2]);
            int maxReplays = 0;
            for (int i = 3; i < a.length; i++) {
                if (a[i].startsWith("maxReplays=")) {
                    maxReplays = Integer.parseInt(a[i].substring("maxReplays=".length()));
                }
            }
            for (int i = 0; i < k; i++) {
                final File d = root.resolve("src" + i).toFile();
                IoUtil.delete(d, true);
                sources.add(new Source(i, d, base + 100 * i, maxReplays));
            }
            final StringBuilder sb = new StringBuilder("OK ");
            for (final Source s : sources) {
                sb.append(s.controlChannel()).append(' ');
            }
            return sb.toString().trim();
        }

        private String load(final String[] a) throws RuntimeException {
            try {
                journal = TranslatorProofs.recordsOf(FeedFiles.decodeFjr(Files.readAllBytes(Path.of(a[1]))));
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
            dense = true;
            for (int i = 0; i < journal.size(); i++) {
                dense &= JournalRecords.journalSeq(journal.get(i)) == i + 1;
            }
            return "OK loaded " + journal.size() + (dense ? " dense" : " by position");
        }

        /** The bytes of record {@code n} (seq or position) as source {@code s} writes it: epoch override and corruption. */
        private byte[] bytesFor(final Source s, final long n) {
            byte[] r = journal.get((int) n - 1);
            final Map.Entry<Long, Long> ep = s.epochFrom.floorEntry(n);
            if (ep != null) {
                r = JournalRecords.withHead(r, JournalRecords.journalSeq(r), ep.getValue(), JournalRecords.egressSeq(r));
            }
            final String mode = s.corrupt.get(n);
            return mode == null ? r : corrupted(r, mode);
        }

        static byte[] corrupted(final byte[] r, final String mode) {
            final byte[] c = r.clone();
            switch (mode) {
                case "schema3" -> {
                    return schema3Trade();
                }
                case "version2" -> c[6] = 2;
                case "block" -> c[0] = (byte) (c[0] + 1);
                case "length" -> {
                    return java.util.Arrays.copyOf(r, r.length + 1);
                }
                case "head0" -> new UnsafeBuffer(c).putLong(8, 0L, ByteOrder.LITTLE_ENDIAN);
                case "oversize" -> {
                    return java.util.Arrays.copyOf(r, 2000);
                }
                default -> throw new IllegalArgumentException(mode);
            }
            return c;
        }

        /** A schema 3 v1 JournalTrade (the frozen bridge's wire). */
        static byte[] schema3Trade() {
            final UnsafeBuffer b = new UnsafeBuffer(new byte[256]);
            final com.match.infrastructure.journal.generated.JournalTradeEncoder e =
                    new com.match.infrastructure.journal.generated.JournalTradeEncoder();
            e.wrapAndApplyHeader(b, 0, new com.match.infrastructure.journal.generated.MessageHeaderEncoder())
                    .egressSeq(1000L).tradeId(42L).marketId(1).takerOrderId(11L).takerUserId(200L).makerOrderId(22L)
                    .makerUserId(100L).price(6_000_000_000_000L).quantity(100_000_000L)
                    .takerIsBuy(com.match.infrastructure.journal.generated.BooleanType.TRUE).timestamp(1_752_100_000_123L)
                    .takerOmsOrderId(2L).makerOmsOrderId(1L);
            final byte[] out = new byte[8 + e.encodedLength()];
            b.getBytes(0, out);
            return out;
        }

        private String writeVerb(final String[] a) {
            final long from = Long.parseLong(a[2]);
            final long to = Long.parseLong(a[3]);
            if (from < 1 || to > journal.size() || from > to + 1) {
                throw new IllegalArgumentException("WRITE " + from + ".." + to + " outside the loaded journal (" + journal.size() + ")");
            }
            int written = 0;
            for (final Source s : pick(a[1])) {
                if (!s.alive()) {
                    throw new IllegalStateException("source " + s.index + " is killed");
                }
                for (long n = from; n <= to; n++) {
                    if (s.held.contains(n)) {
                        continue;
                    }
                    s.write(bytesFor(s, n));
                    s.lastWritten = Math.max(s.lastWritten, n);
                    written++;
                }
            }
            return "OK wrote " + written;
        }

        private String record(final String[] a) {
            int term = 64 * 1024;
            for (int i = 2; i < a.length; i++) {
                if (a[i].startsWith("term=")) {
                    final String v = a[i].substring(5).toLowerCase();
                    term = v.endsWith("m") ? Integer.parseInt(v.substring(0, v.length() - 1)) << 20
                            : v.endsWith("k") ? Integer.parseInt(v.substring(0, v.length() - 1)) << 10 : Integer.parseInt(v);
                }
            }
            final int t = term;
            return each(a[1], s -> {
                s.termLength = t;
                s.startRecording(t);
            }, "recording term " + t);
        }

        private String restart(final String[] a) {
            final long from = Long.parseLong(a[2]);
            return each(a[1], s -> {
                final long upTo = s.lastWritten;
                s.startRecording(s.termLength);
                for (long n = from; n <= upTo; n++) {
                    if (!s.held.contains(n)) {
                        s.write(bytesFor(s, n));
                    }
                }
            }, "restarted from " + from);
        }

        private String fill(final String[] a) {
            final long n = Long.parseLong(a[2]);
            return each(a[1], s -> {
                if (!s.held.remove(n)) {
                    throw new IllegalStateException("seq " + n + " is not held on source " + s.index);
                }
                s.startRecording(s.termLength);
                s.write(bytesFor(s, n));
            }, "filled " + n);
        }

        private String regenesis(final String[] a) {
            final long epoch = Long.decode(a[2]);
            List<byte[]> src = journal;
            if (a.length > 3) {
                try {
                    src = TranslatorProofs.recordsOf(FeedFiles.decodeFjr(Files.readAllBytes(Path.of(a[3]))));
                } catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            final List<byte[]> fresh = new ArrayList<>();
            if (FeedHarnessSeams.on(FeedHarnessSeams.REGENESIS_AS_RELABEL)) {
                for (final byte[] r : src) {
                    fresh.add(JournalRecords.withHead(r, JournalRecords.journalSeq(r), epoch, JournalRecords.egressSeq(r)));
                }
            } else {
                long seq = 1;
                long egress = 1;
                fresh.add(j.config(seq++, epoch, egress, 1, 2, 4096, 12, 0));
                for (final byte[] r : src) {
                    if (JournalRecords.templateId(r) == 5 && JournalRecords.journalSeq(r) == 1) {
                        continue; // the old epoch's FRESH: the new FRESH replaces it
                    }
                    egress = Math.max(egress, JournalRecords.egressSeq(r));
                    fresh.add(JournalRecords.withHead(r, seq++, epoch, egress));
                }
            }
            return each(a[1], s -> {
                s.startRecording(s.termLength);
                for (final byte[] r : fresh) {
                    s.write(r);
                }
            }, "regenesis epoch " + a[2] + " " + fresh.size() + " records");
        }

        private void holdReplay(final Source s) {
            if (s.pub == null) {
                throw new IllegalStateException("source " + s.index + " has no active recording to hold a replay on");
            }
            final Subscription sub = s.client.context().aeron().addSubscription("aeron:ipc", HOLD_STREAM);
            s.holdSubs.add(sub);
            s.replays.add(new long[] {s.client.startReplay(s.recordingId, 0L, Long.MAX_VALUE, "aeron:ipc", HOLD_STREAM)});
        }

        private String list(final String[] a) {
            final StringBuilder sb = new StringBuilder("OK");
            for (final Source s : pick(a[1])) {
                s.client.listRecordingsForUri(0, 10_000, "aeron:ipc", JOURNAL_STREAM,
                        (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
                         initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                         originalChannel, sourceIdentity) -> sb.append(' ').append(recordingId).append(' ').append(startPosition)
                                .append(' ').append(stopPosition).append(';'));
            }
            return sb.toString();
        }

        @Override
        public void close() {
            for (final Source s : sources) {
                if (s.alive()) {
                    s.close();
                }
            }
        }
    }
}
