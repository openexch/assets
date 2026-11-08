// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Client;
import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.RcsSettings;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * RCS S4 W3: the single-member story of 11.3, 11.5, 11.6 and 11.10 on {@link ShadowClusterRig} (one member, embedded
 * driver, 64 KiB log terms and archive segments so a purge is reachable with a few hundred frames). Boot, ADOPT,
 * deposits, a cluster snapshot (a stub in shadow mode), shadows from the member's OWN committed log (contract 11.0),
 * quiesce (close every client first: they ride the member's driver), kill -9 equivalent, restart on the same
 * directories, then either the restored image or the halt text.
 */
final class StubStory implements AutoCloseable {

    static final int SEGMENT = 64 * 1024;

    final ShadowClusterRig rig;
    final Member m;
    Client admin;
    Client tool;
    private int producerId = 200;

    StubStory(final int portBase, final String name, final RcsSettings settings) {
        this.rig = new ShadowClusterRig(portBase, name, 1, "64k", SEGMENT);
        this.m = rig.members.get(0);
        this.m.settings = settings;
    }

    static RcsSettings shadow() {
        return RcsSettings.with(Map.of(RcsSettings.Key.RCS_MODE, "shadow"));
    }

    static RcsSettings off() {
        return RcsSettings.defaults();
    }

    StubStory start(final boolean adopt) throws InterruptedException {
        rig.boot(m);
        rig.awaitLeader(null);
        connect(adopt);
        return this;
    }

    void connect(final boolean adopt) throws InterruptedException {
        admin = rig.connect(SessionClass.ADMIN, m);
        if (adopt) {
            rig.adopt(admin);
        }
        tool = rig.connect(SessionClass.TOOL, m);
    }

    void deposits(final int n) {
        tool.deposits(n);
    }

    long snapshot() {
        return rig.snapshotAll();
    }

    RecordedLog log() {
        return rig.captureCommitted(m);
    }

    Who who() {
        return new Who(ProducerKind.FOLLOWER, producerId++);
    }

    TestShadowProducer producer() {
        return rig.producer(m);
    }

    Written produce(final RecordedLog log, final long p) {
        final Written w = producer().produce(who(), log, p);
        assertTrue(w.toString(), w.ok());
        System.out.println("[W3] shadow " + w);
        return w;
    }

    /** Frame ends of {@code log}, and the index of {@code p} among them (asserted to be a frame end). */
    static int indexOf(final RecordedLog log, final long p) {
        final int i = Arrays.binarySearch(log.positions(), p);
        assertTrue(p + " is a frame end of the log", i >= 0);
        return i;
    }

    /** Closes every client, waits until the image is at rest (two equal reads 200 ms apart) and returns it. */
    byte[] quiesce() {
        rig.forgetAll();
        final byte[][] last = new byte[1][];
        rig.await(() -> {
            final byte[] a = ServiceProbe.image(m.service);
            rig.pumpMs(200);
            final byte[] b = ServiceProbe.image(m.service);
            last[0] = b;
            return Arrays.equals(a, b);
        }, 30_000, "the image never came to rest");
        return last[0];
    }

    void kill() {
        rig.kill(m);
    }

    void restart(final String... faults) {
        m.faults = faults;
        rig.tee.clear();
        rig.errTee.clear();
        m.halts.clear();
        rig.restart(m);
    }

    long awaitReady() {
        return rig.awaitReady(m, 120_000);
    }

    /** The restored image must come to rest equal to {@code expected}. */
    void awaitImage(final byte[] expected, final String why) {
        final byte[][] last = new byte[1][];
        rig.await(() -> {
            last[0] = ServiceProbe.image(m.service);
            return Arrays.equals(expected, last[0]);
        }, 30_000, () -> why + ": final image differs from baseline (sizes " + expected.length + " vs "
                + (last[0] == null ? -1 : last[0].length) + ")");
    }

    /** Waits for the failFast seam and a stderr line containing {@code fragment}; returns that line. */
    String awaitHalt(final String fragment) {
        rig.await(() -> !m.halts.isEmpty() && line(rig.errTee.lines, fragment) != null, 60_000,
                () -> "no halt with <" + fragment + ">; halts " + m.halts + ", stderr " + rig.errTee.lines + ", stdout "
                        + rig.tee.lines);
        return line(rig.errTee.lines, fragment);
    }

    /** The {@code [SNAPSHOT] restored ...} line of the last boot, or null. */
    String pathLine() {
        return line(rig.tee.lines, "restored ");
    }

    static String line(final List<String> lines, final String fragment) {
        for (final String l : lines) {
            if (l.contains(fragment)) {
                return l;
            }
        }
        return null;
    }

    /**
     * Copies {@code clusterDir}'s RecordingLog into {@code into} with the LAST term entry naming recording id + 1 (a
     * byte-level forgery: Aeron's own {@code appendTerm} refuses a second recording id). Returns the real id.
     */
    static long forgeLastTermRecordingId(final java.io.File clusterDir, final java.io.File into) throws java.io.IOException {
        final java.nio.file.Path src = new java.io.File(clusterDir, io.aeron.cluster.RecordingLog.RECORDING_LOG_FILE_NAME)
                .toPath();
        final byte[] bytes = java.nio.file.Files.readAllBytes(src);
        final java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int lastTerm = -1;
        for (int off = 0; off + io.aeron.cluster.RecordingLog.ENDPOINT_OFFSET <= bytes.length; off += 64) {
            final int type = bb.getInt(off + io.aeron.cluster.RecordingLog.ENTRY_TYPE_OFFSET)
                    & ~io.aeron.cluster.RecordingLog.ENTRY_TYPE_INVALID_FLAG;
            assertTrue("no standby entries (64-byte records only)",
                    type != io.aeron.cluster.RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT);
            if (type == io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM) {
                lastTerm = off;
            }
        }
        assertTrue("a term entry", lastTerm >= 0);
        final long realId = bb.getLong(lastTerm + io.aeron.cluster.RecordingLog.RECORDING_ID_OFFSET);
        bb.putLong(lastTerm + io.aeron.cluster.RecordingLog.RECORDING_ID_OFFSET, realId + 1);
        java.nio.file.Files.write(new java.io.File(into, io.aeron.cluster.RecordingLog.RECORDING_LOG_FILE_NAME).toPath(),
                bytes);
        return realId;
    }

    long counter(final String metric) {
        return ServiceProbe.rcs(m.service, metric);
    }

    @Override
    public void close() {
        rig.close();
    }
}
