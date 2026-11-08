// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.ShadowClusterRig.Client;
import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.TimeInForce;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.assets.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowStub;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.RecordingLog;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 contract 11.13 (WK-7), a FUNCTIONAL guard only: leader-pause milliseconds are not asserted here (S10 measures
 * them on a rig). Three members at 10k resting orders:
 * <ul>
 *   <li>stub mode: every member's service snapshot is the stub's envelope (a format constant, 64 payload bytes plus the
 *       kit envelope, asserted structurally), {@code awaitSnapshotRecorded} is not called
 *       ({@code rcs_await_recorded_skipped_total == 1}), the line says {@code recordMs=0}, the group is valid and a
 *       restart recovers from it;</li>
 *   <li>control {@code RCS_MODE=off}: the bytes are the full image (the test can see the difference);</li>
 *   <li>Q9 drill, in process: a member's container is closed right after {@code onTakeSnapshot} returns, 20 times, in
 *       stub mode; every restart finds every valid group's recordings complete (never {@code unknown recording id})
 *       and recovers. A container close is not a kill inside the window (Aeron's own await and the ack may still run),
 *       so the window drill proper is the child leg (D6):
 *       {@code ShadowRestoreExternalDriverProcessTest.q9HaltRightAfterOnTakeSnapshotInAChildJvmLeavesNoGroupAndEveryRestartRecovers},
 *       {@code Runtime.halt} right after {@code onTakeSnapshot} returns, at least 5 times, with the group proven
 *       absent from {@code RecordingLog}.</li>
 * </ul>
 */
public class StubSnapshotLeaderPauseClusterTest {

    private static final int ORDERS = 10_000;
    private static final Pattern LINE = Pattern.compile(
            "\\[SNAPSHOT\\] accounts=\\d+ bytes=(\\d+) .* recordMs=(\\d+) logPosition=(\\d+) kind=(stub|full)");

    /** One parsed {@code [SNAPSHOT]} line: bytes, recordMs, logPosition, kind. */
    private record Taken(long bytes, long recordMs, long logPosition, String kind, String line) {
    }

    private static List<Taken> taken(final ShadowClusterRig rig, final long at) {
        final List<Taken> out = new ArrayList<>();
        for (final String l : rig.tee.all("[SNAPSHOT] accounts=")) {
            final Matcher m = LINE.matcher(l);
            if (m.find() && Long.parseLong(m.group(3)) == at) {
                out.add(new Taken(Long.parseLong(m.group(1)), Long.parseLong(m.group(2)), at, m.group(4), l));
            }
        }
        return out;
    }

    /** The stub assertion: every member's service snapshot at {@code at} is the stub envelope and skipped the await. */
    private static void assertStubSnapshots(final List<Taken> lines, final int members) {
        assertEquals("one [SNAPSHOT] line per member: " + lines, members, lines.size());
        for (final Taken t : lines) {
            if (t.bytes() != ShadowStub.ENVELOPED_LENGTH) {
                throw new AssertionError("service snapshot bytes " + t.bytes() + " != stub envelope length "
                        + ShadowStub.ENVELOPED_LENGTH + ": " + t.line());
            }
            assertEquals("kind=stub: " + t.line(), "stub", t.kind());
            assertEquals("recordMs=0 on the stub path (no awaitSnapshotRecorded): " + t.line(), 0, t.recordMs());
        }
    }

    private static ShadowClusterRig.Member[] loaded(final ShadowClusterRig rig, final boolean stub) throws Exception {
        for (final Member m : rig.members) {
            m.settings = stub ? StubStory.shadow() : StubStory.off();
        }
        rig.bootAll();
        final Member leader = rig.awaitLeader(null);
        final Member f = rig.followerOtherThan(leader, null);
        final Member g = rig.followerOtherThan(leader, f);
        final OrderLoad load = OrderLoad.prepare(rig, g, ORDERS / OrderLoad.ORDERS_PER_USER);
        load.orders(rig, 1, ORDERS, TimeInForce.GTC, 0L);
        rig.awaitImagesEqual(f, leader, 60_000);
        assertEquals("10k resting orders", ORDERS, leader.service.engine().orders().orders().size());
        return new Member[] {leader, f, g};
    }

    @Test
    public void inStubModeTheServiceSnapshotIsTheStubAndTheAwaitIsSkipped() throws Exception {
        assertEquals("the stub is 64 payload bytes in the kit envelope (a format constant)",
                64 + SnapshotEnvelope.HEADER_LENGTH + SnapshotEnvelope.TRAILER_LENGTH, ShadowStub.ENVELOPED_LENGTH);
        try (ShadowClusterRig rig = new ShadowClusterRig(24100, "rcs-s4-11.13-stub")) {
            final Member[] ms = loaded(rig, true);
            final Member leader = ms[0];
            final Member f = ms[1];
            rig.tee.clear();
            final long pCm = rig.snapshotAll();
            final List<Taken> lines = taken(rig, pCm);
            assertStubSnapshots(lines, 3);
            for (final Member m : rig.members) {
                assertEquals("member " + m.id + " rcs_await_recorded_skipped_total", 1,
                        ServiceProbe.rcs(m.service, "rcs_await_recorded_skipped_total"));
                assertEquals("member " + m.id + " rcs_snapshot_stub_total", 1, ServiceProbe.rcs(m.service, "rcs_snapshot_stub_total"));
                assertTrue("member " + m.id + " holds a valid group at P_cm", rig.snapshots(m).stream().anyMatch(e -> e[0] == pCm));
                assertCompleteGroups(rig, m);
            }
            System.out.println("GREEN 11.13 stub snapshots: " + lines.stream().map(Taken::line).toList());
            // recovery from that group: an EQUAL shadow, a restart of f, the leader's image
            final RecordedLog log = rig.captureCommitted(f);
            final Written w = rig.producer(f).capacity(64 << 20).produce(new Who(ProducerKind.FOLLOWER, 600), log, pCm);
            assertTrue(w.toString(), w.ok());
            rig.tee.clear();
            rig.kill(f);
            rig.restart(f);
            rig.awaitReady(f, 120_000);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue(f.halts.toString(), f.halts.isEmpty());
            RcsDryRun.requirePathLine(rig.tee, "EQUAL P_s=" + pCm, "11.13 recovery from the stub group");
            System.out.println("GREEN 11.13 recovery from the stub group: " + rig.tee.all("restored path="));
        }
    }

    @Test
    public void controlRcsModeOffWritesTheFullImage() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(24200, "rcs-s4-11.13-off")) {
            final Member[] ms = loaded(rig, false);
            rig.forgetAll(); // at rest: the image read below is the image the snapshot wrote
            rig.pumpMs(500);
            final long full = ServiceProbe.image(ms[0].service).length;
            rig.tee.clear();
            final long pCm = rig.snapshotAll();
            final List<Taken> lines = taken(rig, pCm);
            assertEquals("one line per member", 3, lines.size());
            for (final Taken t : lines) {
                assertEquals("RCS_MODE=off writes the full image: " + t.line(), full, t.bytes());
                assertEquals("kind=full", "full", t.kind());
            }
            for (final Member m : rig.members) {
                assertEquals("the full path keeps awaitSnapshotRecorded", 0,
                        ServiceProbe.rcs(m.service, "rcs_await_recorded_skipped_total"));
            }
            System.out.println("CONTROL 11.13 off: full image " + full + " B vs stub " + ShadowStub.ENVELOPED_LENGTH + " B: "
                    + lines.stream().map(Taken::line).toList());
            RcsDryRun.nc("11.13-rcs-mode-off", "service snapshot bytes " + full + " != stub envelope length "
                    + ShadowStub.ENVELOPED_LENGTH, () -> assertStubSnapshots(lines, 3));
        }
    }

    /** Q9 (M Q9, 3.4): a kill right after onTakeSnapshot returns never leaves a group naming an incomplete recording. */
    @Test(timeout = 1_200_000)
    public void q9KilledRightAfterOnTakeSnapshotTwentyTimesEveryGroupIsComplete() throws Exception {
        final int kills = Integer.getInteger("rcs.q9.kills", 20);
        try (ShadowClusterRig rig = new ShadowClusterRig(24300, "rcs-s4-11.13-q9")) {
            for (final Member m : rig.members) {
                m.settings = StubStory.shadow();
            }
            final AtomicInteger returned = new AtomicInteger();
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            f.beforeLaunch = svc -> ServiceProbe.afterTakeSnapshot(svc, returned::incrementAndGet);
            rig.kill(f);
            rig.restart(f); // the hook is installed from this boot on
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(100);
            rig.awaitImagesEqual(f, leader, 60_000);
            int groupsWritten = 0;
            for (int i = 1; i <= kills; i++) {
                final int seen = returned.get();
                final int groupsBefore = rig.snapshots(f).size();
                final AtomicCounter toggle = ClusterControl.findControlToggle(
                        leader.cfg.consensusModuleContext().aeron().countersReader(),
                        leader.cfg.consensusModuleContext().clusterId());
                assertTrue("the SNAPSHOT toggle", ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
                rig.await(() -> returned.get() > seen, 60_000, "f's onTakeSnapshot must return");
                rig.kill(f); // immediately after onTakeSnapshot returned: Aeron's own await and the ack race this kill
                final boolean written = rig.snapshots(f).size() > groupsBefore;
                if (written) {
                    groupsWritten++;
                }
                tool.deposits(10);
                rig.restart(f);
                rig.awaitReady(f, 120_000);
                rig.awaitImagesEqual(f, leader, 60_000);
                assertTrue("kill " + i + ": " + f.halts, f.halts.isEmpty());
                assertCompleteGroups(rig, f);
                System.out.println("[11.13-Q9] kill " + i + "/" + kills + ": group at the kill "
                        + (written ? "written" : "not written") + ", restart #" + f.starts + " recovered; "
                        + rig.tee.all("restored path="));
                rig.tee.clear();
            }
            System.out.println("GREEN 11.13 Q9: " + kills + " kills right after onTakeSnapshot, " + groupsWritten
                    + " of them left a group; every valid group's recordings complete, every restart recovered");
        }
    }

    /** Every valid snapshot entry of {@code m}'s RecordingLog names a recording the archive holds, stopped (complete). */
    private static void assertCompleteGroups(final ShadowClusterRig rig, final Member m) {
        final AeronArchive archive = rig.archive(m);
        try (RecordingLog log = new RecordingLog(m.clusterDir(), false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type != RecordingLog.ENTRY_TYPE_SNAPSHOT || !e.isValid) {
                    continue;
                }
                final long[] stop = {Long.MIN_VALUE};
                final int found = archive.listRecording(e.recordingId, (controlSessionId, correlationId, recordingId,
                        startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
                        termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                        sourceIdentity) -> stop[0] = stopPosition);
                assertEquals("member " + m.id + ": snapshot entry at " + e.logPosition + " (service " + e.serviceId
                        + ") names recording " + e.recordingId + ": unknown recording id", 1, found);
                assertTrue("member " + m.id + ": snapshot recording " + e.recordingId + " is complete (stopped): " + stop[0],
                        stop[0] != AeronArchive.NULL_POSITION && stop[0] > 0);
            }
        }
    }
}
