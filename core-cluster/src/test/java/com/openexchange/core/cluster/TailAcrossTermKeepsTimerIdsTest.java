// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLog;
import org.junit.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The DRY RUN (C14) of the leader-change-inside-the-tail story on 84b088b with
 * {@code SNAPSHOT_MODE} absent. It proves, before the code exists, what the production test (G28) relies on:
 * <ol>
 *   <li>a shadow at {@code P_s} in term T on both survivors' archives (contract 11.0 producer);</li>
 *   <li>a leader change (T to T+1), more commits, and a cluster snapshot whose {@code ClusterAction(SNAPSHOT, 0)} frame is
 *       in T+1: the {@code [SNAPSHOT]} line's {@code logPosition} equals that action frame's end;</li>
 *   <li>each survivor's {@code RecordingLog} holds two term entries (T and T+1) that name ONE recording id (the
 *       {@code extendRecording} branch, CMA:2366-2368), so the single-recording precondition of 6.4 holds;</li>
 *   <li>{@code P_s < termBase(T+1) <= P_cm}: a TAIL from {@code P_s} to {@code P_cm} crosses the NewLeadershipTermEvent;</li>
 *   <li>the old leader restarts and rejoins (quorum stays), then a survivor is killed and restarted: it reaches
 *       {@code /ready} with an image equal to the leader's, and the restored service's {@code leadershipTermId} is
 *       recorded (the pre-existing hole N2: -1 until the next term event on the FULL path).</li>
 * </ol>
 * <p>The class carries the {@code Test} suffix the surefire default pattern needs (the contract names it
 * {@code TailAcrossTermKeepsTimerIds}; without the suffix the default build would never run it).</p>
 *
 * Expected RED until W2: only the path line ({@code restored path=TAIL}); the G28 assertions
 * ({@code leadershipTermId == stub.leadershipTermId}, the first armed sweep id) are W4.
 */
public class TailAcrossTermKeepsTimerIdsTest {

    private static final int PORT_BASE = 23000;
    private static final Pattern LOG_POSITION = Pattern.compile("logPosition=(\\d+)");

    @Test
    public void dryRunALeaderChangeThenASnapshotInTheNewTermAndOneRecordingId() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "rcs-s4-dry-tailterm")) {
            rig.bootAll();
            final Member leader0 = rig.awaitLeader(null);
            final Member s1 = rig.followerOtherThan(leader0, null);
            final Member s2 = rig.followerOtherThan(leader0, s1);
            System.out.println("[restore-dryrun] leader member " + leader0.id + " (term T), survivors " + s1.id + " and " + s2.id);
            Client admin = rig.connect(SessionClass.ADMIN, s2);
            rig.adopt(admin);
            Client tool = rig.connect(SessionClass.TOOL, s2);
            tool.deposits(300);
            rig.awaitImagesEqual(s1, leader0, 60_000);
            rig.awaitImagesEqual(s2, leader0, 60_000);
            final List<long[]> termsBefore = rig.terms(leader0);
            final long termT = termsBefore.get(termsBefore.size() - 1)[0];

            // the shadow in term T, on both survivors (either may be the one restarted later)
            final long pS;
            {
                final RecordedLog log1 = rig.captureCommitted(s1);
                final long[] pos = log1.positions();
                pS = pos[pos.length - 5];
                final Written w1 = rig.producer(s1).produce(new Who(ProducerKind.FOLLOWER, 101), log1, pS);
                assertTrue(w1.toString(), w1.ok());
                final Written w2 = rig.producer(s2).produce(new Who(ProducerKind.FOLLOWER, 102), rig.captureCommitted(s2), pS);
                assertTrue(w2.toString(), w2.ok());
                System.out.println("[restore-dryrun] P_s=" + pS + " in term " + termT + " on both survivors; payload " + w1.payloadLength() + " B");
            }

            // leader change
            rig.forget(tool);
            rig.forget(admin);
            rig.kill(leader0);
            final Member leader1 = rig.awaitLeader(leader0);
            final Member other = rig.followerOtherThan(leader1, leader0);
            // the clients ride the NEW LEADER's driver: it is never killed below (a killed client driver exits the JVM)
            tool = rig.connect(SessionClass.TOOL, leader1);
            tool.deposits(100);
            rig.tee.clear();
            final long pCm = rig.snapshotAll();
            tool.deposits(60);
            rig.awaitImagesEqual(other, leader1, 60_000);

            // 3 and 4: the snapshot belongs to T+1, and the tail range crosses the term event
            final List<long[]> terms = rig.terms(other);
            assertTrue("two term entries (T and T+1) on the survivor: " + dumpTerms(terms), terms.size() >= 2);
            final long[] last = terms.get(terms.size() - 1);
            final long[] first = terms.get(terms.size() - 2);
            assertOneRecording(terms, termT);
            assertEquals("the survivor's log recording is that recording", first[2], rig.logRecordingId(other));
            RestoreDrillDryRun.nc("dry-11.11-one-recording-oracle", "ONE recording id", () -> {
                final List<long[]> forged = new java.util.ArrayList<>(terms);
                final long[] t = forged.get(forged.size() - 1).clone();
                t[2] += 1; // a T+1 entry naming another recording (the multi-recording case of 6.4)
                forged.set(forged.size() - 1, t);
                assertOneRecording(forged, termT);
            });
            assertTrue("P_s < termBase(T+1) <= P_cm: " + pS + " < " + last[1] + " <= " + pCm, pS < last[1] && last[1] <= pCm);
            final long snapshotTerm = rig.snapshots(other).get(0)[2];
            assertEquals("the snapshot entry is in T+1", termT + 1, snapshotTerm);
            final RecordedLog log = rig.captureCommitted(other);
            LogFrame action = null;
            for (final LogFrame fr : log.frames()) {
                if (fr.kind() == FrameKind.CLUSTER_ACTION && fr.position() == pCm) {
                    action = fr;
                }
            }
            assertTrue("a ClusterAction frame ends at P_cm=" + pCm, action != null);
            assertEquals("the SNAPSHOT action frame is in T+1", termT + 1, action.leadershipTermId());
            boolean line = false;
            for (final String l : rig.tee.all("[SNAPSHOT] accounts=")) {
                final Matcher m = LOG_POSITION.matcher(l);
                if (m.find() && Long.parseLong(m.group(1)) == pCm) {
                    line = true;
                }
            }
            assertTrue("the [SNAPSHOT] line shows the T+1 action frame end " + pCm + ": " + rig.tee.lines, line);
            System.out.println("[restore-dryrun] T=" + termT + " T+1=" + last[0] + " termBase(T+1)=" + last[1] + " recording " + last[2]
                    + " (one id for both terms), P_cm=" + pCm + " in T+1");

            // 5: the old leader rejoins, then a survivor is restarted with quorum intact
            rig.restart(leader0);
            rig.awaitReady(leader0, 120_000);
            rig.awaitImagesEqual(leader0, leader1, 60_000);
            final Member r = other;
            rig.tee.clear();
            rig.kill(r);
            tool.deposits(30);
            rig.restart(r);
            final long readyMs = rig.awaitReady(r, 120_000);
            rig.awaitImagesEqual(r, leader1, 60_000);
            assertTrue(r.halts.toString(), r.halts.isEmpty());
            System.out.println("[restore-dryrun] restarted survivor " + r.id + " readyMs=" + readyMs + " leadershipTermId after the FULL-path restore="
                    + ServiceProbe.leadershipTermId(r.service) + " (T+1=" + (termT + 1) + "; -1 is the pre-existing hole N2); image "
                    + ServiceProbe.image(r.service).length + " B; lines " + rig.tee.lines);
            assertTrue("ready within the budget, took " + readyMs, readyMs <= RestoreDrillDryRun.READY_BUDGET_MS);
            RestoreDrillDryRun.pathLine(rig.tee, "TAIL", "11.11");
        }
    }

    // ------------------------------------------------------------------------------------------ W3: the production rows

    /** What the G28 story leaves for its assertions. */
    private record Story(ShadowClusterRig rig, Member leader1, Member r, Client tool, long termT, long pS, long pCm) { }

    /**
     * The dry run's story in stub mode: a shadow at P_s in term T on both survivors, a leader change, a stub snapshot at
     * P_cm in T+1, the old leader back, then survivor r killed and restarted with {@code faults}: TAIL (P_s, P_cm]
     * crosses the NewLeadershipTermEvent of T+1.
     */
    private static Story story(final ShadowClusterRig rig, final String... faults) throws Exception {
        for (final Member m : rig.members) {
            m.settings = StubStory.shadow();
        }
        rig.bootAll();
        final Member leader0 = rig.awaitLeader(null);
        final Member s1 = rig.followerOtherThan(leader0, null);
        final Member s2 = rig.followerOtherThan(leader0, s1);
        Client admin = rig.connect(SessionClass.ADMIN, s2);
        rig.adopt(admin);
        Client tool = rig.connect(SessionClass.TOOL, s2);
        tool.deposits(300);
        rig.awaitImagesEqual(s1, leader0, 60_000);
        rig.awaitImagesEqual(s2, leader0, 60_000);
        final List<long[]> termsBefore = rig.terms(leader0);
        final long termT = termsBefore.get(termsBefore.size() - 1)[0];
        final RecordedLog log1 = rig.captureCommitted(s1);
        final long[] pos = log1.positions();
        final long pS = pos[pos.length - 5];
        assertTrue(rig.producer(s1).produce(new Who(ProducerKind.FOLLOWER, 401), log1, pS).ok());
        assertTrue(rig.producer(s2).produce(new Who(ProducerKind.FOLLOWER, 402), rig.captureCommitted(s2), pS).ok());
        rig.forget(tool);
        rig.forget(admin);
        rig.kill(leader0);
        final Member leader1 = rig.awaitLeader(leader0);
        final Member r = rig.followerOtherThan(leader1, leader0);
        tool = rig.connect(SessionClass.TOOL, leader1);
        tool.deposits(100);
        final long pCm = rig.snapshotAll();
        tool.deposits(60);
        rig.awaitImagesEqual(r, leader1, 60_000);
        final List<long[]> terms = rig.terms(r);
        assertOneRecording(terms, termT);
        final long[] last = terms.get(terms.size() - 1);
        assertTrue("P_s < termBase(T+1) <= P_cm: " + pS + " < " + last[1] + " <= " + pCm, pS < last[1] && last[1] <= pCm);
        System.out.println("[11.11] precondition held: " + dumpTerms(terms) + " (one recording for T=" + termT + " and T+1),"
                + " P_s=" + pS + " P_cm=" + pCm);
        rig.restart(leader0);
        rig.awaitReady(leader0, 120_000);
        rig.awaitImagesEqual(leader0, leader1, 60_000);
        rig.kill(r);
        tool.deposits(30);
        r.faults = faults;
        rig.tee.clear();
        rig.errTee.clear();
        rig.restart(r);
        return new Story(rig, leader1, r, tool, termT, pS, pCm);
    }

    @Test
    public void aTailAcrossTheTermEventEndsAtTheStubsTerm() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(23100, "rcs-s4-11.11-g28")) {
            final Story st = story(rig);
            rig.awaitReady(st.r(), 120_000);
            rig.awaitImagesEqual(st.r(), st.leader1(), 60_000);
            assertTrue(st.r().halts.toString(), st.r().halts.isEmpty());
            RestoreDrillDryRun.requirePathLine(rig.tee, "TAIL P_s=" + st.pS() + " P_cm=" + st.pCm(), "11.11");
            final String stubLine = rig.tee.all("[SNAPSHOT] stub P_cm=" + st.pCm()).toString();
            assertTrue("the stub carries T+1: " + stubLine, stubLine.contains(" term=" + (st.termT() + 1) + " "));
            final String tailLine = rig.tee.all("[shadow restore] tail (" + st.pS() + "," + st.pCm() + "]").toString();
            assertTrue("the tail crossed the term event: " + tailLine, tailLine.contains("term -1 -> " + (st.termT() + 1))
                    || tailLine.contains("term " + st.termT() + " -> " + (st.termT() + 1)));
            final long term = ServiceProbe.leadershipTermId(st.r().service);
            System.out.println("GREEN 11.11: " + tailLine + "; service leadershipTermId after onStart=" + term
                    + " (stub " + stubLine + ")");
            assertEquals("leadershipTermId == stub.leadershipTermId == T+1 (G28)", st.termT() + 1, term);

            // the multi-recording control (review F2) on a forged COPY of r's RecordingLog
            final java.io.File forged = new java.io.File(rig.tmp, "forged-recording-log");
            org.agrona.IoUtil.delete(forged, true);
            assertTrue(forged.mkdirs());
            final long realId = StubStory.forgeLastTermRecordingId(st.r().clusterDir(), forged);
            String refused = null;
            try {
                ServiceProbe.singleRecording(forged, st.pS(), st.pCm());
            } catch (final RuntimeException e) {
                refused = e.getMessage();
            }
            System.out.println("GREEN 11.11 forged T+1 entry naming recording " + (realId + 1) + " -> " + refused);
            assertEquals("Aeron refuses the forged RecordingLog at load", "ERROR - invalid TERM recordingId=" + (realId + 1)
                    + ", expected recordingId=" + realId, refused);
            assertEquals("the real RecordingLog satisfies the precondition", null,
                    ServiceProbe.singleRecording(st.r().clusterDir(), st.pS(), st.pCm()));
        }
    }

    @Test
    public void controlTheAdapterTermUpdateOffHaltsAtTheEndOfTheTail() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(23200, "rcs-s4-11.11-noterm")) {
            final Story st = story(rig, "tailTermUpdateOff");
            rig.await(() -> !st.r().halts.isEmpty(), 60_000, () -> "the G28 halt; stderr " + rig.errTee.lines);
            RestoreDrillDryRun.nc("11.11-term-update-off", "tail ended at term", () -> {
                final String l = StubStory.line(rig.errTee.lines, "FATAL shadow restore: tail ended at term");
                if (l != null) {
                    throw new AssertionError(l.substring(l.indexOf("tail ended")));
                }
            });
            final String l = StubStory.line(rig.errTee.lines, "FATAL shadow restore: tail ended at term");
            assertTrue(l, l.contains(", stub says " + (st.termT() + 1)));
        }
    }

    /** The last two term entries are T and T+1 and name ONE recording id (the extendRecording branch, CMA:2366-2368). */
    private static void assertOneRecording(final List<long[]> terms, final long termT) {
        assertTrue("two term entries (T and T+1) on the survivor: " + dumpTerms(terms), terms.size() >= 2);
        final long[] last = terms.get(terms.size() - 1);
        final long[] first = terms.get(terms.size() - 2);
        assertEquals("term T is followed by T+1", termT + 1, last[0]);
        assertEquals("the extendRecording branch: ONE recording id for T and T+1 (CMA:2366-2368): " + dumpTerms(terms),
                first[2], last[2]);
    }

    private static String dumpTerms(final List<long[]> terms) {
        final StringBuilder sb = new StringBuilder();
        for (final long[] t : terms) {
            sb.append("{term=").append(t[0]).append(" base=").append(t[1]).append(" rec=").append(t[2]).append("} ");
        }
        return sb.toString();
    }
}
