// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.journal.MoneyJournalTail;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowStub;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 W2 unit drivers for the pure rules the restore protocol is built from: the payload-tag dispatch after the
 * envelope (3.3), the stub consistency checks (3.3), the single-recording precondition (6.4, review F2), the virtual
 * genesis precondition (7.1) and the skip-safe rule (8.1). The cluster-level paths are the drills of 11.2-11.14.
 */
public class StubDispatchAndRulesTest {

    private static AssetsClusteredService serviceWithRecorder(final int[] stops) {
        final AssetsClusteredService service = new AssetsClusteredService(RcsSettings.defaults());
        service.failFast = () -> stops[0]++;
        return service;
    }

    private static int stubImage(final ExpandableArrayBuffer buffer, final long pCm) {
        return new ShadowStub(AssetsLogApplier.ENGINE_ID_ASSETS, 0, 0x5EEDL, 3, pCm, -1, -1)
                .encodeEnveloped(new SnapshotEnvelope(), buffer);
    }

    @Test
    public void aStubImageIsDispatchedToTheStubProtocolNotTheCodec() {
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = stubImage(image, 4096);
        assertEquals(ShadowStub.ENVELOPED_LENGTH, length);
        final int[] stops = new int[1];
        final AssetsClusteredService service = serviceWithRecorder(stops);
        // no cluster outside onStart: the stub protocol refuses loudly, the v5 codec never sees tag -100
        final ShadowRestore.Halt halt = assertThrows(ShadowRestore.Halt.class,
                () -> service.restoreFromImage(image, length));
        assertTrue(halt.getMessage(), halt.getMessage().startsWith("FATAL RCS: a stub image needs the cluster context"));
        assertEquals(1, stops[0]);
        assertEquals("the engine is untouched", 0, service.engine().accountCount());
    }

    @Test
    public void anUnknownLayoutTagIsRefusedWithTheTag() {
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        image.putLong(SnapshotEnvelope.payloadOffset(), -4L);
        image.putLong(SnapshotEnvelope.payloadOffset() + 8, 0L);
        final int length = new SnapshotEnvelope().seal(image, 16);
        final int[] stops = new int[1];
        final AssetsClusteredService service = serviceWithRecorder(stops);
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> service.restoreFromImage(image, length));
        assertTrue(e.getMessage(), e.getMessage().contains("layout tag -4 refused")
                && e.getMessage().contains("RCS stub (-100)"));
        assertEquals(1, stops[0]);
    }

    @Test
    public void stubConsistencyChecks() {
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = stubImage(image, 4096);
        final int payloadLength = new SnapshotEnvelope().open(image, length);
        final int off = SnapshotEnvelope.payloadOffset();
        assertEquals(4096, ShadowRestore.decodeStub(image, off, payloadLength, 0, 4096).logPosition());
        assertEquals("FATAL RCS: stub P_cm 4096 != recovery position 4160", assertThrows(ShadowRestore.Halt.class,
                () -> ShadowRestore.decodeStub(image, off, payloadLength, 0, 4160)).getMessage());
        final String cluster = assertThrows(ShadowRestore.Halt.class,
                () -> ShadowRestore.decodeStub(image, off, payloadLength, 7, 4096)).getMessage();
        assertTrue("both cluster ids named: " + cluster, cluster.contains("clusterId 0, host 7"));
    }

    @Test
    public void singleRecordingPrecondition() {
        final List<ShadowRestore.Term> one = List.of(
                new ShadowRestore.Term(0, 0, 5, 1000),
                new ShadowRestore.Term(1, 1000, 5, Long.MAX_VALUE));
        assertNull(ShadowRestore.singleRecording(one, 500, 2000));
        assertNull(ShadowRestore.virtualGenesisPrecondition(one, 0, 2000));
        final List<ShadowRestore.Term> two = List.of(
                new ShadowRestore.Term(0, 0, 5, 1000),
                new ShadowRestore.Term(1, 1000, 6, Long.MAX_VALUE));
        assertEquals("tail range (500,2000] spans recordings 5,6: unsupported",
                ShadowRestore.singleRecording(two, 500, 2000));
        assertNull("a tail inside the second term only needs its recording", ShadowRestore.singleRecording(two, 1000,
                2000));
        assertNull("frames ending at the base belong to the earlier term", ShadowRestore.singleRecording(two, 0, 1000));
        assertEquals("the tail replays the recording the precondition named", 5, ShadowRestore.rangeRecording(two, 0,
                1000));
        assertEquals(6, ShadowRestore.rangeRecording(two, 1000, 2000));
        assertEquals(-1, ShadowRestore.rangeRecording(two, 500, 2000));
        assertEquals("log recording starts at 64", ShadowRestore.virtualGenesisPrecondition(one, 64, 2000));
        assertEquals("tail range (0,2000] spans recordings 5,6: unsupported",
                ShadowRestore.virtualGenesisPrecondition(two, 0, 2000));
        assertEquals("oldest term entry starts at 1000", ShadowRestore.virtualGenesisPrecondition(
                List.of(new ShadowRestore.Term(1, 1000, 5, Long.MAX_VALUE)), 0, 2000));
    }

    @Test
    public void skipSafeRule() {
        final ShadowHeader off = header(0, 9);
        final ShadowHeader on = header(1, 9);
        assertTrue("journaling off: always skip-safe", ShadowRestore.skipSafe(off, new MoneyJournalTail.NoJournal(0)));
        assertTrue(ShadowRestore.skipSafe(on, new MoneyJournalTail.Known(9, 1, 0)));
        assertTrue(ShadowRestore.skipSafe(on, new MoneyJournalTail.Known(10, 2, 0)));
        assertEquals(false, ShadowRestore.skipSafe(on, new MoneyJournalTail.Known(8, 1, 0)));
        assertEquals(false, ShadowRestore.skipSafe(on, new MoneyJournalTail.NoJournal(0)));
        assertEquals(false, ShadowRestore.skipSafe(on, new MoneyJournalTail.Unknown("archive down")));
        assertEquals(false, ShadowRestore.skipSafe(on, null));
    }

    private static ShadowHeader header(final int journalEnabled, final long journalSeq) {
        return new ShadowHeader(1, 0, 1, 1, 1, 1, 0x5EEDL, 4096, 0, 0, 1, journalEnabled, journalSeq, 64, 0, 64, 5);
    }
}
