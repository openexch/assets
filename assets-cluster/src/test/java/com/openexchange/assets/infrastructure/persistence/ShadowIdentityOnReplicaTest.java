// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.persistence.AeShadowRig.Arm;
import com.openexchange.assets.infrastructure.persistence.AeShadowRig.Log;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowIdentity.LocalIdentity;
import com.openexchange.cluster.shadow.ShadowIdentity.Outcome;
import com.openexchange.cluster.shadow.ShadowIdentity.Policy;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowImageLoader;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RCS S3 11.11 (RF-f): a shadow written by a replica, whose history begins at term 3 (no term 0 anywhere), passes a
 * member's identity check, because identity is the replicated genesisId plus the term base the checking host knows for
 * the shadow's term (WK-1), not a {@code RecordingLog} term-0 timestamp (the M 2.1 rule, which a replica cannot
 * answer and which would refuse every replica shadow).
 */
public class ShadowIdentityOnReplicaTest {

    static final long X = 1L << 20; // the replica's history starts at term 3's base
    static final long TERM = 3;

    private static RecordedLogFixture fixture;
    private static Log log;
    private static ShadowEntry entry;
    private static ShadowImageLoader.LoadedShadow loaded;

    @BeforeClass
    public static void writeAReplicaShadow() {
        fixture = new RecordedLogFixture();
        log = AeShadowRig.write(fixture, AeShadowRig.story(0x5EED_0003L, 300), -1, TERM, X);
        assertEquals("the replica saw term 3 only", Map.of(TERM, X), log.terms());
        final Arm replica = new Arm().genesis().apply(log, -1, Long.MAX_VALUE);
        final LogCursor at = replica.container.cursor();
        assertEquals(TERM, at.leadershipTermId());
        assertEquals(X, at.termBaseLogPosition());
        final ShadowImageBuffer image = AeShadowRig.image(replica, 4 << 20);
        final ShadowWriter.WriteResult r = AeShadowRig.writer(fixture, new ShadowCounters()).write(image,
                AeShadowRig.ENGINE, ShadowKind.STANDBY, at, AeShadowRig.identityOf(replica.applier));
        assertTrue(r.toString(), r.ok());
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final ShadowCatalog catalog = AeShadowRig.catalog(fixture.aeron(), fixture.archive(), verdicts,
                AeShadowRig.GENESIS);
        entry = catalog.verifyAllComplete().entry(r.recordingId());
        assertEquals(ShadowEntry.State.VERIFIED, entry.state());
        final ShadowImageLoader.LoadResult l = new ShadowImageLoader(fixture.aeron(), fixture.archive(), verdicts,
                SystemNanoClock.INSTANCE, AeShadowRig.TIMEOUT_NS, new ShadowCounters())
                .load(entry, new UnsafeBuffer(ByteBuffer.allocateDirect(4 << 20)));
        assertTrue(l.ok());
        loaded = l.loaded();
    }

    @AfterClass
    public static void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    /** A member that took part in terms 0..3 knows every term base from its RecordingLog term entries. */
    static Map<Long, Long> memberTerms() {
        final Map<Long, Long> terms = new HashMap<>();
        terms.put(0L, 0L);
        terms.put(1L, 4096L);
        terms.put(2L, 65_536L);
        terms.put(TERM, X);
        return terms;
    }

    /** The M 2.1 rule (M:214, M:281-283): identity = the RecordingLog's term-0 timestamp; null = unknown here. */
    static Long oldRuleGenesisTs(final Map<Long, Long> termTimestamps) {
        return termTimestamps.get(0L);
    }

    @Test
    public void aMemberAcceptsTheReplicaShadowTheOldRuleCouldNot() {
        final ShadowHeader h = entry.header();
        assertEquals(TERM, h.leadershipTermId());
        assertEquals(X, h.termBaseLogPosition());
        assertEquals(Outcome.OK, ShadowIdentity.check(h, AeShadowRig.member(AeShadowRig.GENESIS, memberTerms())));
        // the historical rule, evaluated where a replica stands: its backup RecordingLog has no term 0
        final Map<Long, Long> replicaRecordingLog = Map.of(TERM, 1_700_000_000_000L);
        assertNull("the old rule cannot answer on a replica (no term 0)", oldRuleGenesisTs(replicaRecordingLog));
        System.out.println("RED-on-the-old-rule RF-f: genesisTs from RecordingLog term 0 is unknown on a replica-shaped"
                + " history, so the M 2.1 rule refuses every replica shadow; the adopted rule returns "
                + ShadowIdentity.check(h, AeShadowRig.member(AeShadowRig.GENESIS, memberTerms())));
    }

    static ShadowHeader withTermBase(final ShadowHeader g, final long termBase) {
        return new ShadowHeader(g.engineId(), g.clusterId(), g.kind(), g.producerKind(), g.producerId(),
                g.producerSeq(), g.genesisId(), g.logPosition(), g.leadershipTermId(), termBase, g.clusterTime(),
                g.journalEnabled(), g.journalSeq(), g.payloadLength(), g.payloadDigest(), g.chunkLength(),
                g.payloadLayoutHint());
    }

    static void assertHistoryMismatch(final Outcome o) {
        if (o != Outcome.HISTORY_MISMATCH) {
            throw new AssertionError("HISTORY_MISMATCH expected, got " + o);
        }
    }

    @Test
    public void theTermBaseIsPartOfTheIdentity() {
        final ShadowHeader shifted = withTermBase(entry.header(), X + 32);
        final LocalIdentity member = AeShadowRig.member(AeShadowRig.GENESIS, memberTerms());
        assertHistoryMismatch(ShadowIdentity.check(shifted, member));
        // NC-8: a check that ignores termBaseOf (I6/I7 switched off: the lookup echoes the header)
        AeShadowRig.nc("NC-8 termBaseOf ignored", "HISTORY_MISMATCH expected, got OK", () -> assertHistoryMismatch(
                ShadowIdentity.check(shifted, new LocalIdentity(AeShadowRig.ENGINE, 0, AeShadowRig.GENESIS,
                        t -> shifted.termBaseLogPosition()))));
        // a lookup that does not know term 3
        final LocalIdentity noTerm3 = new LocalIdentity(AeShadowRig.ENGINE, 0, AeShadowRig.GENESIS,
                t -> t == TERM ? -1L : memberTerms().getOrDefault(t, -1L));
        assertEquals(Outcome.HISTORY_UNVERIFIED, ShadowIdentity.check(entry.header(), noTerm3));
        assertFalse(ShadowIdentity.admits(entry.header(), noTerm3, Policy.STRICT));
        assertTrue(ShadowIdentity.admits(entry.header(), noTerm3, Policy.ALLOW_UNVERIFIED_HISTORY));
    }

    @Test
    public void bootstrapAdoptIsGuardedByThePostRestoreCrossCheck() {
        final LocalIdentity bootstrap = new LocalIdentity(AeShadowRig.ENGINE, 0, 0, t -> -1L);
        final ShadowHeader genuine = loaded.header();
        assertEquals(Outcome.GENESIS_UNVERIFIED, ShadowIdentity.check(genuine, bootstrap));
        assertFalse(ShadowIdentity.admits(genuine, bootstrap, Policy.STRICT));
        assertFalse(ShadowIdentity.admits(genuine, bootstrap, Policy.ALLOW_UNVERIFIED_HISTORY));
        assertTrue(ShadowIdentity.admits(genuine, bootstrap, Policy.BOOTSTRAP_ADOPT));
        final Arm restored = new Arm().restore(loaded.payload(), loaded.length(), genuine.logPosition(),
                loaded.cursor());
        assertTrue("genuine: the restored engine's identity equals the header",
                ShadowIdentity.matches(genuine, AeShadowRig.identityOf(restored.applier)));
        // a forged header genesisId passes I5 under BOOTSTRAP_ADOPT and is caught only after the restore
        final ShadowHeader forged = new ShadowHeader(genuine.engineId(), genuine.clusterId(), genuine.kind(),
                genuine.producerKind(), genuine.producerId(), genuine.producerSeq(), AeShadowRig.GENESIS + 1,
                genuine.logPosition(), genuine.leadershipTermId(), genuine.termBaseLogPosition(),
                genuine.clusterTime(), genuine.journalEnabled(), genuine.journalSeq(), genuine.payloadLength(),
                genuine.payloadDigest(), genuine.chunkLength(), genuine.payloadLayoutHint());
        assertTrue(ShadowIdentity.admits(forged, bootstrap, Policy.BOOTSTRAP_ADOPT));
        final Arm second = new Arm().restore(loaded.payload(), loaded.length(), forged.logPosition(), loaded.cursor());
        assertFalse("forged: the cross-check refuses it", ShadowIdentity.matches(forged,
                AeShadowRig.identityOf(second.applier)));
        assertTrue(ShadowIdentity.mismatch(forged, AeShadowRig.identityOf(second.applier)).startsWith("genesisId"));
    }
}
