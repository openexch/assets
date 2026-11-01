// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.determinism.AssetsScenarioRunner;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.assets.domain.commands.DepositCommand;
import com.openexchange.assets.domain.commands.SetMoneyJournalCommand;
import com.openexchange.assets.infrastructure.persistence.AeShadowRig.Arm;
import com.openexchange.assets.infrastructure.persistence.AeShadowRig.Log;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** RCS S3 11.12: {@code AssetsShadowIdentity} over the engine's recorded genesis, journal arming and journal seq. */
public class AssetsShadowIdentityTest {

    static int shadowRecordings(final RecordedLogFixture f) {
        final int[] n = {0};
        f.archive().listRecordingsForUri(0, 1000, com.openexchange.cluster.shadow.ShadowAlias.prefix(1),
                com.openexchange.cluster.shadow.ShadowAlias.STREAM_ID,
                (a, b, c, d, e, g, h, i, j, k, l, m, nn, o, p, q) -> n[0]++);
        return n[0];
    }

    static void assertNoShadow(final RecordedLogFixture f, final String failure) {
        final int n = shadowRecordings(f);
        if (n != 0) {
            throw new AssertionError(failure + " (" + n + " shadow recording(s))");
        }
    }

    static ShadowWriter.WriteResult tryWrite(final RecordedLogFixture f, final Arm arm, final long p) {
        final ShadowImageBuffer image = AeShadowRig.image(arm, 4 << 20);
        return AeShadowRig.writer(f, new ShadowCounters()).write(image, AeShadowRig.ENGINE, ShadowKind.LOCAL,
                new LogCursor(p, 1_000, 0, 0), AeShadowRig.identityOf(arm.applier));
    }

    @Test
    public void genesisFollowsTheAdoptFrameAndSurvivesASnapshot() {
        try (RecordedLogFixture f = new RecordedLogFixture()) {
            final Log log = AeShadowRig.write(f, AeShadowRig.story(0x5EED_0004L, 200), -1, 0, 0);
            final Arm arm = new Arm().genesis();
            final ShadowIdentitySource id = AeShadowRig.identityOf(arm.applier);
            assertEquals("fresh engine", 0L, id.genesisId());
            assertEquals(4, id.payloadLayoutHint());
            final List<LogFrame> frames = log.frames().frames();
            long previous = -1;
            boolean refusedBeforeAdopt = false;
            int adoptedAt = -1;
            for (int i = 0; i < frames.size(); i++) {
                arm.apply(log, previous, frames.get(i).position());
                previous = frames.get(i).position();
                if (id.genesisId() != 0 && adoptedAt < 0) {
                    adoptedAt = i;
                    assertEquals(AeShadowRig.GENESIS, id.genesisId());
                    assertEquals("one CoreConfig ADOPTed", 1, arm.applier.engine().coreConfig().adoptedCount());
                }
                if (adoptedAt < 0) {
                    assertEquals("0 before the ADOPT frame", 0L, id.genesisId());
                    assertNoShadow(f, "a shadow exists before CoreConfig ADOPT");
                    if (!refusedBeforeAdopt) {
                        // the writer refuses an engine that has not ADOPTed yet (W1/H10)
                        final ShadowWriter.WriteResult r = tryWrite(f, arm, previous);
                        assertEquals(ShadowWriter.Reason.UNIDENTIFIED, r.reason());
                        assertNoShadow(f, "shadow written before CoreConfig ADOPT");
                        refusedBeforeAdopt = true;
                    }
                }
            }
            assertTrue("the story ADOPTs", adoptedAt > 0);
            assertTrue("the pre-ADOPT write refusal ran (review NOTE-10)", refusedBeforeAdopt);
            final byte[] image = arm.image();
            final AssetsLogApplier back = new AssetsLogApplier(() -> { });
            back.restore(new org.agrona.concurrent.UnsafeBuffer(image), image.length);
            assertEquals("unchanged across a snapshot round trip", AeShadowRig.GENESIS,
                    AeShadowRig.identityOf(back).genesisId());
            // positive control (C6): the same helper sees a shadow once the engine is identified
            final ShadowWriter.WriteResult ok = tryWrite(f, arm, previous);
            assertTrue(ok.toString(), ok.ok());
            AeShadowRig.nc("NC-9 (assets leg, detection) a written shadow is seen", "shadow written before CoreConfig ADOPT",
                    () -> assertNoShadow(f, "shadow written before CoreConfig ADOPT"));
        }
    }

    @Test
    public void aNegativeNonceIsRefusedAtAdoptAndARestoredOneStopsTheProducer() {
        try (RecordedLogFixture f = new RecordedLogFixture()) {
            final Arm arm = new Arm().genesis();
            final AssetsEngine engine = arm.applier.engine();
            final CoreConfigCommand negative = AssetsScenarioRunner.coreConfigFor(1, 1, 1L, 1L,
                    1_000_000_000_000_000L, 0L, 1);
            negative.genesisId = -42L;
            engine.applyCoreConfig(negative, 1L);
            final ShadowIdentitySource id = AeShadowRig.identityOf(arm.applier);
            assertEquals("A7: ADOPT of -42 refused, nothing recorded", 0L, id.genesisId());
            assertEquals(1, engine.coreConfig().rejectCount());
            // an image recorded before A7 can still carry -42: the writer refuses it and the boot guard stops the host
            engine.coreConfig().restore(negative);
            assertEquals(-42L, id.genesisId());
            final ShadowWriter.WriteResult r = tryWrite(f, arm, 4096);
            assertEquals(ShadowWriter.Reason.UNIDENTIFIED, r.reason());
            assertNoShadow(f, "shadow written with a negative genesisId");
            try {
                ShadowIdentity.requireProducible(id.genesisId());
                fail("requireProducible(-42) passed");
            } catch (final IllegalStateException e) {
                assertEquals("shadow producer refused: recorded genesisId <= 0 (-42); see RCS S3 5.5", e.getMessage());
            }
        }
    }

    @Test
    public void journalArmingAndSeqAreTheReplicatedOnes() {
        final Arm arm = new Arm().genesis();
        final AssetsEngine engine = arm.applier.engine();
        final ShadowIdentitySource id = AeShadowRig.identityOf(arm.applier);
        assertFalse(id.journalEnabled());
        assertEquals(0L, id.journalSeq());
        final SetMoneyJournalCommand on = new SetMoneyJournalCommand();
        on.reset();
        on.setEnabled(true);
        engine.applyCommand(AssetsEngine.CMD_SET_MONEY_JOURNAL, on, 1L);
        assertTrue(id.journalEnabled());
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(1);
        d.setAssetId(0);
        d.setAmount(1_000L);
        engine.applyCommand(AssetsEngine.CMD_DEPOSIT, d, 2L);
        assertTrue("a journaled deposit assigns a seq: " + id.journalSeq(), id.journalSeq() >= 1);
        assertEquals(engine.getJournalSeq(), id.journalSeq());
        final ExpandableArrayBuffer b = new ExpandableArrayBuffer(1 << 16);
        final int n = arm.applier.serialize(b);
        final AssetsLogApplier back = new AssetsLogApplier(() -> { });
        back.restore(b, n);
        assertTrue(AeShadowRig.identityOf(back).journalEnabled());
        assertEquals(id.journalSeq(), AeShadowRig.identityOf(back).journalSeq());
    }
}
