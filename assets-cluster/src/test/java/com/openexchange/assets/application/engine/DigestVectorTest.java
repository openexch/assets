// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine;

import com.openexchange.assets.application.engine.feed.V3Rig;
import com.openexchange.assets.domain.ConfigDecision;
import com.openexchange.assets.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.Assert.assertEquals;

/**
 * RC2 S8 3.10 item 2: the 64 and 67 vectors and the section-10 image are regenerated for the widened digests (S7 pinned
 * 0x4528d93c074f1fc4 for 64, 0x1a7ef7e195eb9f4 for 67 and a0269ed1... for section 10).
 *
 * <p>RC2 S7 contract 3.8 (belt review F-3): the duplicate-belt digest is behaviour-3 replicated state (section 10 stores it,
 * and it decides REFUSE vs DUPLICATE), so its definition is frozen by these vectors: one fixed entry per template and one
 * fixed section-10 image. A change here is a visible failure; once behaviour 3 is in the production set the digest may
 * change only under a new behaviour version (the stored window carried as is).
 */
public class DigestVectorTest {

    static final long[] VECTORS = {
        AssetsEngine.tradeDigest(1_001, 7, 42, 1, 101, 2, 202, 3, 100_00000000L, 25_000000L, true, 1_700_000_000_123L, 11, 12, 0x5EED),
        AssetsEngine.terminalDigest(1_002, 8, 101, 2, 1, 3, 17, 1_700_000_000_124L, 11, 0x5EED),
        AssetsEngine.outcomeDigest(outcome()),
        AssetsEngine.releaseDigest(release()),
        AssetsEngine.configDigest(1_005, 1, 0x5EED, 1, 2, 4096, 12, ConfigDecision.FRESH),
    };

    /** RC2 S8 3.10 item 2: the S7 fields of the 64 vector, then the widened tail. */
    static com.openexchange.assets.application.engine.order.MeOutcomeView outcome() {
        final var v = new com.openexchange.assets.application.engine.order.MeOutcomeView();
        v.egressSeq = 1_003; v.journalSeq = 9; v.commandIdHigh = 11; v.commandIdLow = 6; v.userId = 2; v.omsOrderId = 11;
        v.oldOrderId = 101; v.price = 100_00000000L; v.quantity = 50_000000L; v.budget = 0; v.marketId = 1; v.commandKind = 2;
        v.result = 1; v.orderId = 0; v.reason = 21; v.oldCancelled = true; v.meTimestamp = 1_700_000_000_125L; v.meEpoch = 0x5EED;
        v.orderType = 0; v.orderSide = 1; v.appliedPosition = 1_003; v.status = 4; v.timeInForce = 0;
        v.expectedOldRemaining = 75_000000L; v.ledgerSize = 3; v.ledgerCapacity = 4096; v.behaviorVersion = 2;
        return v;
    }

    /** RC2 S8 3.10 item 2: the 67 vector with two entries in group order. */
    static com.openexchange.assets.application.engine.order.MeReleaseView release() {
        final var v = new com.openexchange.assets.application.engine.order.MeReleaseView();
        v.egressSeq = 1_004; v.journalSeq = 10; v.meEpoch = 0x5EED; v.releaseSeq = 7; v.ledgerSize = 2; v.ledgerCapacity = 4096;
        v.tombstoneRanges = 1;
        v.add(11, 7, 1, 0, 2);
        v.add(12, 4, 0, 1, 0);
        return v;
    }

    static String section10Sha256() throws Exception {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        r.sellHits(1, V3Rig.ONE / 4);
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final int header = 8 + 7 * 8 + 1 + 4;
        int pos = header;
        for (int i = 0; i < buf.getInt(header - 4); i++) {
            final int id = buf.getShort(pos);
            final int sectionLength = buf.getInt(pos + 2);
            if (id == BalanceSnapshotCodec.SECTION_FEED_STATE) {
                final byte[] body = new byte[sectionLength];
                buf.getBytes(pos + 6, body);
                return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
            }
            pos += 6 + sectionLength;
        }
        throw new AssertionError("no section 10 in a " + len + "-byte v3 image");
    }

    @Test
    public void theEntryDigestIsPinned() {
        // RC2 S8 3.10 item 2: the 64 and 67 vectors are the widened digests (61/62, 63 and 68 unchanged from S7)
        final long[] pinned = {0xbc05d65633b98254L, 0x1f40ada882cb334eL, 0x2082e936a5c78687L, 0x59db45e54210c93aL,
            0x4f666ef4f9e72c64L};
        final String[] names = {"61/62 trade", "63 terminal", "64 outcome", "67 release", "68 engine config"};
        final StringBuilder all = new StringBuilder();
        for (final long v : VECTORS) {
            all.append(String.format("0x%xL ", v));
        }
        for (int i = 0; i < pinned.length; i++) {
            assertEquals(names[i] + " digest changed: a behaviour-3 state change (contract 3.8); computed " + all, pinned[i],
                    VECTORS[i]);
        }
    }

    @Test
    public void theSection10ImageIsPinned() throws Exception {
        // RC2 S8 3.10 item 2: regenerated with the widened 64 digest (the fixture's two acks are 64s); S7 pinned a0269ed1...
        assertEquals("section 10 bytes (layout v2 with the digest window) changed (contract 3.8, 14)",
                "7515e9f7f21898e1436b4ba478f9f2ceaee3380c87a21240109fef91a40c3c62", section10Sha256());
    }
}
