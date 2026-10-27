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
 * RC2 S7 contract 3.8 (belt review F-3): the duplicate-belt digest is behaviour-3 replicated state (section 10 stores it,
 * and it decides REFUSE vs DUPLICATE), so its definition is frozen by these vectors: one fixed entry per template and one
 * fixed section-10 image. A change here is a visible failure; once behaviour 3 is in the production set the digest may
 * change only under a new behaviour version (the stored window carried as is).
 */
public class DigestVectorTest {

    static final long[] VECTORS = {
        AssetsEngine.tradeDigest(1_001, 7, 42, 1, 101, 2, 202, 3, 100_00000000L, 25_000000L, true, 1_700_000_000_123L, 11, 12, 0x5EED),
        AssetsEngine.terminalDigest(1_002, 8, 101, 2, 1, 3, 17, 1_700_000_000_124L, 11, 0x5EED),
        AssetsEngine.outcomeDigest(1_003, 9, 11, 6, 2, 11, 101, 100_00000000L, 50_000000L, 0, 1, 2, 1, 0, 21, true,
                1_700_000_000_125L, 0x5EED),
        AssetsEngine.releaseDigest(1_004, 10, 0x5EED),
        AssetsEngine.configDigest(1_005, 1, 0x5EED, 1, 2, 4096, 12, ConfigDecision.FRESH),
    };

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
        final long[] pinned = {0xbc05d65633b98254L, 0x1f40ada882cb334eL, 0x4528d93c074f1fc4L, 0x1a7ef7e195eb9f4L,
            0x4f666ef4f9e72c64L};
        final String[] names = {"61/62 trade", "63 terminal", "64 outcome", "67 release", "68 engine config"};
        for (int i = 0; i < pinned.length; i++) {
            assertEquals(names[i] + " digest changed: a behaviour-3 state change (contract 3.8)", pinned[i], VECTORS[i]);
        }
    }

    @Test
    public void theSection10ImageIsPinned() throws Exception {
        assertEquals("section 10 bytes (layout v2 with the digest window) changed (contract 3.8, 14)",
                "a0269ed104dcd289977297c9de6dd246f72377ed74d57c495703a56554faa24c", section10Sha256());
    }
}
