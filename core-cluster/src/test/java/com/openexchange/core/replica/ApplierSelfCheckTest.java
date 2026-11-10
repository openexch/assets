// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 8.5, the production copy of {@code CoreShadowRig.ApplierSelfCheck}: a real image round-trips through a
 * scratch applier; a truncated or garbage payload is {@code false} (never a throw out of {@code verify}); the self-check
 * never writes into the payload it checks. The cluster-level rows (mutated image, scratch writing into the buffer) are
 * {@code ReplicaSelfCheckTest}'s.
 */
public class ApplierSelfCheckTest {

    private static byte[] genesisImage() {
        final CoreLogApplier live = new CoreLogApplier(() -> {
            throw new IllegalStateException("test applier cross-check");
        });
        live.genesis();
        final ExpandableArrayBuffer out = new ExpandableArrayBuffer(4096);
        final int n = live.serialize(out);
        return Arrays.copyOf(out.byteArray(), n);
    }

    @Test
    public void aRealImageRoundTrips() {
        final byte[] image = genesisImage();
        final byte[] before = image.clone();
        assertTrue("genesis image of " + image.length + " B round-trips", new ApplierSelfCheck().verify(
                new UnsafeBuffer(image), image.length));
        assertArrayEquals("the self-check never writes into the payload it checks", before, image);
    }

    @Test
    public void aTruncatedImageIsRefused() {
        final byte[] image = genesisImage();
        assertFalse("half an image", new ApplierSelfCheck().verify(new UnsafeBuffer(image), image.length / 2));
    }

    @Test
    public void garbageIsRefusedNotThrown() {
        final byte[] garbage = new byte[256];
        Arrays.fill(garbage, (byte)0x5a);
        assertFalse("garbage", new ApplierSelfCheck().verify(new UnsafeBuffer(garbage), garbage.length));
        assertFalse("empty", new ApplierSelfCheck().verify(new UnsafeBuffer(new byte[0]), 0));
    }

    @Test
    public void anImageWithTrailingBytesIsRefused() {
        final byte[] image = genesisImage();
        final byte[] longer = Arrays.copyOf(image, image.length + 8);
        assertFalse("the re-serialized length must equal the payload length",
                new ApplierSelfCheck().verify(new UnsafeBuffer(longer), longer.length));
    }
}
