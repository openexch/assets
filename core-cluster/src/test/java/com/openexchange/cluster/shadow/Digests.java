// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.concurrent.UnsafeBuffer;

/** The kit's payload digest of an image (this class sits in the kit package in core TEST sources only). */
public final class Digests {

    private Digests() {
    }

    /** The SHA-256-based payload digest the writer stamps into a shadow header for these payload bytes. */
    public static long of(final byte[] payload) {
        return PayloadDigest.of(new UnsafeBuffer(payload), 0, payload.length);
    }
}
