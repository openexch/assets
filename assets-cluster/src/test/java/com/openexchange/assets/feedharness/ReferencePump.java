// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.infrastructure.generated.SessionClass;

import java.util.List;
import java.util.Map;

/**
 * RC2 S9h 2.1 / D-S9h-6: the harness self-test's trivial in-order feed. It registers one BRIDGE session (the pinned
 * identity 0xB1) and offers the frames it is handed, in order, nothing else: it reads no archive, keeps no cursor table,
 * never fails over and never re-offers. It is NOT a bridge (R1; {@code FeedHarnessGuardTest} pins that this class imports
 * no archive client) and exists only so the T1 tools can be proved against a real cluster before the C++ bridge exists.
 */
public final class ReferencePump implements AutoCloseable {

    public static final long IDENTITY = 0xB1L;

    private final T1Session session;
    private long offered;

    private ReferencePump(final T1Session session) {
        this.session = session;
    }

    /** Connects with {@code T1_BRIDGE_KEY} and registers {BRIDGE, acks|settlements, resumable, 0xB1, sourceEpoch 0}. */
    public static ReferencePump open(final Map<String, String> env) throws InterruptedException {
        final T1Session s = T1Session.connect(env, "T1_BRIDGE_KEY");
        final long corr = 0x9B1L;
        s.offer(new CoreFrames().register(corr, SessionClass.BRIDGE, IDENTITY, 0));
        final byte[] ack = s.reply(corr, 110);
        final int result = ack[8 + 10] & 0xFF;
        if (result != 0 && result != 1) {
            s.close();
            throw new IllegalStateException("the pump's RegisterSession was refused (AdminAck result " + result + ")");
        }
        return new ReferencePump(s);
    }

    public void pump(final List<byte[]> frames) {
        for (final byte[] f : frames) {
            session.offer(f);
            offered++;
        }
        session.poll();
    }

    public long offered() {
        return offered;
    }

    @Override
    public void close() {
        session.close();
    }
}
