// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import com.openexchange.assets.auth.CoreCredentials;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * RC2 S9h 7.3: one authenticated core session of a T1 tool (t1-core, the reference pump): egress captured as raw money v7
 * frames, keep-alives while waiting, offers retried. Credentials are {@code keyId:CLASS:secretHex} from the environment
 * (7.1); the ingress endpoints from {@code T1_CORE_INGRESS}, the client media driver from {@code T1_AERON_DIR}.
 */
public final class T1Session implements EgressListener, AutoCloseable {

    public static final long TIMEOUT_MS = 30_000;
    public static final String INGRESS_CHANNEL = "aeron:udp?term-length=4m|mtu=8192";

    final AeronCluster client;
    final List<byte[]> frames = new ArrayList<>();
    private long lastKeepAlive = System.currentTimeMillis();
    private int scanned;

    /** The value of {@code name} in {@code env}, or Exit 78 (rule H5). */
    public static String need(final Map<String, String> env, final String name) {
        final String v = env.get(name);
        if (v == null || v.isBlank()) {
            throw new FeedHarnessMain.Exit(FeedHarnessMain.CONFIG, "H5: " + name + " is not set");
        }
        return v;
    }

    public static T1Session connect(final Map<String, String> env, final String keyVar) throws InterruptedException {
        final CoreCredentials credentials;
        try {
            credentials = CoreCredentials.fromSpec(need(env, keyVar));
        } catch (FeedHarnessMain.Exit e) {
            throw e;
        } catch (RuntimeException e) {
            throw new FeedHarnessMain.Exit(FeedHarnessMain.CONFIG, "H5: " + keyVar + " is malformed: " + e.getMessage());
        }
        return new T1Session(need(env, "T1_AERON_DIR"), need(env, "T1_CORE_INGRESS"), credentials);
    }

    public T1Session(final String aeronDir, final String ingress, final CoreCredentials credentials) throws InterruptedException {
        AeronCluster c = null;
        RuntimeException last = null;
        final long deadline = System.currentTimeMillis() + 60_000;
        while (c == null && System.currentTimeMillis() < deadline) {
            try {
                c = AeronCluster.connect(new AeronCluster.Context().credentialsSupplier(credentials.supplier()).egressListener(this)
                        .egressChannel("aeron:udp?endpoint=localhost:0").ingressChannel(INGRESS_CHANNEL)
                        .aeronDirectoryName(aeronDir).ingressEndpoints(ingress).messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
            } catch (RuntimeException e) {
                last = e;
                if (String.valueOf(e.getMessage()).contains("authentication")) {
                    break; // a refused key does not get better by retrying
                }
                Thread.sleep(250);
            }
        }
        if (c == null) {
            throw new IllegalStateException("could not connect as " + credentials.sessionClass() + " key " + credentials.keyId(), last);
        }
        client = c;
    }

    @Override
    public void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer, final int offset, final int length,
                          final Header header) {
        final byte[] b = new byte[length];
        buffer.getBytes(offset, b);
        frames.add(b);
    }

    public void poll() {
        client.pollEgress();
        final long now = System.currentTimeMillis();
        if (now - lastKeepAlive >= 500) {
            client.sendKeepAlive();
            lastKeepAlive = now;
        }
    }

    public void offer(final byte[] frame) {
        final UnsafeBuffer b = new UnsafeBuffer(frame);
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (client.offer(b, 0, frame.length) < 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("offer not accepted within " + TIMEOUT_MS + " ms");
            }
            poll();
            Thread.onSpinWait();
        }
    }

    public static int templateOf(final byte[] f) {
        return f.length >= 4 ? (f[2] & 0xFF) | (f[3] & 0xFF) << 8 : -1;
    }

    /** The correlationId of a reply that starts with one (OrderAck/Reject, Deposit/Withdraw acks and rejects, AdminAck, 17). */
    public static long correlationOf(final byte[] f) {
        return new UnsafeBuffer(f).getLong(8, ByteOrder.LITTLE_ENDIAN);
    }

    /** The first frame (from the beginning of the capture) matching {@code p}, waiting for it. */
    public byte[] await(final Predicate<byte[]> p, final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            for (final byte[] f : frames) {
                if (p.test(f)) {
                    return f;
                }
            }
            if (System.currentTimeMillis() > deadline) {
                return null;
            }
            poll();
            Thread.onSpinWait();
        }
    }

    /** The next frame after the last one {@link #next} returned that matches {@code p}. */
    public byte[] next(final Predicate<byte[]> p, final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            while (scanned < frames.size()) {
                final byte[] f = frames.get(scanned++);
                if (p.test(f)) {
                    return f;
                }
            }
            if (System.currentTimeMillis() > deadline) {
                return null;
            }
            poll();
            Thread.onSpinWait();
        }
    }

    public void skipToEnd() {
        poll();
        scanned = frames.size();
    }

    /** Waits for the reply to {@code corr} among {@code templates}. */
    public byte[] reply(final long corr, final int... templates) {
        final byte[] f = await(x -> {
            final int t = templateOf(x);
            for (final int k : templates) {
                if (t == k && correlationOf(x) == corr) {
                    return true;
                }
            }
            return false;
        }, TIMEOUT_MS);
        if (f == null) {
            throw new IllegalStateException("no reply to correlationId " + corr + " within " + TIMEOUT_MS + " ms");
        }
        return f;
    }

    @Override
    public void close() {
        client.close();
    }
}
