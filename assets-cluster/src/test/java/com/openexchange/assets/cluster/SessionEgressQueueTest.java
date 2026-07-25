// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.infrastructure.publisher.AssetsEventPublisher;
import com.openexchange.assets.infrastructure.publisher.SessionEgressQueue;
import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The money-safety contract of the egress queue, without a cluster: a back-pressured session must lose
 * nothing and must keep its frames in order, and a dead session must not hold anything up.
 *
 * <p>These are the properties the old inline {@code broadcast} bought by parking the whole
 * deterministic thread. They have to survive the move to a queue.</p>
 */
public class SessionEgressQueueTest {

    /** A session whose offer result is scripted, recording every frame it accepts. */
    private static final class ScriptedSession implements ClientSession {
        private final long id;
        private final List<Integer> delivered = new ArrayList<>();
        private long result = 1; // positive == delivered
        private int acceptsRemaining = Integer.MAX_VALUE;

        ScriptedSession(final long id) {
            this.id = id;
        }

        void backPressureAfter(final int accepts) {
            this.acceptsRemaining = accepts;
        }

        void allowAll() {
            this.acceptsRemaining = Integer.MAX_VALUE;
        }

        void result(final long result) {
            this.result = result;
        }

        int[] delivered() {
            return delivered.stream().mapToInt(Integer::intValue).toArray();
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public int responseStreamId() {
            return 0;
        }

        @Override
        public String responseChannel() {
            return "aeron:ipc";
        }

        @Override
        public byte[] encodedPrincipal() {
            return new byte[0];
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosing() {
            return false;
        }

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            if (result <= 0) {
                return result;
            }
            if (acceptsRemaining <= 0) {
                return Publication.BACK_PRESSURED;
            }
            acceptsRemaining--;
            delivered.add(buffer.getInt(offset)); // frames are tagged with an int marker
            return 1;
        }

        @Override
        public long offer(final DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long tryClaim(final int length, final BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }
    }

    private final UnsafeBuffer frame = new UnsafeBuffer(new byte[64]);

    private void append(final SessionEgressQueue queue, final int marker) {
        frame.putInt(0, marker);
        assertTrue("append below max capacity must succeed", queue.append(frame, 0, 32));
    }

    @Test
    public void deliversInOrderWhenTheSessionKeepsUp() {
        final ScriptedSession session = new ScriptedSession(7);
        final SessionEgressQueue queue = new SessionEgressQueue(session);

        for (int i = 1; i <= 5; i++) {
            append(queue, i);
        }
        assertFalse("frames are queued, not offered inline", queue.isEmpty());

        queue.drainAll();

        assertArrayEquals(new int[] {1, 2, 3, 4, 5}, session.delivered());
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.backPressureCount());
    }

    @Test
    public void backPressureKeepsTheRefusedFrameAndEverythingAfterIt() {
        final ScriptedSession session = new ScriptedSession(7);
        final SessionEgressQueue queue = new SessionEgressQueue(session);

        for (int i = 1; i <= 5; i++) {
            append(queue, i);
        }
        session.backPressureAfter(2);

        queue.drainAll();

        assertArrayEquals("only what the session took", new int[] {1, 2}, session.delivered());
        assertFalse("the refused frames are still queued", queue.isEmpty());
        assertTrue("back-pressure is counted", queue.backPressureCount() > 0);

        // The consumer recovers: the next drain delivers the rest, in order, nothing lost, nothing
        // duplicated. This is what replaces the inline path's unbounded park.
        session.allowAll();
        queue.drainAll();

        assertArrayEquals(new int[] {1, 2, 3, 4, 5}, session.delivered());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void framesQueuedWhileBackPressuredAreDeliveredAfterRecovery() {
        final ScriptedSession session = new ScriptedSession(7);
        final SessionEgressQueue queue = new SessionEgressQueue(session);

        session.backPressureAfter(0);
        append(queue, 1);
        queue.drainAll();
        assertEquals(0, session.delivered().length);

        append(queue, 2);
        append(queue, 3);
        session.allowAll();
        queue.drainAll();

        assertArrayEquals(new int[] {1, 2, 3}, session.delivered());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void aDeadSessionDiscardsRatherThanBlocking() {
        final ScriptedSession session = new ScriptedSession(7);
        final SessionEgressQueue queue = new SessionEgressQueue(session);

        for (int i = 1; i <= 3; i++) {
            append(queue, i);
        }
        session.result(Publication.NOT_CONNECTED);

        queue.drainAll();

        assertEquals("nothing delivered to a gone session", 0, session.delivered().length);
        assertTrue("but the queue drains, so it can never wedge the engine", queue.isEmpty());
        assertEquals("a gone session is not back-pressure", 0, queue.backPressureCount());
    }

    @Test
    public void sessionsAreIsolatedFromEachOther() {
        final ScriptedSession slow = new ScriptedSession(1);
        final ScriptedSession fast = new ScriptedSession(2);
        final SessionEgressQueue slowQueue = new SessionEgressQueue(slow);
        final SessionEgressQueue fastQueue = new SessionEgressQueue(fast);

        slow.backPressureAfter(0);
        for (int i = 1; i <= 3; i++) {
            append(slowQueue, i);
            append(fastQueue, i);
        }

        slowQueue.drainAll();
        fastQueue.drainAll();

        assertEquals("the slow consumer took nothing", 0, slow.delivered().length);
        assertArrayEquals("and did not hold up the other session", new int[] {1, 2, 3}, fast.delivered());
    }

    @Test
    public void appendRefusesOnlyAtMaxCapacitySoTheCallerCanFreeSpace() {
        final ScriptedSession session = new ScriptedSession(7);
        final SessionEgressQueue queue = new SessionEgressQueue(session);
        session.backPressureAfter(0);

        // Fill past the growth ceiling. append() must report false rather than shed a money event.
        boolean refused = false;
        for (int i = 0; i < 1_000_000 && !refused; i++) {
            frame.putInt(0, i);
            refused = !queue.append(frame, 0, 64);
        }
        assertTrue("the queue must refuse rather than grow without bound", refused);
        assertTrue("it grew to its ceiling first", queue.pendingBytes() > 8 * 1024 * 1024);

        // Freeing space is what the caller does instead of dropping: once the consumer moves, the
        // refused frame fits.
        session.allowAll();
        queue.drainAll();
        frame.putInt(0, 42);
        assertTrue(queue.append(frame, 0, 64));
    }

    @Test
    public void resetDropsEverything() {
        final ScriptedSession session = new ScriptedSession(7);
        final SessionEgressQueue queue = new SessionEgressQueue(session);
        append(queue, 1);
        append(queue, 2);

        queue.reset();

        assertTrue(queue.isEmpty());
        queue.drainAll();
        assertEquals(0, session.delivered().length);
    }

    // ---- step 3: per-session subscription ----

    @Test
    public void anUnsubscribedSessionWantsEverything() {
        final SessionEgressQueue q = new SessionEgressQueue(new ScriptedSession(1));
        assertTrue("default must be the pre-subscription behaviour", q.wants(AssetsEventPublisher.CH_ACKS));
        assertTrue(q.wants(AssetsEventPublisher.CH_BALANCES));
        assertTrue(q.wants(AssetsEventPublisher.CH_SETTLEMENTS));
        assertTrue(q.wants(AssetsEventPublisher.CH_SNAPSHOTS));
    }

    @Test
    public void subscribingNarrowsToExactlyWhatWasAskedFor() {
        final SessionEgressQueue q = new SessionEgressQueue(new ScriptedSession(1));
        q.subscribe(AssetsEventPublisher.CH_ACKS | AssetsEventPublisher.CH_SETTLEMENTS);

        assertTrue(q.wants(AssetsEventPublisher.CH_ACKS));
        assertTrue(q.wants(AssetsEventPublisher.CH_SETTLEMENTS));
        assertFalse("this is the firehose the bridge was drowning in",
                q.wants(AssetsEventPublisher.CH_BALANCES));
        assertFalse(q.wants(AssetsEventPublisher.CH_SNAPSHOTS));
    }

    @Test
    public void snapshotsImplyBalancesBecauseEntriesRideOnBalanceUpdates() {
        final SessionEgressQueue q = new SessionEgressQueue(new ScriptedSession(1));
        q.subscribe(AssetsEventPublisher.CH_SNAPSHOTS);

        // A balance snapshot streams its entries as BalanceUpdate frames and then a terminator carrying
        // the entry COUNT. Subscribing to snapshots without balances would deliver the count with no
        // entries: a silently wrong answer. That combination must not be expressible.
        assertTrue(q.wants(AssetsEventPublisher.CH_SNAPSHOTS));
        assertTrue("snapshots must drag balances in with them",
                q.wants(AssetsEventPublisher.CH_BALANCES));
    }

    @Test
    public void subscribingToNothingIsHonoured() {
        final SessionEgressQueue q = new SessionEgressQueue(new ScriptedSession(1));
        q.subscribe(0);
        assertFalse(q.wants(AssetsEventPublisher.CH_ACKS));
        assertFalse(q.wants(AssetsEventPublisher.CH_BALANCES));
        assertFalse(q.wants(AssetsEventPublisher.CH_SETTLEMENTS));
        assertFalse(q.wants(AssetsEventPublisher.CH_SNAPSHOTS));
    }
}
