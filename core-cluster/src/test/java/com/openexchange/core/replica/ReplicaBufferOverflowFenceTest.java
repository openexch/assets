// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.replica.ReplicaFaults;
import com.openexchange.cluster.replica.ReplicaNegativeControl;
import com.openexchange.cluster.replica.ShadowTriggerPolicy;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowWriterThread;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A core image larger than the replica's shadow buffer fences the replica
 * with the buffer text, on the production chain the follower builds (the production core binding, its applier in a
 * kit {@code ReplayContainer}, {@code container::serialize} as the policy's serializer, ReplicaFollower:464-467). The
 * kit's own row ({@code ShadowTriggerPolicyTest.anImageLargerThanTheBufferFences}) throws the overflow from a fake
 * serializer; here the overflow comes out of the real codec through the container, which wraps every engine failure
 * in a {@code ReplayStoppedException}. Nothing reaches the writer.
 */
public class ReplicaBufferOverflowFenceTest {

    static final String OVERFLOW_TEXT = "FATAL replica: image exceeds REPLICA_SHADOW_BUFFER_MB=16";

    /** An idle, running writer that records what it is offered. */
    static final class IdleWriter implements ShadowTriggerPolicy.WriterPort {
        final List<ShadowWriterThread.Request> offered = new ArrayList<>();

        @Override
        public boolean isIdle() {
            return true;
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean offer(final ShadowWriterThread.Request request) {
            offered.add(request);
            return true;
        }
    }

    static ShadowIdentitySource identity() {
        return new ShadowIdentitySource() {
            @Override
            public long genesisId() {
                return 7; // past ADOPT: the trigger serializes (the identity is not what this row tests)
            }

            @Override
            public boolean journalEnabled() {
                return false;
            }

            @Override
            public long journalSeq() {
                return 0;
            }

            @Override
            public int payloadLayoutHint() {
                return 0;
            }
        };
    }

    @Test
    public void aCoreImageLargerThanTheShadowBufferFencesWithTheBufferText() {
        final ReplicaEngine binding = ReplicaTestBindings.production(() -> {
            throw new AssertionError("no CoreConfig frame in this row");
        });
        final ReplayContainer[] container = new ReplayContainer[1];
        final List<String> fences = new ArrayList<>();

        // the full image of a genesis engine, through the same applier: the payload room is half of it
        final ReplayContainer sizing = new ReplayContainer(ReplicaTestBindings.production(() -> { }).engine(),
                binding.appVersion(), binding.validator(), ShadowTriggerListener.NONE);
        sizing.genesis();
        final int full = sizing.serialize(new ExpandableArrayBuffer(64 * 1024));
        final int room = full / 2;
        assertTrue("a genesis image has bytes: " + full, room > 8);

        final ShadowImageBuffer image = new ShadowImageBuffer(new UnsafeBuffer(new byte[ShadowHeader.LENGTH + room]));
        assertEquals(room, image.payloadCapacity());
        final ShadowTriggerPolicy policy = new ShadowTriggerPolicy(image, binding.engineId(),
                () -> container[0].cursor(), identity(), dst -> container[0].serialize(dst), SystemNanoClock.INSTANCE,
                new BusySpinIdleStrategy(), TimeUnit.SECONDS.toNanos(1), 0, 16, fences::add, ReplicaFaults.NONE,
                EnumSet.noneOf(ReplicaNegativeControl.class));
        container[0] = new ReplayContainer(binding.engine(), binding.appVersion(), binding.validator(), policy);
        container[0].genesis();
        policy.startAt(container[0].cursor());
        final IdleWriter writer = new IdleWriter();
        policy.attachWriter(writer);

        assertTrue(policy.onSnapshotAction(container[0].cursor().logPosition(), 0, 1_000, 0));
        assertNull("nothing handed to the writer", policy.producePending());
        System.out.println("[overflow] image " + full + " B, payload capacity " + room + " B, fences "
                + fences);
        assertEquals(fences.toString(), 1, fences.size());
        assertTrue("an image over the buffer must fence with the buffer text (the operator's only hint to raise"
                + " REPLICA_SHADOW_BUFFER_MB), got: " + fences.get(0), fences.get(0).startsWith(OVERFLOW_TEXT));
        assertTrue("the fence names the payload capacity: " + fences.get(0),
                fences.get(0).contains("(payload capacity " + room + " B)"));
        assertTrue("nothing offered", writer.offered.isEmpty());
    }
}
