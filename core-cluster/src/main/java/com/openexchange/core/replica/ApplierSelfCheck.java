// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

/**
 * The production copy of the {@code CoreShadowRig.ApplierSelfCheck} round trip. The kit's
 * writer thread calls it on the frozen payload before the first chunk is offered: restore the payload into a SCRATCH
 * {@link CoreLogApplier}, run the engine's restore invariants (locked == sum of holds included), re-serialize and
 * compare byte for byte. The kit compares the payload digest before and after ({@code SELF_CHECK_MUTATED_BUFFER}).
 *
 * <p>The scratch applier is a transient allocation per write: the live engine keeps applying on the follower
 * thread meanwhile and is never touched here. A CoreConfig cross-check on the scratch is a failure, not a stop: the
 * scratch's fail-fast throws, which makes {@link #verify} return false.</p>
 *
 * <p>Any failure of the round trip is {@code false} with one CRITICAL line naming the cause (the writer turns it into
 * {@code SELF_CHECK_FAILED} and the replica fences); a {@link VirtualMachineError} (an undersized heap, 8.6) is not a
 * verdict on the image and propagates to the writer thread's error path.</p>
 */
public final class ApplierSelfCheck implements ShadowSelfCheck {

    @Override
    public boolean verify(final DirectBuffer payload, final int length) {
        try {
            final CoreLogApplier scratch = new CoreLogApplier(() -> {
                throw new IllegalStateException("CoreConfig cross-check failed on the scratch applier");
            });
            scratch.restore(payload, length);
            scratch.engine().verifyRestoredInvariants();
            final ExpandableArrayBuffer again = new ExpandableArrayBuffer(Math.max(64, length + 64));
            final int n = scratch.serialize(again);
            if (n != length) {
                return refuse("re-serialized length " + n + " != payload length " + length);
            }
            for (int i = 0; i < length; i++) {
                if (again.getByte(i) != payload.getByte(i)) {
                    return refuse("re-serialized byte " + i + " of " + length + " differs");
                }
            }
            return true;
        } catch (final VirtualMachineError fatal) {
            throw fatal;
        } catch (final Throwable failed) {
            return refuse(failed.toString());
        }
    }

    private static boolean refuse(final String why) {
        System.err.println("[REPLICA] CRITICAL: shadow self-check refused the image: " + why);
        return false;
    }

    @Override
    public String toString() {
        return "ApplierSelfCheck(scratch CoreLogApplier round trip)";
    }
}
