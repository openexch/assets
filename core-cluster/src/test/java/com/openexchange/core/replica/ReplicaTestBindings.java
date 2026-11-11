// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import io.aeron.cluster.VersionValidator;

import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * The production core binding ({@link CoreShadowReplicaMain.Binding}) for the cluster tests in other
 * packages, plus one wrapper that swaps only the self-check. Test sources only.
 */
public final class ReplicaTestBindings {

    private ReplicaTestBindings() {
    }

    /** The binding {@code core-cluster.jar replica} runs; {@code onCoreConfigFailure} is the lifecycle stop it calls. */
    public static ReplicaEngine production(final Runnable onCoreConfigFailure) {
        return new CoreShadowReplicaMain.Binding(onCoreConfigFailure);
    }

    /** The live applier behind a binding from {@link #production} (read-only use by the tests). */
    public static CoreLogApplier applier(final ReplicaEngine binding) {
        return ((CoreShadowReplicaMain.Binding)binding).applier();
    }

    /** {@code binding} with its self-check replaced by {@code swap.apply(original)}. */
    public static ReplicaEngine withSelfCheck(final ReplicaEngine binding, final UnaryOperator<ShadowSelfCheck> swap) {
        return wrap(binding, swap.apply(binding.selfCheck()), binding.engine());
    }

    /**
     * Alignment test seam: {@code binding} with its live engine replaced by {@code swap.apply(original)} (a
     * delegate that mutates the replica's state, which the scratch self-check cannot see).
     */
    public static ReplicaEngine withEngine(final ReplicaEngine binding, final UnaryOperator<ShadowEngine> swap) {
        return wrap(binding, binding.selfCheck(), swap.apply(binding.engine()));
    }

    /**
     * A delegate that drops the next session message after {@link #armDropNext} (one shot): the replica's state then
     * misses one command the members applied (the self-check still round-trips it).
     */
    public static final class DroppingEngine implements ShadowEngine {
        private final ShadowEngine delegate;
        private volatile boolean dropNext;
        private volatile long droppedAt = -1;

        public DroppingEngine(final ShadowEngine delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        public void armDropNext() {
            droppedAt = -1;
            dropNext = true;
        }

        public long droppedAt() {
            return droppedAt;
        }

        @Override
        public int engineId() {
            return delegate.engineId();
        }

        @Override
        public void genesis() {
            delegate.genesis();
        }

        @Override
        public void restore(final org.agrona.DirectBuffer image, final int length) {
            delegate.restore(image, length);
        }

        @Override
        public int serialize(final org.agrona.MutableDirectBuffer dst) {
            return delegate.serialize(dst);
        }

        @Override
        public void onSessionOpen(final long clusterSessionId, final long logPosition, final long timestamp,
                                  final byte[] encodedPrincipal) {
            delegate.onSessionOpen(clusterSessionId, logPosition, timestamp, encodedPrincipal);
        }

        @Override
        public void onSessionClose(final long clusterSessionId, final long logPosition, final long timestamp,
                                   final io.aeron.cluster.codecs.CloseReason closeReason) {
            delegate.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
        }

        @Override
        public boolean onSessionMessage(final long clusterSessionId, final long logPosition, final long timestamp,
                                        final org.agrona.DirectBuffer buffer, final int offset, final int length) {
            if (dropNext) {
                dropNext = false;
                droppedAt = logPosition;
                System.err.println("[REPLICA-TEST] DroppingEngine: session message at " + logPosition
                        + " dropped (the replica state now differs from the members')");
                return true;
            }
            return delegate.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
        }

        @Override
        public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
            return delegate.onTimerEvent(correlationId, logPosition, timestamp);
        }

        @Override
        public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                             final long timestamp, final java.util.concurrent.TimeUnit timeUnit) {
            delegate.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
        }
    }

    /** A delegate that drops every session open before the engine sees it (a replica-side divergence). */
    public static final class SkipSessionOpenEngine implements ShadowEngine {
        private final ShadowEngine delegate;

        public SkipSessionOpenEngine(final ShadowEngine delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public int engineId() {
            return delegate.engineId();
        }

        @Override
        public void genesis() {
            delegate.genesis();
        }

        @Override
        public void restore(final org.agrona.DirectBuffer image, final int length) {
            delegate.restore(image, length);
        }

        @Override
        public int serialize(final org.agrona.MutableDirectBuffer dst) {
            return delegate.serialize(dst);
        }

        @Override
        public void onSessionOpen(final long clusterSessionId, final long logPosition, final long timestamp,
                                  final byte[] encodedPrincipal) {
            // dropped
        }

        @Override
        public void onSessionClose(final long clusterSessionId, final long logPosition, final long timestamp,
                                   final io.aeron.cluster.codecs.CloseReason closeReason) {
            delegate.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
        }

        @Override
        public boolean onSessionMessage(final long clusterSessionId, final long logPosition, final long timestamp,
                                        final org.agrona.DirectBuffer buffer, final int offset, final int length) {
            return delegate.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
        }

        @Override
        public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
            return delegate.onTimerEvent(correlationId, logPosition, timestamp);
        }

        @Override
        public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                             final long timestamp, final java.util.concurrent.TimeUnit timeUnit) {
            delegate.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
        }
    }

    private static ReplicaEngine wrap(final ReplicaEngine binding, final ShadowSelfCheck check,
                                      final ShadowEngine engine) {
        Objects.requireNonNull(binding, "binding");
        return new ReplicaEngine() {
            @Override
            public int engineId() {
                return binding.engineId();
            }

            @Override
            public ShadowEngine engine() {
                return engine;
            }

            @Override
            public ShadowIdentitySource identity() {
                return binding.identity();
            }

            @Override
            public ShadowSelfCheck selfCheck() {
                return check;
            }

            @Override
            public int appVersion() {
                return binding.appVersion();
            }

            @Override
            public VersionValidator validator() {
                return binding.validator();
            }

            @Override
            public String postRestoreWitness(final ShadowHeader header) {
                return binding.postRestoreWitness(header);
            }

            @Override
            public long imageGenesisId(final org.agrona.DirectBuffer image, final int length) {
                return binding.imageGenesisId(image, length);
            }
        };
    }
}
