// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.replica;

import com.openexchange.assets.infrastructure.Logger;
import com.openexchange.assets.infrastructure.persistence.AssetsLogApplier;
import com.openexchange.assets.infrastructure.persistence.AssetsShadowIdentity;
import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.replica.ReplicaProcess;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import io.aeron.cluster.VersionValidator;
import io.aeron.cluster.service.ClusteredServiceContainer;

import java.util.Objects;

/**
 * {@code java -jar assets.jar replica}: the Assets Engine shadow replica, a fourth process that is not a
 * cluster member. It follows the AE core log with Aeron's {@code ClusterBackup} into its own archive, applies the
 * committed recording through the members' {@link AssetsLogApplier} and writes shadows at ALIGNED, STANDBY
 * and LOCAL positions. The skeleton is the kit's {@code cluster.replica}; this class binds the assets engine.
 *
 * <p>Order (the {@code AeronCluster} discipline, AC:58-67): install the lifecycle, parse {@link ReplicaSettings} under
 * the {@value ReplicaSettings#PREFIX} guard BEFORE any Aeron component, build the binding, then
 * {@code ReplicaProcess.run}. A refused setting, a failed launch or a fatal fault exits 1 through the lifecycle; a
 * signal exits 143; there is no cluster termination for a replica.</p>
 *
 * <p>Rules: the replica's engine is touched only by the kit follower thread through
 * {@code ReplayContainer}; it publishes nothing (no event sink, journal sink or transport hooks: the applier's
 * defaults are {@code NoopAssetsEventSink} and no journal), schedules nothing and sends no ingress. Positions,
 * times, terms and principals reaching the applier come from log frames only; nothing in this class reads a clock.</p>
 */
public final class AssetsShadowReplicaMain {

    private static final Logger log = Logger.getLogger(AssetsShadowReplicaMain.class);

    /** The text a CoreConfig cross-check failure carries on the replica. */
    static final String CORE_CONFIG_FAILED = "CoreConfig cross-check failed on the replica";

    private AssetsShadowReplicaMain() {
    }

    /** Runs the replica until it stops, then exits through the kit's {@link EngineLifecycle}. Never returns normally. */
    public static void run() {
        final EngineLifecycle lifecycle = EngineLifecycle.install("Assets Engine replica", msg -> log.error("%s", msg));
        try {
            run(lifecycle);
        } catch (final Throwable t) {
            lifecycle.startupFailed(t);
            return;
        }
        lifecycle.finish();
    }

    private static void run(final EngineLifecycle lifecycle) {
        // Settings first: a bad value is refused with the boot guard prefix and exit 1 before anything launches.
        final ReplicaSettings settings = ReplicaSettings.enforce();
        for (final String line : settings.storageWarnings()) {
            log.warn("%s", line);
        }
        final Binding binding = new Binding(() -> lifecycle.fatal(EngineLifecycle.EXIT_FATAL, CORE_CONFIG_FAILED));
        log.info("Assets Engine replica R%d starting: members=%s driverMode=%s source=%s",
                settings.slot(), settings.memberHosts(), settings.driverMode(), settings.sourceType());
        ReplicaProcess.run(settings.config(), binding, lifecycle);
    }

    /**
     * The assets engine for the kit replica: one live {@link AssetsLogApplier}, its identity, the
     * scratch-applier self-check and the members' container app version and validator.
     */
    static final class Binding implements ReplicaEngine {

        private final AssetsLogApplier applier;
        private final AssetsShadowIdentity identity;
        private final ApplierSelfCheck selfCheck = new ApplierSelfCheck();
        private final int appVersion;
        private final VersionValidator validator;

        /**
         * @param onCoreConfigFailure the node-local stop for a CoreConfig cross-check failure; after it the
         *                            applier's fail-fast THROWS, so the frame's apply fails and the follower fences
         *                            before any further shadow is produced
         */
        Binding(final Runnable onCoreConfigFailure) {
            Objects.requireNonNull(onCoreConfigFailure, "onCoreConfigFailure");
            this.applier = new AssetsLogApplier(() -> {
                onCoreConfigFailure.run();
                throw new IllegalStateException(CORE_CONFIG_FAILED);
            });
            this.identity = new AssetsShadowIdentity(applier.engine());
            // the members' container defaults: members set neither appVersion nor the validator (CFG:197-205)
            final ClusteredServiceContainer.Context members = new ClusteredServiceContainer.Context();
            this.appVersion = members.appVersion();
            this.validator = members.appVersionValidator() != null ? members.appVersionValidator()
                    : io.aeron.cluster.AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
        }

        @Override
        public int engineId() {
            return AssetsLogApplier.ENGINE_ID_ASSETS;
        }

        @Override
        public ShadowEngine engine() {
            return applier;
        }

        @Override
        public ShadowIdentitySource identity() {
            return identity;
        }

        @Override
        public ShadowSelfCheck selfCheck() {
            return selfCheck;
        }

        @Override
        public int appVersion() {
            return appVersion;
        }

        @Override
        public VersionValidator validator() {
            return validator;
        }

        /** The ShadowRestore post-restore witness: the restored engine never ran ahead of the image's cluster time. */
        @Override
        public String postRestoreWitness(final ShadowHeader header) {
            final long lastLogTime = applier.engine().getLastLogTime();
            if (lastLogTime > header.clusterTime()) {
                return "shadow P_s witness failed: lastLogTime=" + lastLogTime + " > header.clusterTime="
                        + header.clusterTime();
            }
            return null;
        }

        /**
         * The genesis a member's full snapshot image ADOPTed, through a SCRATCH applier (the live one is
         * never touched; the image is the members' {@code restoreFromImage} input). A CoreConfig cross-check on the
         * scratch throws, so an undecodable image is a loud refusal on the follower thread, never a guess.
         */
        @Override
        public long imageGenesisId(final org.agrona.DirectBuffer image, final int length) {
            final AssetsLogApplier scratch = new AssetsLogApplier(() -> {
                throw new IllegalStateException("CoreConfig cross-check failed on the genesis-check scratch applier");
            });
            scratch.restore(image, length);
            return new AssetsShadowIdentity(scratch.engine()).genesisId();
        }

        /** The live applier (tests and the scans read it; only the follower thread applies to it). */
        AssetsLogApplier applier() {
            return applier;
        }
    }
}
