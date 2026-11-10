// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.replica;

import com.openexchange.assets.infrastructure.persistence.AssetsLogApplier;
import com.openexchange.cluster.testkit.ShadowRestoreRunner;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The replica's assets binding carries the members' container app version and validator (the
 * members set neither, Aeron's defaults), the assets engine id, a stable identity over the live engine and the
 * production self-check. It pins what the replica cluster tests rely on.
 */
public class AssetsReplicaBindingTest {

    @Test
    public void bindingCarriesTheMembersContainerDefaults() {
        final AssetsShadowReplicaMain.Binding b = new AssetsShadowReplicaMain.Binding(() -> { });
        assertEquals("members' appVersion (0.0.1)", ShadowRestoreRunner.DEFAULT_APP_VERSION, b.appVersion());
        assertSame("members' validator", ShadowRestoreRunner.DEFAULT_VALIDATOR, b.validator());
        assertEquals(AssetsLogApplier.ENGINE_ID_ASSETS, b.engineId());
        assertSame("the applier is the engine", b.applier(), b.engine());
        assertSame("one identity source over the live engine", b.identity(), b.identity());
        assertTrue(b.selfCheck() instanceof ApplierSelfCheck);
    }

    private static com.openexchange.cluster.shadow.ShadowHeader header(final long clusterTime) {
        return new com.openexchange.cluster.shadow.ShadowHeader(1, 0, 1, 2, 3, 1, 7, 4096, 0, 0, clusterTime, 0, 0, 64,
                0, 4096, 5);
    }

    @Test
    public void theRestoreWitnessIsTheShadowRestoreRule() {
        final AssetsShadowReplicaMain.Binding b = new AssetsShadowReplicaMain.Binding(() -> { });
        b.applier().engine().setLastLogTime(1_000);
        assertEquals("equal times hold", null, b.postRestoreWitness(header(1_000)));
        assertEquals("shadow P_s witness failed: lastLogTime=1000 > header.clusterTime=999",
                b.postRestoreWitness(header(999)));
    }

    @Test
    public void aCoreConfigFailureStopsAndThrows() throws Exception {
        final AtomicInteger stops = new AtomicInteger();
        final AssetsShadowReplicaMain.Binding b = new AssetsShadowReplicaMain.Binding(stops::incrementAndGet);
        // the fail-fast the applier holds: reach it through the engine's own seam
        final java.lang.reflect.Field f = b.applier().engine().getClass().getDeclaredField("configFailFast");
        f.setAccessible(true);
        final Runnable failFast = (Runnable)f.get(b.applier().engine());
        final IllegalStateException thrown = org.junit.Assert.assertThrows(IllegalStateException.class, failFast::run);
        assertEquals(AssetsShadowReplicaMain.CORE_CONFIG_FAILED, thrown.getMessage());
        assertEquals("the lifecycle stop ran once before the throw", 1, stops.get());
    }
}
