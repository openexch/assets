// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.auth;

import com.openexchange.assets.auth.AssetsAuth;
import com.openexchange.assets.cluster.TestAuth;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.auth.ClusterAuthenticator;
import io.aeron.security.Authenticator;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.3: the AE's consensus module runs the kit authenticator with the AE policy (one copy, both engines). */
public class KitAuthenticatorInstalledTest {

    @Test
    public void theConsensusModuleAuthenticatorIsTheKitCopyWithTheAePolicy() {
        TestAuth.install();
        final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), 29_100, new AssetsClusteredService());
        final Authenticator a = cfg.consensusModuleContext().authenticatorSupplier().get();
        assertTrue("installed: " + a.getClass().getName(), a instanceof ClusterAuthenticator);
        final ClusterAuthenticator kit = (ClusterAuthenticator) a;
        assertSame(AssetsAuth.POLICY, kit.keys().policy());
        assertEquals("the test table's seven keys", 7, kit.keys().size());
    }
}
