// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.auth;

import com.openexchange.core.auth.CoreAuth;
import com.openexchange.core.cluster.TestAuth;
import com.openexchange.core.infrastructure.persistence.CoreClusteredService;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.auth.ClusterAuthenticator;
import io.aeron.security.Authenticator;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** The core's consensus module runs the kit authenticator with the core policy (one copy, both engines). */
public class KitAuthenticatorInstalledTest {

    @Test
    public void theConsensusModuleAuthenticatorIsTheKitCopyWithTheCorePolicy() {
        TestAuth.install();
        final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), 29_100, new CoreClusteredService());
        final Authenticator a = cfg.consensusModuleContext().authenticatorSupplier().get();
        assertTrue("installed: " + a.getClass().getName(), a instanceof ClusterAuthenticator);
        final ClusterAuthenticator kit = (ClusterAuthenticator) a;
        assertSame(CoreAuth.POLICY, kit.keys().policy());
        assertEquals("the test table's eight keys (the replica added the REPLICA key)", 8, kit.keys().size());
    }
}
