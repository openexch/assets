// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets;

import com.openexchange.assets.infrastructure.persistence.AeronCluster;

/**
 * Entry point for an Assets Engine cluster node. Launches a media driver (embedded by default) +
 * archive + consensus module + clustered service on AE-isolated ports ({@code 9300+}) and dirs. Idle
 * mode defaults to BACKOFF so the node does not busy-spin on a shared box; a dedicated deployment opts
 * into {@code TRANSPORT_IDLE_MODE=busy_spin}. All config is env-var-first (see {@code AeronCluster}).
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        new AeronCluster();
    }
}
