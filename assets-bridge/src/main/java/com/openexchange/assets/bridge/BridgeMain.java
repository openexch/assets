// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.IOException;

/**
 * Settlement bridge entry point: ME settlement journal -> Assets Engine ingress.
 *
 * Runs a lightweight embedded media driver (own /dev/shm dir, SHARED threading — the bridge
 * is backoff-idle by nature, it must never busy-spin on the shared box) and the single
 * {@link BridgeAgent} thread. Stateless: safe to kill and restart at any time; a duplicate
 * instance is safe too (the AE dedupes), just wasteful.
 */
public final class BridgeMain {

    public static void main(final String[] args) {
        // Read-only ledger snapshot for the money-check guardrail: `java -jar assets-bridge.jar dump`.
        // Kept in this one entry point so the shade jar stays single-main; AeDump owns its own
        // media driver and exits when done (0 = JSON printed, 3 = snapshot timeout).
        if (args.length > 0 && "dump".equals(args[0])) {
            System.exit(AeDump.run(java.util.Arrays.copyOfRange(args, 1, args.length)));
            return;
        }

        final BridgeConfig config = BridgeConfig.fromEnv();
        final BridgeState state = new BridgeState();

        final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-bridge-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));

        final BridgeAgent agent = new BridgeAgent(config, driver.aeronDirectoryName(), state);
        final Thread thread = new Thread(agent, "settlement-bridge");
        thread.setDaemon(false);
        thread.start();

        final BridgeMetricsServer metricsServer = new BridgeMetricsServer(state);
        try {
            metricsServer.start(config.metricsPort);
        } catch (IOException e) {
            throw new RuntimeException("failed to start bridge metrics server on port " + config.metricsPort, e);
        }

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            agent.stop();
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            metricsServer.stop();
            CloseHelper.quietClose(driver);
        }, "bridge-shutdown"));
        barrier.await();
    }

    private BridgeMain() {
    }
}
