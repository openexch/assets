// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.IOException;

/**
 * Money projector entry point: AE money journal (Aeron Archive recording) -> Postgres.
 *
 * Runs a lightweight embedded media driver (own /dev/shm dir, SHARED threading - the projector is
 * backoff-idle by nature, it must never busy-spin on the shared box), one {@link ProjectorAgent}
 * thread, and the metrics server. Resumable purely from the Postgres high-water: safe to kill and
 * restart at any time (idempotent ON CONFLICT), and a duplicate instance is safe too (both converge
 * on the same rows), just wasteful.
 */
public final class ProjectorMain {

    public static void main(final String[] args) {
        final ProjectorConfig config = ProjectorConfig.fromEnv();
        final ProjectorState state = new ProjectorState();

        // Balances are money: Postgres being reachable is a hard boot dependency (the pool itself is
        // lazy, but a bad URL/credential should fail loudly rather than idle-park pretending health).
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.postgresUrl);
        hikariConfig.setUsername(config.postgresUser);
        hikariConfig.setPassword(config.postgresPassword);
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(5000);
        hikariConfig.setPoolName("projector-pg");
        final HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        final PostgresMoneyJournalStore store = new PostgresMoneyJournalStore(dataSource);

        final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName("/dev/shm/aeron-projector-" + ProcessHandle.current().pid())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));

        final ProjectorAgent agent = new ProjectorAgent(config, driver.aeronDirectoryName(), state, store);
        final Thread thread = new Thread(agent, "money-projector");
        thread.setDaemon(false);
        thread.start();

        final ProjectorMetricsServer metricsServer = new ProjectorMetricsServer(state);
        try {
            metricsServer.start(config.metricsPort);
        } catch (IOException e) {
            throw new RuntimeException("failed to start projector metrics server on port "
                    + config.metricsPort, e);
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
            dataSource.close();
        }, "projector-shutdown"));
        barrier.await();
    }

    private ProjectorMain() {
    }
}
