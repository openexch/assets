// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

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

        // SIGTERM: this hook is the whole stop. The JVM exits with the signal's status (143) once it returns; nothing
        // calls System.exit or halt. No agrona ShutdownSignalBarrier: constructing one registers agrona's own static
        // shutdown hook, which waits for close() on every barrier it signalled (agrona 2.6.0
        // ShutdownSignalBarrier.java:96-100, 251-279) and nothing called it, so the JVM never exited after this
        // hook finished in about 1 ms; docker killed it at its 10 s stop timeout (137, RC2 S16 W6).
        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                agent.stop();
                // Wakes the agent out of a blocking archive connect (AeronArchive.connect waits up to its message
                // timeout per endpoint and honours only an interrupt) or a backoff sleep. A flush cut short rolls
                // back; the projector resumes from the Postgres high-water (idempotent ON CONFLICT).
                thread.interrupt();
                try {
                    thread.join(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                metricsServer.stop();
                CloseHelper.quietClose(driver);
                dataSource.close();
            } finally {
                stopped.countDown();
            }
        }, "projector-shutdown"));
        // Park main until the stop: a projector halted on a gap keeps serving /health (halted=true).
        awaitUninterruptibly(stopped);
    }

    private static void awaitUninterruptibly(final CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private ProjectorMain() {
    }
}
