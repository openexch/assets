// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.cluster.EngineLifecycle;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;

import java.io.IOException;

/**
 * Money projector entry point: core money journal (Aeron Archive recording) -> Postgres.
 *
 * Runs on the media driver {@link ProjectorDriver} selects: by default a lightweight embedded driver (own /dev/shm
 * dir, SHARED threading - the projector is backoff-idle by nature, it must never busy-spin on the shared box), in
 * production the external aeronmd at AERON_DIR ({@code PROJECTOR_DRIVER_MODE=external}); then one
 * {@link ProjectorAgent} thread and the metrics server. A setting it cannot use, or no active external driver within
 * the driver wait, exits 1 before anything starts. Resumable purely from the Postgres high-water: safe to kill and
 * restart at any time (idempotent ON CONFLICT), and a duplicate instance is safe too (both converge
 * on the same rows), just wasteful.
 */
public final class ProjectorMain {

    /** The lifecycle's name: its log lines and thread names. */
    static final String NAME = "money-projector";
    /** Bound on the agent's stop after the stop cause, below the lifecycle's default 4 s shutdown timeout. */
    private static final long AGENT_JOIN_MS = 3_000;

    public static void main(final String[] args) {
        // First, before the lifecycle: a deployment written for the retired names refuses before any setting is read.
        com.openexchange.core.config.RetiredSettings.refuseRetired("core-projector.jar");
        final EngineLifecycle lifecycle;
        try {
            // First, before any thread starts (as the engines): owns the SIGTERM hook and parses its three settings.
            lifecycle = EngineLifecycle.install(NAME, System.out::println);
        } catch (final RuntimeException e) {
            // Nothing has started yet (no pool, no driver, no hook): print and exit 1, never a silent default.
            System.err.println("[PROJECTOR] FATAL (exit " + EngineLifecycle.EXIT_FATAL + "): start refused: " + e);
            e.printStackTrace();
            System.exit(EngineLifecycle.EXIT_FATAL);
            return;
        }
        try {
            run(lifecycle);
        } catch (final Throwable t) {
            // A refused setting, no active external driver within the wait, an unreachable Postgres: exit 1.
            lifecycle.startupFailed(t);
        }
    }

    private static void run(final EngineLifecycle lifecycle) {
        final ProjectorConfig config = ProjectorConfig.fromEnv();
        final ProjectorDriver driverSettings = ProjectorDriver.fromEnv();
        final boolean external = driverSettings.mode == ProjectorDriver.Mode.EXTERNAL;
        System.out.println("[PROJECTOR] driver mode " + driverSettings);
        if (external) {
            driverSettings.awaitExternalDriver(lifecycle::stopRequested);
            System.out.println("[PROJECTOR] external media driver active at " + driverSettings.aeronDir);
        }
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
        MediaDriver driver = null;
        ProjectorMetricsServer metricsServer = null;
        Thread thread = null;
        ProjectorAgent agent = null;
        try {
            final PostgresMoneyJournalStore store = new PostgresMoneyJournalStore(dataSource);

            // Embedded: the projector's own driver, closed below. External: none; the aeronmd is not ours to close,
            // and each epoch's archive client connects to it at AERON_DIR.
            driver = external ? null : MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName("/dev/shm/aeron-projector-" + ProcessHandle.current().pid())
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            final String aeronDirectoryName = external ? driverSettings.aeronDir : driver.aeronDirectoryName();

            // The archive clients' errors go to the lifecycle, as the engines' do. With an external
            // driver, driver loss halts at once with 1 (143 when a SIGTERM stop is in progress), on the client's own
            // thread before it unmaps the CnC; Aeron's default handler would print "exiting client" and
            // Runtime.exit(-1) (status 255) through the shutdown path, the race behind the ME SIGSEGV.
            final ErrorHandler aeronErrors = t -> {
                System.err.println("[PROJECTOR] Aeron error: " + t);
                lifecycle.onError(t, external);
            };
            agent = new ProjectorAgent(config, aeronDirectoryName, state, store, aeronErrors);
            thread = new Thread(agent, "money-projector");
            thread.setDaemon(false);
            thread.start();

            metricsServer = new ProjectorMetricsServer(state);
            try {
                metricsServer.start(config.metricsPort);
            } catch (IOException e) {
                throw new IllegalStateException("failed to start projector metrics server on port "
                        + config.metricsPort, e);
            }

            // Park until a stop cause: SIGTERM (the lifecycle's JVM hook), a fatal fault. A projector halted on a
            // gap keeps serving /health (halted=true). No agrona ShutdownSignalBarrier (its static hook
            // waited for a close() nobody called, docker killed the JVM at 10 s, 137).
            lifecycle.awaitStop();
        } finally {
            close(agent, thread, metricsServer, driver, dataSource);
        }
        // SIGTERM: returns, and the JVM exits with the signal's status (143). A fatal fault: halts with its status.
        lifecycle.finish();
    }

    private static void close(final ProjectorAgent agent, final Thread thread, final ProjectorMetricsServer metricsServer,
                              final MediaDriver driver, final HikariDataSource dataSource) {
        if (agent != null) {
            agent.stop();
            // Wakes the agent out of a blocking archive connect (AeronArchive.connect waits up to its message
            // timeout per endpoint and honours only an interrupt) or a backoff sleep. A flush cut short rolls
            // back; the projector resumes from the Postgres high-water (idempotent ON CONFLICT).
            thread.interrupt();
            try {
                thread.join(AGENT_JOIN_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (metricsServer != null) {
            metricsServer.stop();
        }
        CloseHelper.quietClose(driver);
        dataSource.close();
    }

    private ProjectorMain() {
    }
}
