// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.io.File;
import java.util.function.Supplier;

/**
 * Transport-layer configuration for the Assets Engine's Aeron media driver and channels. Read
 * env-var-first with a system-property fallback.
 *
 * <p><b>Default idle mode is BACKOFF, not busy-spin</b> — the opposite of the matching engine's
 * default. The AE co-resides with the live matching engine on one box; a busy-spin AE would peg cores
 * and starve the money stack. Backoff parks when idle. A future dedicated deployment opts into
 * {@code TRANSPORT_IDLE_MODE=busy_spin}.</p>
 */
public final class TransportConfig {

    private static final Logger log = Logger.getLogger(TransportConfig.class);

    public enum DriverMode { EXTERNAL, EMBEDDED }

    public enum IdleMode { BUSY_SPIN, BACKOFF }

    public static final long EXTERNAL_DRIVER_TIMEOUT_MS = 30_000;

    private TransportConfig() {
    }

    /** Driver mode. Defaults to EMBEDDED (single-process dev convenience for the AE). */
    public static DriverMode driverMode() {
        final String value = envOrProp("TRANSPORT_DRIVER_MODE", "transport.driver.mode", "embedded");
        try {
            return DriverMode.valueOf(value.trim().toUpperCase());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid TRANSPORT_DRIVER_MODE/transport.driver.mode '" + value + "' (expected external|embedded)");
        }
    }

    /** Idle profile. Defaults to BACKOFF so the AE never busy-spins on a shared box. */
    public static IdleMode idleMode() {
        final String value = envOrProp("TRANSPORT_IDLE_MODE", "transport.idle.mode", "backoff");
        try {
            return IdleMode.valueOf(value.trim().toUpperCase());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid TRANSPORT_IDLE_MODE/transport.idle.mode '" + value + "' (expected busy_spin|backoff)");
        }
    }

    /** Supplier of idle strategies matching {@link #idleMode()} (each Aeron agent needs its own). */
    public static Supplier<IdleStrategy> idleStrategySupplier() {
        return idleMode() == IdleMode.BUSY_SPIN
                ? BusySpinIdleStrategy::new
                : BackoffIdleStrategy::new;
    }

    /** aeron.dir this process uses to reach its media driver (external mode / explicit override). */
    public static String aeronDir(final int nodeId) {
        return envOrProp("AERON_DIR", "aeron.driver.dir",
                CommonContext.getAeronDirectoryName() + "-assets-" + nodeId + "-driver");
    }

    public static String networkInterface() {
        final String value = envOrProp("TRANSPORT_INTERFACE", "transport.interface", "");
        return value.isEmpty() ? null : value;
    }

    public static int mtuLength() {
        return Integer.parseInt(envOrProp("TRANSPORT_MTU", "transport.mtu", "8192"));
    }

    public static String termLength() {
        return envOrProp("TRANSPORT_TERM_LENGTH", "transport.term.length", "16m");
    }

    public static String logTermLength() {
        return envOrProp("TRANSPORT_LOG_TERM_LENGTH", "transport.log.term.length", "64m");
    }

    /** Build a UDP channel URI with the configured term-length, MTU and optional interface applied. */
    public static String udpChannel(final String base) {
        final StringBuilder sb = new StringBuilder(base);
        sb.append(base.indexOf('?') >= 0 ? '|' : '?');
        sb.append("term-length=").append(termLength());
        sb.append("|mtu=").append(mtuLength());
        final String iface = networkInterface();
        if (iface != null) {
            sb.append("|interface=").append(iface);
        }
        return sb.toString();
    }

    /** Block until an external media driver is up (its cnc.dat exists) or the timeout elapses. */
    public static void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
        final File cncFile = new File(aeronDir, CncFileDescriptor.CNC_FILE);
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cncFile.exists()) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException(
                        "No external media driver found at " + aeronDir + " after " + timeoutMs + "ms.");
            }
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for external media driver at " + aeronDir, e);
            }
        }
        log.info("External media driver detected at %s", aeronDir);
    }

    private static String envOrProp(final String envName, final String propName, final String defaultValue) {
        final String env = System.getenv(envName);
        if (env != null && !env.isEmpty()) {
            return env;
        }
        return System.getProperty(propName, defaultValue);
    }
}
