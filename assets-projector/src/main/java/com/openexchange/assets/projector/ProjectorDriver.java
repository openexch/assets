// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.openexchange.cluster.EngineLifecycle;
import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.exceptions.DriverTimeoutException;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Which media driver the money projector runs on (RC2 S17 W10, PO-11 (a)), set like the engines' driver mode
 * ({@code TRANSPORT_DRIVER_MODE}, assets {@code TransportConfig.driverMode}) and with the engines' driver wait
 * ({@link EngineLifecycle#driverWaitMs()}, {@code TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS}, default 30 s):
 * <ul>
 *   <li>{@code PROJECTOR_DRIVER_MODE=embedded} (the default, dev/demo): the projector launches its own driver on
 *       {@code /dev/shm/aeron-projector-<pid>}, as before W10.</li>
 *   <li>{@code PROJECTOR_DRIVER_MODE=external} (production): the projector attaches to the aeronmd at
 *       {@code AERON_DIR} and launches none. The start waits at most the driver wait for an ACTIVE driver there
 *       (its cnc.dat exists and its heartbeat is fresh), else the process exits 1.</li>
 * </ul>
 * Parsing is strict: an unknown mode, external without {@code AERON_DIR}, or {@code AERON_DIR} set in embedded mode
 * (an operator who set it expects the external driver; embedded would silently ignore it) refuses the start.
 */
public final class ProjectorDriver {

    public static final String MODE_ENV = "PROJECTOR_DRIVER_MODE";
    public static final String AERON_DIR_ENV = "AERON_DIR";

    public enum Mode { EXTERNAL, EMBEDDED }

    public final Mode mode;
    /** The external driver's directory; null in embedded mode (the embedded driver's dir is per pid). */
    public final String aeronDir;
    /** External mode: the bound on the start's wait for an active driver. Parsed (and validated) in every mode. */
    public final long driverWaitMs;

    private ProjectorDriver(final Mode mode, final String aeronDir, final long driverWaitMs) {
        this.mode = mode;
        this.aeronDir = aeronDir;
        this.driverWaitMs = driverWaitMs;
    }

    public static ProjectorDriver fromEnv() {
        return parse(System.getenv(MODE_ENV), System.getenv(AERON_DIR_ENV), EngineLifecycle.driverWaitMs());
    }

    static ProjectorDriver parse(final String modeValue, final String aeronDirValue, final long driverWaitMs) {
        final Mode mode = parseMode(modeValue);
        final boolean dirSet = aeronDirValue != null && !aeronDirValue.isEmpty();
        if (mode == Mode.EXTERNAL) {
            if (!dirSet || aeronDirValue.isBlank()) {
                throw new IllegalArgumentException(MODE_ENV + "=external needs " + AERON_DIR_ENV
                        + " (the external media driver's directory)");
            }
            return new ProjectorDriver(mode, aeronDirValue.trim(), driverWaitMs);
        }
        if (dirSet) {
            throw new IllegalArgumentException(AERON_DIR_ENV + "='" + aeronDirValue + "' is set but " + MODE_ENV
                    + " is embedded (the embedded driver never uses it): set " + MODE_ENV + "=external or unset "
                    + AERON_DIR_ENV);
        }
        return new ProjectorDriver(mode, null, driverWaitMs);
    }

    /** Unset or empty is the default (embedded); anything else must name a mode, case-insensitive. */
    static Mode parseMode(final String value) {
        if (value == null || value.isEmpty()) {
            return Mode.EMBEDDED;
        }
        try {
            return Mode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid " + MODE_ENV + " '" + value + "' (expected external|embedded)");
        }
    }

    /**
     * External mode: wait at most {@link #driverWaitMs} for an active driver at {@link #aeronDir}; throws on timeout.
     * A cnc.dat alone is not enough: a driver killed without its shutdown leaves one behind, and the projector's
     * per-epoch reconnects would then retry against it forever. A CnC version this client cannot use throws at
     * once (Aeron's own check). A stop requested meanwhile (SIGTERM through the lifecycle) ends the wait at once.
     */
    public void awaitExternalDriver(final BooleanSupplier stopRequested) {
        if (mode != Mode.EXTERNAL) {
            throw new IllegalStateException("no external driver in " + mode + " mode");
        }
        final File dir = new File(aeronDir);
        final long heartbeatTimeoutMs = new CommonContext().driverTimeoutMs();
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(driverWaitMs);
        while (true) {
            if (stopRequested.getAsBoolean()) {
                throw new IllegalStateException("stop requested while waiting for the external media driver at "
                        + aeronDir);
            }
            try {
                if (CommonContext.isDriverActive(dir, heartbeatTimeoutMs, ignored -> { })) {
                    return;
                }
            } catch (final DriverTimeoutException e) {
                // cnc.dat created but not yet initialised: the driver is still starting, keep waiting.
            }
            if (System.nanoTime() - deadlineNs > 0) {
                final boolean cnc = new File(dir, CncFileDescriptor.CNC_FILE).exists();
                throw new IllegalStateException("No external media driver found at " + aeronDir + " after "
                        + driverWaitMs + "ms (" + EngineLifecycle.DRIVER_WAIT_ENV + ")"
                        + (cnc ? ": a cnc.dat exists but its driver is not active" : ""));
            }
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for the external media driver at " + aeronDir, e);
            }
        }
    }

    @Override
    public String toString() {
        return mode == Mode.EXTERNAL ? "EXTERNAL (" + AERON_DIR_ENV + "=" + aeronDir + ", wait " + driverWaitMs + " ms)"
                : "EMBEDDED";
    }
}
