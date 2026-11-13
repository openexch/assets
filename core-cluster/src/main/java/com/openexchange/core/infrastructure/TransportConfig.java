// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure;

import io.aeron.CommonContext;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.function.Supplier;

/**
 * Transport-layer configuration for the core's Aeron media driver and channels. Read
 * env-var-first with a system-property fallback.
 *
 * <p><b>Default idle mode is BACKOFF, not busy-spin</b> — the opposite of the matching engine's
 * default. The core co-resides with the live matching engine on one box; a busy-spin core would peg cores
 * and starve the money stack. Backoff parks when idle. A future dedicated deployment opts into
 * {@code TRANSPORT_IDLE_MODE=busy_spin}.</p>
 */
public final class TransportConfig {

    public enum DriverMode { EXTERNAL, EMBEDDED }

    public enum IdleMode { BUSY_SPIN, BACKOFF }

    private TransportConfig() {
    }

    /** Driver mode. Defaults to EMBEDDED (single-process dev convenience for the core). */
    public static DriverMode driverMode() {
        final String value = envOrProp("TRANSPORT_DRIVER_MODE", "transport.driver.mode", "embedded");
        try {
            return DriverMode.valueOf(value.trim().toUpperCase());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid TRANSPORT_DRIVER_MODE/transport.driver.mode '" + value + "' (expected external|embedded)");
        }
    }

    /** Idle profile. Defaults to BACKOFF so the core never busy-spins on a shared box. */
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

    /**
     * aeron.dir this process uses to reach its media driver (external mode / explicit override).
     * Term buffers here are ephemeral by design (transport-only state, not the money record); this
     * defaulting onto tmpfs is intentional and does not need the durability warning that the
     * cluster/archive state dir gets (see {@code AeronCluster.pathLooksEphemeral}).
     */
    public static String aeronDir(final int nodeId) {
        return envOrProp("AERON_DIR", "aeron.driver.dir",
                CommonContext.getAeronDirectoryName() + "-core-" + nodeId + "-driver");
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

    /**
     * Term length for the SNAPSHOT ipc channel. A snapshot publication maps a fresh
     * 3-term buffer per cycle (64m = 192MB in /dev/shm every snapshot), which on a
     * co-located box (two clusters sharing one tmpfs) exhausts the archive and fails
     * snapshots with StorageSpaceException, so nothing prunes and the log grows.
     * Small/co-located hosts set this down (snapshots are ~6MB); default 64m preserves prod.
     */
    public static String snapshotTermLength() {
        return envOrProp("TRANSPORT_SNAPSHOT_TERM_LENGTH", "transport.snapshot.term.length", "64m");
    }

    /** {@code TRANSPORT_LOG_TERM_LENGTH}. */
    public static final String LOG_TERM_LENGTH = "TRANSPORT_LOG_TERM_LENGTH";
    /** {@code TRANSPORT_LOG_RCV_WINDOW}: the log channel's {@code rcv-wnd}. */
    public static final String LOG_RCV_WINDOW = "TRANSPORT_LOG_RCV_WINDOW";
    /** {@code TRANSPORT_LOG_SO_RCVBUF}: the log channel's {@code so-rcvbuf}. */
    public static final String LOG_SO_RCVBUF = "TRANSPORT_LOG_SO_RCVBUF";
    /** {@code TRANSPORT_LOG_SO_SNDBUF}: the log channel's {@code so-sndbuf}. */
    public static final String LOG_SO_SNDBUF = "TRANSPORT_LOG_SO_SNDBUF";
    /** {@code CORE_DRIVER_DIR_ACTIVE_MS}: the embedded driver directory bound (external mode ignores it). */
    public static final String DRIVER_DIR_ACTIVE_MS = "CORE_DRIVER_DIR_ACTIVE_MS";
    /** The cap of the derived window: an unset window is {@code min(16m, term / 2)}. */
    public static final String DEFAULT_LOG_RCV_WINDOW = "16m";
    public static final long MAX_DERIVED_LOG_WINDOW = 16L * 1024 * 1024;
    public static final String DEFAULT_LOG_SO_BUF = "16m";
    public static final long MIN_LOG_WINDOW = 64 * 1024;
    public static final long MIN_LOG_SO_BUF = 64 * 1024;
    public static final long MAX_LOG_SO_BUF = 1024L * 1024 * 1024;

    /**
     * The log channel's transport settings, parsed and checked once at boot in every mode (the log channel is the same
     * channel in every snapshot mode). The window and both socket buffers are URI parameters of the log channel, so
     * they govern that channel under an embedded driver and under an external aeronmd alike (a URI value overrides the
     * driver context's default for that channel only); every other channel keeps the driver's own values.
     *
     * <p>An unset (or blank) {@code TRANSPORT_LOG_RCV_WINDOW} derives {@code min(16m, term-length / 2)}, rendered as
     * Aeron accepts it ({@code 8m}, {@code 512k}, or bytes) and named by {@link #windowLine()} in the boot guard's
     * line; an explicit value is taken as given and never changed.</p>
     *
     * <p>Refused at boot: a size that does not parse; {@code rcv-wnd} below 64k or above {@code term-length / 2}
     * (Aeron would clamp the window to half the term silently); a term below 128k when the window is derived (no legal
     * window); a socket buffer outside [64k, 1g]; {@code so-rcvbuf} below {@code rcv-wnd} (Aeron refuses that too, but
     * late, at the subscription inside the follower's election). The kernel's grant of the two buffers is a separate
     * boot probe ({@code SocketBufferProbe}).</p>
     */
    public record LogTransport(String termLengthText, long termLength, String rcvWindowText, long rcvWindow,
                               String soRcvbufText, long soRcvbuf, String soSndbufText, long soSndbuf,
                               boolean rcvWindowDerived) {

        /** The window as the boot line names it: {@code rcv-wnd <w> derived (min(16m, term/2) of <t>)} or {@code set}. */
        public String windowLine() {
            return rcvWindowDerived
                    ? "rcv-wnd " + rcvWindowText + " derived (min(16m, term/2) of " + termLengthText + ")"
                    : "rcv-wnd " + rcvWindowText + " set";
        }

        /** The log channel URI: {@code aeron:udp?term-length=<t>|rcv-wnd=<w>|so-rcvbuf=<r>|so-sndbuf=<s>}. */
        public String uri() {
            return "aeron:udp?term-length=" + termLengthText + "|rcv-wnd=" + rcvWindowText + "|so-rcvbuf="
                    + soRcvbufText + "|so-sndbuf=" + soSndbufText;
        }

        /** The receiver window the log image gets: {@code rcv-wnd} (already checked to be at most term / 2). */
        public long receiverWindow() {
            return rcvWindow;
        }

        /**
         * @param env the setting's raw text by its env name, null when unset
         * @throws IllegalArgumentException with the refusal text (no host prefix)
         */
        public static LogTransport parse(final java.util.function.Function<String, String> env) {
            final String term = text(env, LOG_TERM_LENGTH, "64m");
            final String rcv = text(env, LOG_SO_RCVBUF, DEFAULT_LOG_SO_BUF);
            final String snd = text(env, LOG_SO_SNDBUF, DEFAULT_LOG_SO_BUF);
            final long t = size(LOG_TERM_LENGTH, term);
            final boolean derived = text(env, LOG_RCV_WINDOW, null) == null;
            final String window;
            final long w;
            if (derived) {
                w = Math.min(MAX_DERIVED_LOG_WINDOW, t / 2);
                if (w < MIN_LOG_WINDOW) {
                    throw new IllegalArgumentException(LOG_TERM_LENGTH + " " + term + " too small for a 64k receive"
                            + " window");
                }
                window = sizeText(w);
            } else {
                window = text(env, LOG_RCV_WINDOW, null);
                w = size(LOG_RCV_WINDOW, window);
            }
            final long r = size(LOG_SO_RCVBUF, rcv);
            final long s = size(LOG_SO_SNDBUF, snd);
            if (w < MIN_LOG_WINDOW) {
                throw new IllegalArgumentException(LOG_RCV_WINDOW + " " + window + " below 64k (" + MIN_LOG_WINDOW
                        + " B)");
            }
            if (w > t / 2) {
                throw new IllegalArgumentException(LOG_RCV_WINDOW + " " + window + " above " + LOG_TERM_LENGTH + "/2 ("
                        + term + "/2): Aeron would clamp it silently");
            }
            bufferRange(LOG_SO_RCVBUF, rcv, r);
            bufferRange(LOG_SO_SNDBUF, snd, s);
            if (r < w) {
                throw new IllegalArgumentException(LOG_SO_RCVBUF + " " + rcv + " below " + LOG_RCV_WINDOW + " " + window
                        + ": Aeron refuses this inside the follower's election (SubscriptionParams); set so-rcvbuf >="
                        + " rcv-wnd");
            }
            return new LogTransport(term, t, window, w, rcv, r, snd, s, derived);
        }

        /** {@code bytes} as Aeron's size syntax accepts it: {@code <n>m}, {@code <n>k}, or plain bytes. */
        static String sizeText(final long bytes) {
            if (bytes % (1024L * 1024) == 0) {
                return bytes / (1024L * 1024) + "m";
            }
            if (bytes % 1024 == 0) {
                return bytes / 1024 + "k";
            }
            return Long.toString(bytes);
        }

        /** This process's settings (env first, then the system property, else the default). */
        public static LogTransport fromEnv() {
            return parse(TransportConfig::settingFromEnv);
        }

        private static String text(final java.util.function.Function<String, String> env, final String name,
                                   final String dflt) {
            final String v = env.apply(name);
            return v == null || v.isBlank() ? dflt : v.trim();
        }

        private static void bufferRange(final String name, final String text, final long v) {
            if (v < MIN_LOG_SO_BUF || v > MAX_LOG_SO_BUF) {
                throw new IllegalArgumentException(name + " " + text + " outside [64k, 1g]");
            }
        }
    }

    /** {@code ARCHIVE_SEGMENT_LENGTH}: the archive's segment file length, the granule a log purge removes. */
    public static final String ARCHIVE_SEGMENT_LENGTH = "ARCHIVE_SEGMENT_LENGTH";
    public static final String DEFAULT_ARCHIVE_SEGMENT_LENGTH = "64m";
    public static final long MIN_ARCHIVE_SEGMENT_LENGTH = 64 * 1024;
    public static final long MAX_ARCHIVE_SEGMENT_LENGTH = 1024L * 1024 * 1024;

    /**
     * The archive's segment file length. Every recording of the archive is cut into segment files of this length and a
     * log purge removes whole segments only, so the length is also how far behind the floor a log keeps its start.
     *
     * <p>An unset (or blank) value is {@code max(64m, term-length)}: 64m as before the setting existed, and the log
     * term when the term is longer, because Aeron raises the segment of a recording to its term length without a word.
     * An explicit value is taken as given and must be a power of two in [64k, 1g] (Aeron's own range) and at least the
     * log channel's term length (below it Aeron would raise the log's segments to the term silently). A recording whose
     * term is longer than the segment (a snapshot on a 64m IPC term, for example) still gets its term as segment; only
     * the log is checked, because the log is what a purge shortens.</p>
     */
    public record ArchiveSegment(String text, long length, boolean derived) {

        /** As the boot line names it: {@code archive segment <s> set} or {@code ... derived (max(64m, term) of <t>)}. */
        public String line(final String termText) {
            return derived
                    ? "archive segment " + text + " derived (max(64m, term) of " + termText + ")"
                    : "archive segment " + text + " set";
        }

        /**
         * @param env the setting's raw text by its env name, null when unset (reads {@code ARCHIVE_SEGMENT_LENGTH} and
         *            {@code TRANSPORT_LOG_TERM_LENGTH})
         * @throws IllegalArgumentException with the refusal text (no host prefix)
         */
        public static ArchiveSegment parse(final java.util.function.Function<String, String> env) {
            final String raw = env.apply(ARCHIVE_SEGMENT_LENGTH);
            final String termRaw = env.apply(LOG_TERM_LENGTH);
            final String term = termRaw == null || termRaw.isBlank() ? "64m" : termRaw.trim();
            final long t = size(LOG_TERM_LENGTH, term);
            if (raw == null || raw.isBlank()) {
                final long dflt = size(ARCHIVE_SEGMENT_LENGTH, DEFAULT_ARCHIVE_SEGMENT_LENGTH);
                return t > dflt
                        ? new ArchiveSegment(term, t, true)
                        : new ArchiveSegment(DEFAULT_ARCHIVE_SEGMENT_LENGTH, dflt, false);
            }
            final String text = raw.trim();
            final long s = size(ARCHIVE_SEGMENT_LENGTH, text);
            if (s < MIN_ARCHIVE_SEGMENT_LENGTH || s > MAX_ARCHIVE_SEGMENT_LENGTH) {
                throw new IllegalArgumentException(ARCHIVE_SEGMENT_LENGTH + " " + text + " outside [64k, 1g]");
            }
            if (Long.bitCount(s) != 1) {
                throw new IllegalArgumentException(ARCHIVE_SEGMENT_LENGTH + " " + text + " is not a power of two");
            }
            if (s < t) {
                throw new IllegalArgumentException(ARCHIVE_SEGMENT_LENGTH + " " + text + " below " + LOG_TERM_LENGTH
                        + " " + term + ": Aeron would raise the log's segments to the term silently");
            }
            return new ArchiveSegment(text, s, false);
        }

        /** This process's setting (env first, then {@code archive.segment.length}, else the default). */
        public static ArchiveSegment fromEnv() {
            return parse(TransportConfig::settingFromEnv);
        }

        /** The length as Aeron's {@code segmentFileLength(int)} takes it (at most 1g, so it fits). */
        public int lengthInt() {
            return Math.toIntExact(length);
        }
    }

    /**
     * {@code CORE_DRIVER_DIR_ACTIVE_MS} (default 10000, 2000..600000): a heartbeat at most this old in the embedded
     * driver's directory is a live driver and refuses the boot; an older one is stale and replaced.
     *
     * @throws IllegalArgumentException {@code CORE_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: <v>}
     */
    public static long driverDirActiveMs(final java.util.function.Function<String, String> env) {
        final String v = env.apply(DRIVER_DIR_ACTIVE_MS);
        if (v == null || v.isBlank()) {
            return com.openexchange.cluster.EmbeddedDriverDir.DEFAULT_ACTIVE_MS;
        }
        final long ms;
        try {
            ms = Long.parseLong(v.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(DRIVER_DIR_ACTIVE_MS + " is not an integer: " + v.trim());
        }
        final String refusal = com.openexchange.cluster.EmbeddedDriverDir.rangeRefusal(DRIVER_DIR_ACTIVE_MS, ms);
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
        return ms;
    }

    /** A size {@code <n>}, {@code <n>k}, {@code <n>m} or {@code <n>g}. */
    static long size(final String name, final String text) {
        try {
            return org.agrona.SystemUtil.parseSize(name, text);
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException(name + " '" + text + "' is not a size (expected <n>, <n>k, <n>m or <n>g)");
        }
    }

    /** A transport setting by its env name: env first, then {@code transport.<lower dotted>}, null when unset. */
    public static String settingFromEnv(final String envName) {
        final String prop = envName.toLowerCase(java.util.Locale.ROOT).replace('_', '.');
        final String v = envOrProp(envName, prop, null);
        return v == null || v.isEmpty() ? null : v;
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

    /** Env first (non-empty), then the system property, else {@code defaultValue}; settings read through it too. */
    public static String envOrProp(final String envName, final String propName, final String defaultValue) {
        final String env = System.getenv(envName);
        if (env != null && !env.isEmpty()) {
            return env;
        }
        return System.getProperty(propName, defaultValue);
    }
}
