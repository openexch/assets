// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SocketBufferProbe;
import com.openexchange.core.infrastructure.TransportConfig;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The log channel's transport settings and the kernel probe of its socket buffers (guard row 6): the URI carries
 * {@code term-length}, {@code rcv-wnd}, {@code so-rcvbuf} and {@code so-sndbuf}; the refusals at boot; the probe with a
 * fake kernel (the refusal text names the sysctl, the current and the needed value) and with the real kernel of this
 * host; {@code CORE_DRIVER_DIR_ACTIVE_MS} at its bounds. The control switches the probe off and must boot a capped
 * buffer silently.
 */
public class TransportLogChannelTest {

    private static Function<String, String> env(final String... pairs) {
        final Map<String, String> m = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put(pairs[i], pairs[i + 1]);
        }
        return m::get;
    }

    /** A kernel that grants {@code min(request, max)} for both buffers (the JDK's view on Linux). */
    private record FakeKernel(long rmemMax, long wmemMax) implements SocketBufferProbe.Kernel {
        @Override
        public long grantedRcvbuf(final long requested) {
            return Math.min(requested, rmemMax);
        }

        @Override
        public long grantedSndbuf(final long requested) {
            return Math.min(requested, wmemMax);
        }

        @Override
        public String sysctl(final String name) {
            return Long.toString(SocketBufferProbe.RMEM_MAX.equals(name) ? rmemMax : wmemMax);
        }
    }

    @Test
    public void theLogChannelUriCarriesTheWindowAndBothSocketBuffers() {
        final TransportConfig.LogTransport d = TransportConfig.LogTransport.parse(env());
        assertTrue(d.uri(), d.uri().contains("rcv-wnd=16m"));
        assertEquals("aeron:udp?term-length=64m|rcv-wnd=16m|so-rcvbuf=16m|so-sndbuf=16m", d.uri());
        assertEquals(16L * 1024 * 1024, d.receiverWindow());
        final TransportConfig.LogTransport small = TransportConfig.LogTransport.parse(env(
                "TRANSPORT_LOG_TERM_LENGTH", "4m", "TRANSPORT_LOG_RCV_WINDOW", "2m", "TRANSPORT_LOG_SO_RCVBUF", "2m",
                "TRANSPORT_LOG_SO_SNDBUF", "4m"));
        assertEquals("aeron:udp?term-length=4m|rcv-wnd=2m|so-rcvbuf=2m|so-sndbuf=4m", small.uri());
        // the producer's W read back from the URI the member runs with
        assertEquals(2L * 1024 * 1024, CoreClusteredService.logReceiverWindow(small.uri()));
        assertEquals(512L * 1024, CoreClusteredService.logReceiverWindow("aeron:udp?term-length=1m"));
    }

    @Test
    public void windowAndBufferRefusals() {
        assertEquals("TRANSPORT_LOG_RCV_WINDOW 64m above TRANSPORT_LOG_TERM_LENGTH/2 (64m/2): Aeron would clamp it"
                + " silently", assertThrows(IllegalArgumentException.class, () -> TransportConfig.LogTransport.parse(
                env("TRANSPORT_LOG_RCV_WINDOW", "64m", "TRANSPORT_LOG_SO_RCVBUF", "64m"))).getMessage());
        // term/2 exactly is accepted
        assertEquals(32L * 1024 * 1024, TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_RCV_WINDOW", "32m",
                "TRANSPORT_LOG_SO_RCVBUF", "32m")).rcvWindow());
        assertEquals("TRANSPORT_LOG_SO_RCVBUF 8m below TRANSPORT_LOG_RCV_WINDOW 16m: Aeron refuses this inside the"
                + " follower's election (SubscriptionParams); set so-rcvbuf >= rcv-wnd", assertThrows(
                IllegalArgumentException.class, () -> TransportConfig.LogTransport.parse(env(
                        "TRANSPORT_LOG_SO_RCVBUF", "8m"))).getMessage());
        assertEquals("TRANSPORT_LOG_RCV_WINDOW 32k below 64k (65536 B)", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_RCV_WINDOW", "32k"))).getMessage());
        assertEquals(64L * 1024, TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_RCV_WINDOW", "64k"))
                .rcvWindow());
        assertEquals("TRANSPORT_LOG_SO_SNDBUF 32k outside [64k, 1g]", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_SO_SNDBUF", "32k"))).getMessage());
        assertEquals("TRANSPORT_LOG_SO_SNDBUF 2g outside [64k, 1g]", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_SO_SNDBUF", "2g"))).getMessage());
        assertEquals(1024L * 1024 * 1024, TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_SO_SNDBUF", "1g"))
                .soSndbuf());
        assertEquals("TRANSPORT_LOG_SO_RCVBUF '16 MB' is not a size (expected <n>, <n>k, <n>m or <n>g)", assertThrows(
                IllegalArgumentException.class, () -> TransportConfig.LogTransport.parse(env(
                        "TRANSPORT_LOG_SO_RCVBUF", "16 MB"))).getMessage());
    }

    /**
     * An unset window derives {@code min(16m, term / 2)}: a 16m term (the dev-laptop and dev-box profiles, which set
     * no window) boots with 8m. Before the derivation the unset window was a fixed 16m and refused every 16m term.
     */
    @Test
    public void aSixteenMegTermWithNoWindowBoots() {
        final TransportConfig.LogTransport t = TransportConfig.LogTransport.parse(env(
                "TRANSPORT_LOG_TERM_LENGTH", "16m"));
        assertEquals(8L * 1024 * 1024, t.rcvWindow());
        assertEquals("8m", t.rcvWindowText());
        assertEquals("aeron:udp?term-length=16m|rcv-wnd=8m|so-rcvbuf=16m|so-sndbuf=16m", t.uri());
        // a blank value is unset
        assertEquals(8L * 1024 * 1024, TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_TERM_LENGTH", "16m",
                "TRANSPORT_LOG_RCV_WINDOW", " ")).rcvWindow());
    }

    /** The fixture's members run a 1m term: half is 512k, rendered as Aeron accepts it. */
    @Test
    public void aOneMegTermDerivesHalf() {
        final TransportConfig.LogTransport t = TransportConfig.LogTransport.parse(env(
                "TRANSPORT_LOG_TERM_LENGTH", "1m"));
        assertEquals(512L * 1024, t.rcvWindow());
        assertEquals("512k", t.rcvWindowText());
        assertEquals("aeron:udp?term-length=1m|rcv-wnd=512k|so-rcvbuf=16m|so-sndbuf=16m", t.uri());
        // 32m and 64m terms derive the 16m cap, the value they had before the derivation
        assertEquals(16L * 1024 * 1024, TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_TERM_LENGTH", "32m"))
                .rcvWindow());
        assertEquals("16m", TransportConfig.LogTransport.parse(env()).rcvWindowText());
        // half a size that is not a multiple of 2k renders in bytes (the driver refuses such a term later; the
        // rendering stays exact)
        final TransportConfig.LogTransport odd = TransportConfig.LogTransport.parse(env(
                "TRANSPORT_LOG_TERM_LENGTH", "200001"));
        assertEquals(100_000L, odd.rcvWindow());
        assertEquals("100000", odd.rcvWindowText());
        // a term below 128k cannot derive a legal window
        assertEquals("TRANSPORT_LOG_TERM_LENGTH 64k too small for a 64k receive window", assertThrows(
                IllegalArgumentException.class, () -> TransportConfig.LogTransport.parse(env(
                        "TRANSPORT_LOG_TERM_LENGTH", "64k"))).getMessage());
        assertEquals(64L * 1024, TransportConfig.LogTransport.parse(env("TRANSPORT_LOG_TERM_LENGTH", "128k"))
                .rcvWindow());
        // an explicit value is taken as given
        final TransportConfig.LogTransport set = TransportConfig.LogTransport.parse(env(
                "TRANSPORT_LOG_TERM_LENGTH", "1m", "TRANSPORT_LOG_RCV_WINDOW", "256k"));
        assertEquals(256L * 1024, set.rcvWindow());
        assertEquals("256k", set.rcvWindowText());
    }

    /** Control: an explicit value above term / 2 still refuses (passes before and after the derivation). */
    @Test
    public void anExplicitSixteenMegWindowOnASixteenMegTermStillRefuses() {
        assertEquals("TRANSPORT_LOG_RCV_WINDOW 16m above TRANSPORT_LOG_TERM_LENGTH/2 (16m/2): Aeron would clamp it"
                + " silently", assertThrows(IllegalArgumentException.class, () -> TransportConfig.LogTransport.parse(
                env("TRANSPORT_LOG_TERM_LENGTH", "16m", "TRANSPORT_LOG_RCV_WINDOW", "16m"))).getMessage());
    }

    /** The boot guard's line names the derived window and the rule that derived it. */
    @Test
    public void theBootLineNamesTheDerivedWindow() {
        final PrintStream before = System.out;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        final StubModeBootGuard.Transport t;
        try {
            t = StubModeBootGuard.enforceTransport(env("TRANSPORT_LOG_TERM_LENGTH", "16m"),
                    new FakeKernel(16_777_216, 16_777_216));
        } finally {
            System.setOut(before);
        }
        assertEquals(8L * 1024 * 1024, t.log().rcvWindow());
        final String out = captured.toString(StandardCharsets.UTF_8);
        before.print(out);
        assertTrue(out, out.startsWith("[BOOT-GUARD] log channel aeron:udp?term-length=16m|rcv-wnd=8m|so-rcvbuf=16m"
                + "|so-sndbuf=16m rcv-wnd 8m derived (min(16m, term/2) of 16m): "));
        // an explicit window is logged as set, never as derived
        final ByteArrayOutputStream explicit = new ByteArrayOutputStream();
        System.setOut(new PrintStream(explicit, true, StandardCharsets.UTF_8));
        try {
            StubModeBootGuard.enforceTransport(env("TRANSPORT_LOG_TERM_LENGTH", "1m", "TRANSPORT_LOG_RCV_WINDOW",
                    "256k"), new FakeKernel(16_777_216, 16_777_216));
        } finally {
            System.setOut(before);
        }
        final String set = explicit.toString(StandardCharsets.UTF_8);
        before.print(set);
        assertTrue(set, set.startsWith("[BOOT-GUARD] log channel aeron:udp?term-length=1m|rcv-wnd=256k|so-rcvbuf=16m"
                + "|so-sndbuf=16m rcv-wnd 256k set: "));
    }

    /**
     * {@code ARCHIVE_SEGMENT_LENGTH}: unset is max(64m, log term); an explicit value is a power of two in [64k, 1g] and
     * at least the log term; the archive context the member builds carries it; the boot guard refuses with its prefix
     * and logs the length next to the log channel line.
     */
    @Test
    public void archiveSegmentLengthDefaultsRangesAndTheArchiveContext() {
        final TransportConfig.ArchiveSegment dflt = TransportConfig.ArchiveSegment.parse(env());
        assertEquals(64L * 1024 * 1024, dflt.length());
        assertEquals("archive segment 64m set", dflt.line("64m"));
        assertEquals(64L * 1024 * 1024, TransportConfig.ArchiveSegment.parse(env("TRANSPORT_LOG_TERM_LENGTH", "1m"))
                .length());
        final TransportConfig.ArchiveSegment big = TransportConfig.ArchiveSegment.parse(env(
                "TRANSPORT_LOG_TERM_LENGTH", "128m"));
        assertEquals(128L * 1024 * 1024, big.length());
        assertEquals("archive segment 128m derived (max(64m, term) of 128m)", big.line("128m"));
        assertEquals(1024L * 1024, TransportConfig.ArchiveSegment.parse(env("TRANSPORT_LOG_TERM_LENGTH", "1m",
                "ARCHIVE_SEGMENT_LENGTH", "1m")).length());
        assertEquals(64L * 1024, TransportConfig.ArchiveSegment.parse(env("TRANSPORT_LOG_TERM_LENGTH", "64k",
                "ARCHIVE_SEGMENT_LENGTH", "64k")).length());
        assertEquals(1024L * 1024 * 1024, TransportConfig.ArchiveSegment.parse(env("ARCHIVE_SEGMENT_LENGTH", "1g"))
                .length());
        assertEquals(64L * 1024 * 1024, TransportConfig.ArchiveSegment.parse(env("ARCHIVE_SEGMENT_LENGTH", " "))
                .length());
        assertEquals("ARCHIVE_SEGMENT_LENGTH 32k outside [64k, 1g]", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.ArchiveSegment.parse(env("TRANSPORT_LOG_TERM_LENGTH", "64k",
                        "ARCHIVE_SEGMENT_LENGTH", "32k"))).getMessage());
        assertEquals("ARCHIVE_SEGMENT_LENGTH 2g outside [64k, 1g]", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.ArchiveSegment.parse(env("ARCHIVE_SEGMENT_LENGTH", "2g"))).getMessage());
        assertEquals("ARCHIVE_SEGMENT_LENGTH 3m is not a power of two", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.ArchiveSegment.parse(env("TRANSPORT_LOG_TERM_LENGTH", "1m",
                        "ARCHIVE_SEGMENT_LENGTH", "3m"))).getMessage());
        assertEquals("ARCHIVE_SEGMENT_LENGTH '1 MB' is not a size (expected <n>, <n>k, <n>m or <n>g)", assertThrows(
                IllegalArgumentException.class, () -> TransportConfig.ArchiveSegment.parse(env(
                        "ARCHIVE_SEGMENT_LENGTH", "1 MB"))).getMessage());
        assertEquals("ARCHIVE_SEGMENT_LENGTH 1m below TRANSPORT_LOG_TERM_LENGTH 64m: Aeron would raise the log's"
                + " segments to the term silently", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.ArchiveSegment.parse(env("ARCHIVE_SEGMENT_LENGTH", "1m"))).getMessage());

        // the boot guard: refusal with the prefix, the line next to the log channel line
        assertEquals("CORE BOOT GUARD: ARCHIVE_SEGMENT_LENGTH 3m is not a power of two", assertThrows(
                StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforceTransport(env(
                        "TRANSPORT_LOG_TERM_LENGTH", "1m", "ARCHIVE_SEGMENT_LENGTH", "3m"),
                        new FakeKernel(1L << 30, 1L << 30))).getMessage());
        final PrintStream before = System.out;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        final StubModeBootGuard.Transport t;
        try {
            t = StubModeBootGuard.enforceTransport(env("TRANSPORT_LOG_TERM_LENGTH", "1m", "ARCHIVE_SEGMENT_LENGTH",
                    "1m"), new FakeKernel(16_777_216, 16_777_216));
        } finally {
            System.setOut(before);
        }
        final String out = captured.toString(StandardCharsets.UTF_8);
        before.print(out);
        assertEquals(1024L * 1024, t.archiveSegment().length());
        final String[] lines = out.split("\n");
        assertTrue(out, lines.length == 2 && lines[0].startsWith("[BOOT-GUARD] log channel ")
                && lines[1].equals("[BOOT-GUARD] archive segment 1m set (1048576 B; a log purge removes whole segments)"));

        // the archive context the member builds reads the setting (the property form of the env name)
        final String prop = "archive.segment.length";
        final String old = System.getProperty(prop);
        final String oldTerm = System.getProperty("transport.log.term.length");
        try {
            System.setProperty(prop, "1m");
            System.setProperty("transport.log.term.length", "1m");
            assertEquals(1024 * 1024, ClusterConfig.create(0, java.util.List.of("localhost"), 9300,
                    new io.aeron.cluster.service.ClusteredService() {
                        @Override
                        public void onStart(final io.aeron.cluster.service.Cluster c, final io.aeron.Image i) {
                        }

                        @Override
                        public void onSessionOpen(final io.aeron.cluster.service.ClientSession s, final long t) {
                        }

                        @Override
                        public void onSessionClose(final io.aeron.cluster.service.ClientSession s, final long t,
                                                   final io.aeron.cluster.codecs.CloseReason r) {
                        }

                        @Override
                        public void onSessionMessage(final io.aeron.cluster.service.ClientSession s, final long t,
                                                     final org.agrona.DirectBuffer b, final int o, final int l,
                                                     final io.aeron.logbuffer.Header h) {
                        }

                        @Override
                        public void onTimerEvent(final long c, final long t) {
                        }

                        @Override
                        public void onTakeSnapshot(final io.aeron.ExclusivePublication p) {
                        }

                        @Override
                        public void onRoleChange(final io.aeron.cluster.service.Cluster.Role r) {
                        }

                        @Override
                        public void onTerminate(final io.aeron.cluster.service.Cluster c) {
                        }
                    }).archiveContext().segmentFileLength());
        } finally {
            if (old == null) {
                System.clearProperty(prop);
            } else {
                System.setProperty(prop, old);
            }
            if (oldTerm == null) {
                System.clearProperty("transport.log.term.length");
            } else {
                System.setProperty("transport.log.term.length", oldTerm);
            }
        }
    }

    @Test
    public void driverDirBoundAtItsLimits() {
        assertEquals(10_000L, TransportConfig.driverDirActiveMs(env()));
        assertEquals(2_000L, TransportConfig.driverDirActiveMs(env("CORE_DRIVER_DIR_ACTIVE_MS", "2000")));
        assertEquals(600_000L, TransportConfig.driverDirActiveMs(env("CORE_DRIVER_DIR_ACTIVE_MS", "600000")));
        assertEquals("CORE_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 1999", assertThrows(
                IllegalArgumentException.class, () -> TransportConfig.driverDirActiveMs(env(
                        "CORE_DRIVER_DIR_ACTIVE_MS", "1999"))).getMessage());
        assertEquals("CORE_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 600001", assertThrows(
                IllegalArgumentException.class, () -> TransportConfig.driverDirActiveMs(env(
                        "CORE_DRIVER_DIR_ACTIVE_MS", "600001"))).getMessage());
        assertEquals("CORE_DRIVER_DIR_ACTIVE_MS is not an integer: 10s", assertThrows(IllegalArgumentException.class,
                () -> TransportConfig.driverDirActiveMs(env("CORE_DRIVER_DIR_ACTIVE_MS", "10s"))).getMessage());
        // the boot guard refuses a bad bound with its prefix
        assertEquals("CORE BOOT GUARD: CORE_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 500", assertThrows(
                StandbySnapshotBootGuard.Refused.class, () -> StubModeBootGuard.enforceTransport(env(
                        "CORE_DRIVER_DIR_ACTIVE_MS", "500"), new FakeKernel(1L << 30, 1L << 30))).getMessage());
    }

    @Test
    public void theProbeRefusesABufferTheKernelDoesNotGrantInFull() {
        final StandbySnapshotBootGuard.Refused rcv = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StubModeBootGuard.enforceTransport(env(), new FakeKernel(4_194_304, 16_777_216)));
        assertEquals("CORE BOOT GUARD: TRANSPORT_LOG_SO_RCVBUF=16m (16777216 B) but the kernel granted 4194304 B"
                + " (net.core.rmem_max=4194304): set net.core.rmem_max >= 16777216 (sysctl -w"
                + " net.core.rmem_max=16777216, persist in /etc/sysctl.d) or lower TRANSPORT_LOG_SO_RCVBUF and"
                + " TRANSPORT_LOG_RCV_WINDOW", rcv.getMessage());
        final StandbySnapshotBootGuard.Refused snd = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StubModeBootGuard.enforceTransport(env(), new FakeKernel(16_777_216, 8_388_608)));
        assertEquals("CORE BOOT GUARD: TRANSPORT_LOG_SO_SNDBUF=16m (16777216 B) but the kernel granted 8388608 B"
                + " (net.core.wmem_max=8388608): set net.core.wmem_max >= 16777216 (sysctl -w"
                + " net.core.wmem_max=16777216, persist in /etc/sysctl.d) or lower TRANSPORT_LOG_SO_SNDBUF",
                snd.getMessage());
        // granted in full: the settings come back
        assertEquals(16L * 1024 * 1024, StubModeBootGuard.enforceTransport(env(), new FakeKernel(16_777_216,
                16_777_216)).log().soRcvbuf());
        // negative control: the probe off boots the capped buffer silently
        final boolean probeOff = Boolean.getBoolean("core.transport.probeOffControl");
        try {
            StubModeBootGuard.enforceTransport(env(), new FakeKernel(4_194_304, 16_777_216), probeOff);
        } catch (final StandbySnapshotBootGuard.Refused expected) {
            return;
        }
        throw new AssertionError("a capped socket buffer booted silently: rmem_max=4194304, request 16777216");
    }

    @Test
    public void theRealKernelOfThisHost() {
        final String rmem = SocketBufferProbe.SYSTEM.sysctl(SocketBufferProbe.RMEM_MAX);
        final String wmem = SocketBufferProbe.SYSTEM.sysctl(SocketBufferProbe.WMEM_MAX);
        System.out.println("[probe] net.core.rmem_max=" + rmem + " net.core.wmem_max=" + wmem);
        assumeTrue("needs net.core.rmem_max and wmem_max >= 16777216 (this host: " + rmem + ", " + wmem + ")",
                !"unknown".equals(rmem) && !"unknown".equals(wmem) && Long.parseLong(rmem) >= 16_777_216L
                        && Long.parseLong(wmem) >= 16_777_216L);
        final StubModeBootGuard.Transport t = StubModeBootGuard.enforceTransport(env(), SocketBufferProbe.SYSTEM);
        assertEquals(16L * 1024 * 1024, t.log().soRcvbuf());
        if (Long.parseLong(rmem) < 67_108_864L) {
            final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                    () -> StubModeBootGuard.enforceTransport(env("TRANSPORT_LOG_SO_RCVBUF", "64m"),
                            SocketBufferProbe.SYSTEM));
            assertTrue(r.getMessage(), r.getMessage().startsWith("CORE BOOT GUARD: TRANSPORT_LOG_SO_RCVBUF=64m (67108864"
                    + " B) but the kernel granted " + rmem + " B (net.core.rmem_max=" + rmem + "): set"
                    + " net.core.rmem_max >= 67108864"));
            assertFalse(r.getMessage().contains("unknown"));
        }
    }
}
