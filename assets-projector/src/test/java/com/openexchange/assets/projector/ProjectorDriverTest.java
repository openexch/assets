// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S17 W10: {@link ProjectorDriver} parsing is strict; the external wait is bounded. */
public class ProjectorDriverTest {

    @Test
    public void unsetOrEmptyModeIsEmbedded() {
        assertEquals(ProjectorDriver.Mode.EMBEDDED, ProjectorDriver.parseMode(null));
        assertEquals(ProjectorDriver.Mode.EMBEDDED, ProjectorDriver.parseMode(""));
        final ProjectorDriver d = ProjectorDriver.parse(null, null, 30_000);
        assertEquals(ProjectorDriver.Mode.EMBEDDED, d.mode);
        assertNull(d.aeronDir);
    }

    @Test
    public void modesParseCaseInsensitivelyLikeTheEngines() {
        assertEquals(ProjectorDriver.Mode.EXTERNAL, ProjectorDriver.parseMode("external"));
        assertEquals(ProjectorDriver.Mode.EXTERNAL, ProjectorDriver.parseMode(" EXTERNAL "));
        assertEquals(ProjectorDriver.Mode.EMBEDDED, ProjectorDriver.parseMode("Embedded"));
    }

    @Test
    public void anUnknownModeIsAStartupErrorNeverADefault() {
        for (final String bad : new String[] {"extern", "ext", "shared", "true", " ", "embedded,external"}) {
            final IllegalArgumentException e = assertThrows(bad, IllegalArgumentException.class,
                    () -> ProjectorDriver.parseMode(bad));
            assertTrue(e.getMessage(), e.getMessage().contains("PROJECTOR_DRIVER_MODE")
                    && e.getMessage().contains("expected external|embedded"));
        }
    }

    @Test
    public void externalNeedsAeronDir() {
        for (final String dir : new String[] {null, "", "  "}) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ProjectorDriver.parse("external", dir, 30_000));
            assertTrue(e.getMessage(), e.getMessage().contains("needs AERON_DIR"));
        }
        final ProjectorDriver d = ProjectorDriver.parse("external", "/dev/shm/aeron-projector", 7_000);
        assertEquals(ProjectorDriver.Mode.EXTERNAL, d.mode);
        assertEquals("/dev/shm/aeron-projector", d.aeronDir);
        assertEquals(7_000, d.driverWaitMs);
    }

    @Test
    public void aeronDirInEmbeddedModeIsRefusedNotIgnored() {
        for (final String mode : new String[] {null, "", "embedded"}) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ProjectorDriver.parse(mode, "/dev/shm/aeron-x", 30_000));
            assertTrue(e.getMessage(), e.getMessage().contains("is set but PROJECTOR_DRIVER_MODE is embedded"));
        }
    }

    @Test
    public void theExternalWaitIsBoundedOnAnEmptyDir() throws Exception {
        final Path dir = Files.createTempDirectory("projector-w10-wait-");
        try {
            final ProjectorDriver d = ProjectorDriver.parse("external", dir.toString(), 300);
            final long t0 = System.nanoTime();
            final IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> d.awaitExternalDriver(() -> false));
            final long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(e.getMessage(), e.getMessage().startsWith("No external media driver found at " + dir)
                    && e.getMessage().contains("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS"));
            assertTrue("bounded by the wait: " + ms, ms >= 300 && ms < 3_000);
        } finally {
            Files.delete(dir);
        }
    }

    @Test
    public void aStopRequestEndsTheExternalWaitAtOnce() throws Exception {
        final Path dir = Files.createTempDirectory("projector-w10-stop-");
        try {
            final ProjectorDriver d = ProjectorDriver.parse("external", dir.toString(), 30_000);
            final long t0 = System.nanoTime();
            final IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> d.awaitExternalDriver(() -> true));
            assertTrue(e.getMessage(), e.getMessage().startsWith("stop requested while waiting"));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 1_000);
        } finally {
            Files.delete(dir);
        }
    }

    @Test
    public void embeddedHasNoExternalWait() {
        assertThrows(IllegalStateException.class,
                () -> ProjectorDriver.parse(null, null, 1).awaitExternalDriver(() -> false));
    }
}
