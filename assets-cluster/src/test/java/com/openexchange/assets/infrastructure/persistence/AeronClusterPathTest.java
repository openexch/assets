// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.junit.Test;

import static com.openexchange.assets.infrastructure.persistence.AeronCluster.pathLooksEphemeral;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AeronCluster#pathLooksEphemeral(String)}, the pure predicate that drives the
 * startup warning when the Assets Engine's cluster/archive state dir (the money ledger's snapshot +
 * log) resolves onto a tmpfs mount. Covers exact-mount, subdirectory, trailing-slash, and the
 * segment-boundary false-positive case ({@code /tmpfoo} must NOT match {@code /tmp}).
 */
public class AeronClusterPathTest {

    @Test
    public void devShmSubdirectoryIsEphemeral() {
        assertTrue(pathLooksEphemeral("/dev/shm/aeron-assets/ae0"));
    }

    @Test
    public void tmpSubdirectoryIsEphemeral() {
        assertTrue(pathLooksEphemeral("/tmp/foo"));
    }

    @Test
    public void bareTmpIsEphemeral() {
        assertTrue(pathLooksEphemeral("/tmp"));
    }

    @Test
    public void bareDevShmIsEphemeral() {
        assertTrue(pathLooksEphemeral("/dev/shm"));
    }

    @Test
    public void diskPathIsNotEphemeral() {
        assertFalse(pathLooksEphemeral("/data/assets/ae0"));
    }

    @Test
    public void tmpAsPrefixOfAnotherNameIsNotEphemeral() {
        // /tmpfoo shares the "/tmp" string prefix but is a different top-level dir entirely; matching
        // must be on path-segment boundaries, not raw string prefix.
        assertFalse(pathLooksEphemeral("/tmpfoo/bar"));
    }

    @Test
    public void devShmAsPrefixOfAnotherNameIsNotEphemeral() {
        assertFalse(pathLooksEphemeral("/dev/shmoo/bar"));
    }

    @Test
    public void relativePathIsNotEphemeral() {
        // Relative paths are not resolved here; callers pass an absolute path (see getBaseDir /
        // File#getAbsolutePath). A bare relative string never matches.
        assertFalse(pathLooksEphemeral("data/ae0"));
    }

    @Test
    public void trailingSlashIsNormalized() {
        assertTrue(pathLooksEphemeral("/tmp/"));
        assertTrue(pathLooksEphemeral("/dev/shm/"));
        assertTrue(pathLooksEphemeral("/tmp/foo/"));
    }

    @Test
    public void nullAndEmptyAreNotEphemeral() {
        assertFalse(pathLooksEphemeral(null));
        assertFalse(pathLooksEphemeral(""));
    }

    @Test
    public void rootIsNotEphemeral() {
        assertFalse(pathLooksEphemeral("/"));
    }
}
