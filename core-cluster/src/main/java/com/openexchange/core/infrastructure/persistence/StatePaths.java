// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

/**
 * The tmpfs startup warning's predicate, shared by the member entry ({@code AeronCluster}) and the replica settings.
 * Its own class so the replica's closure does not reach the member stack ({@code AeronCluster} launches the driver,
 * the consensus module and the service; the replica closure scan).
 */
public final class StatePaths {

    private StatePaths() {
    }

    /**
     * True if {@code path} is (or is nested under) a well-known ephemeral/tmpfs mount ({@code /tmp} or
     * {@code /dev/shm}). Matches on path-segment boundaries, so a sibling like {@code /tmpfoo} does
     * <b>not</b> false-positive against {@code /tmp}. Pure and static so it is unit-testable without
     * booting a node. Relative paths always return {@code false} (nothing to warn about until they are
     * resolved against a cwd: callers should pass an absolute path, e.g. {@code File#getAbsolutePath()}).
     */
    public static boolean looksEphemeral(final String path) {
        if (null == path || path.isEmpty()) {
            return false;
        }
        String normalized = path;
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return "/tmp".equals(normalized) || normalized.startsWith("/tmp/")
                || "/dev/shm".equals(normalized) || normalized.startsWith("/dev/shm/");
    }
}
