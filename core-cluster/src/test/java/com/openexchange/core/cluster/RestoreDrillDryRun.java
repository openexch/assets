// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Tee;

import java.util.List;

/**
 * The dry runs of 11.2, 11.4 and 11.11 execute the real drills on the base (84b088b, no
 * {@code SNAPSHOT_MODE}) so that rig, sizes and oracles are proven before the code exists, and they may fail only on the path
 * line. That one assertion is the only one expected to go RED until W2 (the production {@code [SNAPSHOT] restored
 * path=EQUAL|SKIP|TAIL} line does not exist yet). It is switched by {@code -Dsnapshot.expectPathLine=true}:
 * <ul>
 *   <li>absent (the default, so the regression suite stays GREEN while W1/W2 are written): the drill prints
 *       {@code [restore-dryrun] ... path line absent (expected RED until W2)} and carries on;</li>
 *   <li>true (W3/W4 switch it on when the production path line lands, and the W0 evidence run shows the RED once): a
 *       missing line fails the drill with {@code no restore path line; ...}.</li>
 * </ul>
 */
final class RestoreDrillDryRun {

    static final boolean EXPECT_PATH_LINE = Boolean.getBoolean("snapshot.expectPathLine");
    /** SNAPSHOT_RESTORE_READY_BUDGET_MS (3.5): the /ready budget the restore drills read. */
    static final long READY_BUDGET_MS = Long.getLong("snapshot.restore.ready.budget.ms", 30_000L);

    private RestoreDrillDryRun() {
    }

    /**
     * C6: an oracle that cannot fail is not one. Runs {@code body} (the same assertion on a wrong input); it must throw an
     * {@link AssertionError} whose message contains {@code named}. Prints {@code NC id -> message}.
     */
    static void nc(final String id, final String named, final Runnable body) {
        try {
            body.run();
        } catch (final AssertionError e) {
            final String msg = String.valueOf(e.getMessage());
            if (!msg.contains(named)) {
                throw new AssertionError("negative control " + id + " failed for the wrong reason: <" + msg + ">, expected <"
                        + named + ">", e);
            }
            System.out.println("NC " + id + " -> " + msg);
            return;
        }
        throw new AssertionError("negative control " + id + " did not fail: the oracle cannot see its rule");
    }

    /**
     * W3: the production drills (stub mode on, the code present) always require the path line; the property only
     * governs the off-mode dry runs above.
     */
    static void requirePathLine(final Tee tee, final String expectedPath, final String drill) {
        final List<String> lines = tee.all("restored path=");
        if (lines.stream().noneMatch(l -> l.contains("restored path=" + expectedPath))) {
            throw new AssertionError("no restore path line; expected `[SNAPSHOT] restored path=" + expectedPath
                    + "` in the " + drill + " drill, saw " + lines + " among " + tee.lines);
        }
    }

    /** {@code [SNAPSHOT] restored path=<expected>} among the lines recorded since the tee was last cleared. */
    static void pathLine(final Tee tee, final String expectedPath, final String drill) {
        final List<String> lines = tee.all("restored path=");
        final boolean seen = lines.stream().anyMatch(l -> l.contains("restored path=" + expectedPath));
        if (EXPECT_PATH_LINE) {
            if (!seen) {
                throw new AssertionError("no restore path line; expected `[SNAPSHOT] restored path=" + expectedPath
                        + "` in the " + drill + " drill, saw " + lines + " among " + tee.lines);
            }
        } else {
            System.out.println("[restore-dryrun] " + drill + ": expected `[SNAPSHOT] restored path=" + expectedPath + "`, "
                    + (seen ? "PRESENT" : "path line absent (expected RED until W2)"));
        }
    }
}
