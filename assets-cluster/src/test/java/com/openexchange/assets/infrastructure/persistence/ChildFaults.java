// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import java.util.ArrayList;
import java.util.List;

/**
 * RCS S4 11.14 and the 11.13 Q9 child leg: the {@code ShadowRestore.Faults} seams installed INSIDE a child engine JVM (the
 * production {@code Main} on an external media driver). The external-driver test calls these through JDWP at the
 * service's {@code onStart} breakpoint (the child's own service instance; the production entry point offers no other
 * handle). Test scope only; the production {@code Faults.NONE} is never touched.
 */
public final class ChildFaults {

    /** The journal writer agent's thread name ({@code MoneyJournalWriterAgent.roleName()}). */
    static final String WRITER_THREAD = "money-journal-writer";

    private ChildFaults() {
    }

    /**
     * RCS S4 11.13 Q9 drill (D6): {@code Runtime.halt(exitCode)} right after {@code onTakeSnapshot} returns, so Aeron's
     * own {@code awaitRecordingComplete} and the service ack to the consensus module never run (the window M Q9 is
     * about). A snapshot taken before this node is the leader (a replayed action during recovery) is let through, so
     * only the drill's own snapshot, requested on the live leader, halts.
     */
    public static void haltAfterTakeSnapshot(final Object service, final long exitCode) {
        final AssetsClusteredService s = (AssetsClusteredService)service;
        ServiceProbe.afterTakeSnapshot(s, () -> {
            if (!ServiceProbe.isLeader(s)) {
                System.out.println("[RCS-CHILD] onTakeSnapshot returned before this node led (recovery): not halting");
                return;
            }
            System.out.println("[RCS-CHILD] halting right after onTakeSnapshot returned (exit " + exitCode + ")");
            System.out.flush();
            Runtime.getRuntime().halt((int)exitCode);
        });
        System.out.println("[RCS-CHILD] installed Faults.afterTakeSnapshot(halt " + exitCode + ")");
    }

    /** Review F8: the tail pauses after {@code n} applied frames, forever (the driver is killed during the pause). */
    public static void pauseTailAfter(final Object service, final long n) {
        ServiceProbe.afterTailFrames((AssetsClusteredService)service, n, () -> {
            System.out.println("[RCS-CHILD] tail paused after " + n + " frame(s) (Faults.afterTailFrames)");
            while (true) {
                try {
                    Thread.sleep(1_000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        System.out.println("[RCS-CHILD] installed Faults.afterTailFrames(" + n + ", pause)");
    }

    /**
     * Review F3: after {@code n} applied tail frames the journal writer agent is stopped (its thread interrupted:
     * {@code AgentRunner} ends its loop on an interrupt while idle), so the ring fills and the tail must hit
     * {@code RCS_TAIL_REPLAY_TIMEOUT_MS}.
     */
    public static void stopJournalWriterAfter(final Object service, final long n) {
        ServiceProbe.afterTailFrames((AssetsClusteredService)service, n, () -> {
            final List<String> stopped = new ArrayList<>();
            for (final Thread t : Thread.getAllStackTraces().keySet()) {
                if (WRITER_THREAD.equals(t.getName())) {
                    t.interrupt();
                    stopped.add(t.getName() + "#" + t.threadId());
                }
            }
            System.out.println("[RCS-CHILD] journal writer agent stopped after " + n + " tail frame(s): " + stopped);
        });
        System.out.println("[RCS-CHILD] installed Faults.afterTailFrames(" + n + ", stop journal writer)");
    }
}
