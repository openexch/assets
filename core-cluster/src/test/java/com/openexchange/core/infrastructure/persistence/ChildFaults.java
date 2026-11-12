// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import java.util.ArrayList;
import java.util.List;

/**
 * And the 11.13 Q9 child leg: the {@code ShadowRestore.Faults} seams installed INSIDE a child engine JVM (the
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
     * Q9 drill (D6): {@code Runtime.halt(exitCode)} right after {@code onTakeSnapshot} returns, so Aeron's
     * own {@code awaitRecordingComplete} and the service ack to the consensus module never run (the window M Q9 is
     * about). A snapshot taken before this node is the leader (a replayed action during recovery) is let through, so
     * only the drill's own snapshot, requested on the live leader, halts.
     */
    public static void haltAfterTakeSnapshot(final Object service, final long exitCode) {
        final CoreClusteredService s = (CoreClusteredService)service;
        ServiceProbe.afterTakeSnapshot(s, () -> {
            if (!ServiceProbe.isLeader(s)) {
                System.out.println("[restore-child] onTakeSnapshot returned before this node led (recovery): not halting");
                return;
            }
            System.out.println("[restore-child] halting right after onTakeSnapshot returned (exit " + exitCode + ")");
            System.out.flush();
            Runtime.getRuntime().halt((int)exitCode);
        });
        System.out.println("[restore-child] installed Faults.afterTakeSnapshot(halt " + exitCode + ")");
    }

    /** Review F8: the tail pauses after {@code n} applied frames, forever (the driver is killed during the pause). */
    public static void pauseTailAfter(final Object service, final long n) {
        ServiceProbe.afterTailFrames((CoreClusteredService)service, n, () -> {
            System.out.println("[restore-child] tail paused after " + n + " frame(s) (Faults.afterTailFrames)");
            while (true) {
                try {
                    Thread.sleep(1_000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        System.out.println("[restore-child] installed Faults.afterTailFrames(" + n + ", pause)");
    }

    /**
     * Review F3: after {@code n} applied tail frames the journal writer agent is stopped (its thread interrupted:
     * {@code AgentRunner} ends its loop on an interrupt while idle), so the ring fills and the tail must hit
     * {@code SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS}.
     */
    public static void stopJournalWriterAfter(final Object service, final long n) {
        ServiceProbe.afterTailFrames((CoreClusteredService)service, n, () -> {
            final List<String> stopped = new ArrayList<>();
            for (final Thread t : Thread.getAllStackTraces().keySet()) {
                if (WRITER_THREAD.equals(t.getName())) {
                    t.interrupt();
                    stopped.add(t.getName() + "#" + t.threadId());
                }
            }
            System.out.println("[restore-child] journal writer agent stopped after " + n + " tail frame(s): " + stopped);
        });
        System.out.println("[restore-child] installed Faults.afterTailFrames(" + n + ", stop journal writer)");
    }

    /**
     * The follower producer's writer holds the first payload chunk of every ALIGNED write for {@code holdMs} (the slow
     * writer of the in-process rows), so a driver kill lands inside a write.
     */
    public static void slowShadowWriter(final Object service, final long holdMs) {
        ServiceProbe.slowWriter((CoreClusteredService)service, holdMs);
        System.out.println("[restore-child] installed a slow shadow writer (" + holdMs + " ms hold)");
    }
}
