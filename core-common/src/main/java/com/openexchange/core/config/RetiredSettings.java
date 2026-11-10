// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.config;

import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * The settings retired by the rename of the engine to core, in ONE table shared by every process entry point (member,
 * replica, bridge and its tools, projector, load generator, feed harness CLI). A retired name that is still set means a
 * deployment was written for the old names: the process refuses to start (exit 1) with one line naming the old and the
 * new name, before it reads any setting. There is no dual read and no fallback: the old name is never honoured.
 *
 * <p>An entry counts as set when the variable or property is present at all, an empty value included (an empty
 * {@code ASSETS_NODE_PORT=} line is still a deployment written for the old names).</p>
 */
public final class RetiredSettings {

    /** Where a retired name lives. */
    public enum Kind {
        ENV("environment variable"),
        PROPERTY("system property"),
        FLAG("command-line flag");

        private final String text;

        Kind(final String text) {
            this.text = text;
        }

        public String text() {
            return text;
        }
    }

    /** One retired name and the name that replaces it. */
    public record Rename(Kind kind, String retired, String replacement) {
        public Rename {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(retired, "retired");
            Objects.requireNonNull(replacement, "replacement");
        }
    }

    /** Every retired name, old to new. */
    public static final List<Rename> TABLE = List.of(
            env("ASSETS_ROLE", "CORE_ROLE"),
            env("ASSETS_NODE_PORT", "CORE_NODE_PORT"),
            env("ASSETS_AUTH_KEYS", "CORE_AUTH_KEYS"),
            env("AE_AUTH_CREDENTIAL", "CORE_AUTH_CREDENTIAL"),
            env("ASSETS_APPLY_SAMPLE_SHIFT", "CORE_APPLY_SAMPLE_SHIFT"),
            env("BRIDGE_AE_EGRESS_ENDPOINT", "BRIDGE_CORE_EGRESS_ENDPOINT"),
            env("BRIDGE_AE_CLUSTER_ADDRESSES", "BRIDGE_CORE_CLUSTER_ADDRESSES"),
            env("BRIDGE_AE_PORT_BASE", "BRIDGE_CORE_PORT_BASE"),
            env("PROJECTOR_AE_JOURNAL_ARCHIVES", "PROJECTOR_CORE_JOURNAL_ARCHIVES"),
            env("PROJECTOR_AE_ARCHIVE_CONTROL_STREAM_ID", "PROJECTOR_CORE_ARCHIVE_CONTROL_STREAM_ID"),
            env("RCS_MODE", "SNAPSHOT_MODE"),
            env("RCS_TAIL_REPLAY_TIMEOUT_MS", "SNAPSHOT_TAIL_REPLAY_TIMEOUT_MS"),
            env("RCS_READY_BUDGET_MS", "SNAPSHOT_RESTORE_READY_BUDGET_MS"),
            env("RCS_A04_CM_SNAPSHOT_MAX_BYTES", "CONSENSUS_SNAPSHOT_MAX_BYTES"),
            property("assets.role", "core.role"),
            property("assets.node.port", "core.node.port"),
            property("assets.auth.keys", "core.auth.keys"),
            property("assets.auth.credential", "core.auth.credential"),
            property("assets.apply.sample.shift", "core.apply.sample.shift"),
            property("assets.egress.max.capacity.bytes", "core.egress.max.capacity.bytes"),
            property("rcs.mode", "snapshot.mode"),
            property("rcs.tail.replay.timeout.ms", "snapshot.tail.replay.timeout.ms"),
            property("rcs.ready.budget.ms", "snapshot.restore.ready.budget.ms"),
            property("rcs.a04.cm.snapshot.max.bytes", "consensus.snapshot.max.bytes"),
            flag("--ae-only", "--core-only"),
            flag("--ae-hosts", "--core-hosts"),
            flag("--ae-port-base", "--core-port-base"));

    private RetiredSettings() {
    }

    /**
     * The refusal line for the first retired environment variable or system property that is set (table order), or
     * {@code null} when none is.
     *
     * @param process the jar or tool named in the line, for example {@code core-cluster.jar}
     * @param env     environment lookup ({@code System::getenv} in production)
     * @param props   system property lookup ({@code System::getProperty} in production)
     */
    public static String refusal(final String process, final UnaryOperator<String> env,
                                 final UnaryOperator<String> props) {
        for (final Rename r : TABLE) {
            final String value = switch (r.kind()) {
                case ENV -> env.apply(r.retired());
                case PROPERTY -> props.apply(r.retired());
                case FLAG -> null;
            };
            if (value != null) {
                return line(process, r);
            }
        }
        return null;
    }

    /** The refusal line for the first retired flag among {@code args}, or {@code null} when none is. */
    public static String flagRefusal(final String process, final String[] args) {
        for (final String arg : args) {
            for (final Rename r : TABLE) {
                if (r.kind() == Kind.FLAG && (arg.equals(r.retired()) || arg.startsWith(r.retired() + "="))) {
                    return line(process, r);
                }
            }
        }
        return null;
    }

    /**
     * The entry-point guard: prints the refusal on stderr and exits 1 when a retired environment variable or system
     * property is set. Call it first in {@code main}, before any setting is read.
     */
    public static void refuseRetired(final String process) {
        exitIfRefused(refusal(process, System::getenv, System::getProperty));
    }

    /** {@link #refuseRetired} plus the retired command-line flags. */
    public static void refuseRetired(final String process, final String[] args) {
        refuseRetired(process);
        exitIfRefused(flagRefusal(process, args));
    }

    private static void exitIfRefused(final String refusal) {
        if (refusal != null) {
            System.err.println(refusal);
            System.err.flush();
            System.exit(1);
        }
    }

    static String line(final String process, final Rename r) {
        return "FATAL: " + process + " refused to start: the " + r.kind().text() + " " + r.retired()
                + " is retired; it is now " + r.replacement() + " (rename the setting; the old name is never read)";
    }

    private static Rename env(final String retired, final String replacement) {
        return new Rename(Kind.ENV, retired, replacement);
    }

    private static Rename property(final String retired, final String replacement) {
        return new Rename(Kind.PROPERTY, retired, replacement);
    }

    private static Rename flag(final String retired, final String replacement) {
        return new Rename(Kind.FLAG, retired, replacement);
    }
}
