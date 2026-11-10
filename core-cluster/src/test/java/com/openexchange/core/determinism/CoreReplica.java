// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.cluster.testkit.Replica;

import java.util.List;

/** The core as the kit's {@link Replica}: scenario lines in, rendered events and enveloped v4 images out. */
public final class CoreReplica implements Replica {

    private final CoreScenarioRunner runner = CoreScenarioRunner.newRunner();

    @Override
    public void apply(final String line) {
        runner.applyLine(line);
    }

    @Override
    public List<String> events() {
        return runner.renderedEvents();
    }

    @Override
    public byte[] snapshot() {
        return runner.snapshotBytes();
    }

    @Override
    public void restore(final byte[] bytes) {
        runner.restoreFrom(bytes);
    }

    CoreScenarioRunner runner() {
        return runner;
    }
}
