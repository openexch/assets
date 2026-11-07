// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

import com.openexchange.cluster.testkit.Replica;

import java.util.List;

/** The AE as the kit's {@link Replica}: scenario lines in, rendered events and enveloped v4 images out. */
public final class AssetsReplica implements Replica {

    private final AssetsScenarioRunner runner = AssetsScenarioRunner.newRunner();

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

    AssetsScenarioRunner runner() {
        return runner;
    }
}
