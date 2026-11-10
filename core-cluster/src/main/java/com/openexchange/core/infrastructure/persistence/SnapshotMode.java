// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.TransportConfig;

/**
 * The node-local stub mode ({@code SNAPSHOT_MODE} / {@code snapshot.mode}, {@code off} by default). It decides what
 * THIS node writes into its own service snapshot ({@code shadow}: a {@code ShadowStub}; {@code off}: the full v5
 * image) and nothing else: the restore path is chosen by the payload tag of the image Aeron hands {@code onStart},
 * and the engine and the applier never read it (hard rule 12.1). A node-local I/O choice (M Q7), so it is
 * read from the environment like the transport settings, once at boot.
 */
public enum SnapshotMode {
    OFF("off"),
    SHADOW("shadow");

    public static final String ENV = "SNAPSHOT_MODE";
    public static final String PROPERTY = "snapshot.mode";

    private final String text;

    SnapshotMode(final String text) {
        this.text = text;
    }

    public String text() {
        return text;
    }

    /**
     * @throws IllegalArgumentException with the refusal text (the boot guard prefixes it)
     */
    public static SnapshotMode parse(final String value) {
        final String v = value == null ? "" : value.trim();
        for (final SnapshotMode m : values()) {
            if (m.text.equals(v)) {
                return m;
            }
        }
        throw new IllegalArgumentException("SNAPSHOT_MODE '" + value + "' (expected off|shadow)");
    }

    /** {@code SNAPSHOT_MODE} first, then {@code -Dsnapshot.mode}, else {@code off} (the transport settings' helper). */
    public static SnapshotMode fromEnv() {
        return parse(TransportConfig.envOrProp(ENV, PROPERTY, "off"));
    }
}
