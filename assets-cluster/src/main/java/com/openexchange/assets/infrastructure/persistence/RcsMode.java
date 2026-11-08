// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.TransportConfig;

/**
 * RCS S4 3.1: the node-local stub mode ({@code RCS_MODE} / {@code rcs.mode}, {@code off} by default). It decides what
 * THIS node writes into its own service snapshot ({@code shadow}: a {@code ShadowStub}; {@code off}: the full v5
 * image) and nothing else: the restore path is chosen by the payload tag of the image Aeron hands {@code onStart}
 * (D-S4-1), and the engine and the applier never read it (hard rule 12.1). A node-local I/O choice (M Q7), so it is
 * read from the environment like the transport settings, once at boot.
 */
public enum RcsMode {
    OFF("off"),
    SHADOW("shadow");

    public static final String ENV = "RCS_MODE";
    public static final String PROPERTY = "rcs.mode";

    private final String text;

    RcsMode(final String text) {
        this.text = text;
    }

    public String text() {
        return text;
    }

    /**
     * @throws IllegalArgumentException with the S4 3.5 refusal text (the boot guard prefixes it)
     */
    public static RcsMode parse(final String value) {
        final String v = value == null ? "" : value.trim();
        for (final RcsMode m : values()) {
            if (m.text.equals(v)) {
                return m;
            }
        }
        throw new IllegalArgumentException("RCS_MODE '" + value + "' (expected off|shadow)");
    }

    /** {@code RCS_MODE} first, then {@code -Drcs.mode}, else {@code off} (the transport settings' helper). */
    public static RcsMode fromEnv() {
        return parse(TransportConfig.envOrProp(ENV, PROPERTY, "off"));
    }
}
