// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

/**
 * RC2 S9h 4 / 4.1: C++ initialisers in the {@code golden_frames.inc} style
 * ({@code static const std::vector<std::uint8_t> NAME = {...};}, 16 bytes per line, lower-case hex).
 * {@code NAME_IN} holds the case's FJR1 file bytes (records and FLUSH markers), {@code NAME_OUT} its FFR1 file bytes.
 */
public final class IncWriter {

    private IncWriter() {
    }

    public static void array(final StringBuilder sb, final String name, final byte[] bytes) {
        sb.append("static const std::vector<std::uint8_t> ").append(name).append(" = {\n");
        for (int i = 0; i < bytes.length; i += 16) {
            sb.append("   ");
            for (int k = i; k < Math.min(bytes.length, i + 16); k++) {
                sb.append(String.format(" 0x%02x,", bytes[k] & 0xFF));
            }
            sb.append('\n');
        }
        sb.append("};\n");
    }

    /** {@code NAME_IN} and {@code NAME_OUT} of one case. */
    public static String pair(final String name, final byte[] fjr, final byte[] ffr) {
        final StringBuilder sb = new StringBuilder();
        array(sb, name + "_IN", fjr);
        array(sb, name + "_OUT", ffr);
        return sb.toString();
    }
}
