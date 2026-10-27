// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RC2 S9h 7.1: the T1 key layout ({@code ASSETS_AUTH_KEYS}, every key {@code users=*}) and the client variables the T1
 * tools read. Keys 3, 10 and 11 are the three BRIDGE identities (bridge0, bridge1, bridge_b the upgrade build); key 6 is
 * the FORWARDER key the story's market ticks need (an addition to the 7.1 list, see the S9h README deviations).
 */
public final class T1Keys {

    static String hex(final String pair) {
        return pair.repeat(32);
    }

    public static final String ADMIN = "1:ADMIN:" + hex("11");
    public static final String TOOL = "2:TOOL:" + hex("22");
    public static final String BRIDGE0 = "3:BRIDGE:" + hex("33");
    public static final String BRIDGE1 = "10:BRIDGE:" + hex("aa");
    public static final String BRIDGE_B = "11:BRIDGE:" + hex("bb");
    public static final String GATEWAY = "5:GATEWAY:" + hex("55");
    public static final String FORWARDER = "6:FORWARDER:" + hex("66");
    public static final String TREASURY = "9:TREASURY:" + hex("99");

    private T1Keys() {
    }

    /** The {@code ASSETS_AUTH_KEYS} value; {@code withBridge0 = false} drops key 3 (the RED of 11.4). */
    public static String table(final boolean withBridge0) {
        final StringBuilder sb = new StringBuilder();
        for (final String k : new String[] {ADMIN, TOOL, withBridge0 ? BRIDGE0 : null, BRIDGE1, BRIDGE_B, GATEWAY, FORWARDER,
                TREASURY}) {
            if (k != null) {
                sb.append(sb.length() == 0 ? "" : ";").append(k).append("::users=*");
            }
        }
        return sb.toString();
    }

    /** The T1 tool environment for a cluster at {@code ingress} with a client driver at {@code aeronDir}. */
    public static Map<String, String> env(final String ingress, final String aeronDir) {
        final Map<String, String> env = new LinkedHashMap<>();
        env.put("T1_CORE_INGRESS", ingress);
        env.put("T1_AERON_DIR", aeronDir);
        env.put("T1_ADMIN_KEY", ADMIN);
        env.put("T1_TOOL_KEY", TOOL);
        env.put("T1_GATEWAY_KEY", GATEWAY);
        env.put("T1_TREASURY_KEY", TREASURY);
        env.put("T1_FORWARDER_KEY", FORWARDER);
        env.put("T1_BRIDGE_KEY", BRIDGE0);
        return env;
    }
}
