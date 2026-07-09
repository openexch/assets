// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure;

/**
 * Minimal printf-style logger for the cluster bootstrap. Deliberately dependency-free (no slf4j) so
 * the Assets Engine node has no logging framework to configure — it writes to stdout/stderr, which the
 * process manager captures into the node log.
 */
public final class Logger {

    private final String name;

    private Logger(String name) {
        this.name = name;
    }

    public static Logger getLogger(Class<?> cls) {
        return new Logger(cls.getSimpleName());
    }

    public void info(String format, Object... args) {
        System.out.println("[INFO ] " + name + " - " + fmt(format, args));
    }

    public void warn(String format, Object... args) {
        System.out.println("[WARN ] " + name + " - " + fmt(format, args));
    }

    public void error(String format, Object... args) {
        System.err.println("[ERROR] " + name + " - " + fmt(format, args));
    }

    private static String fmt(String format, Object... args) {
        return args.length == 0 ? format : String.format(format, args);
    }
}
