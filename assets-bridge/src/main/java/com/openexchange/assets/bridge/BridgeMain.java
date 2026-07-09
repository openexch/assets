// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.bridge;

/**
 * Entry point for the settlement bridge agent. Placeholder only: this change lands the module
 * skeleton and the pure {@link JournalToMoneyTranslator}. The real agent loop (archive-tailing the
 * ME settlement journal, publishing translated commands to the Assets Engine cluster, tracking
 * {@code FeedPositionReport} for resume, retry/backoff) is a separate, lead-owned change.
 */
public final class BridgeMain {

    private BridgeMain() {
    }

    public static void main(String[] args) {
        System.out.println("settlement bridge: core lands in the next change");
        System.exit(1);
    }
}
