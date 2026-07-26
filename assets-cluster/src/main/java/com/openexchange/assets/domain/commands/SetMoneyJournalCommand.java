// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain.commands;

/**
 * Arm or disarm the money journal for the whole cluster. Carried through the replicated log because
 * what it controls — whether {@code journalSeq} advances — is snapshotted state: a setting that lives
 * in a node's environment instead would let two replicas disagree, and would make a replayed log
 * produce a different ledger than the one it recorded.
 *
 * <p>Mutates no money and emits no egress. Pooled; {@link #reset()} for reuse.</p>
 */
public final class SetMoneyJournalCommand {
    private long correlationId;
    private boolean enabled;

    public void reset() {
        correlationId = 0L;
        enabled = false;
    }

    public long getCorrelationId() { return correlationId; }
    public void setCorrelationId(long correlationId) { this.correlationId = correlationId; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
