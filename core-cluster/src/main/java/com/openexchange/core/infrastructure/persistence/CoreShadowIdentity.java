// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.domain.commands.CoreConfigCommand;
import com.openexchange.cluster.shadow.ShadowIdentitySource;

import java.util.Objects;

/**
 * The core's replicated identity values for a kit shadow header, read on the apply thread at the
 * same instant as {@code CoreLogApplier.serialize} (the host's obligation; the kit's post-restore
 * {@code ShadowIdentity.matches} catches a host that reads them at another instant).
 *
 * <ul>
 *   <li>{@link #genesisId}: the nonce the recorded CoreConfig ADOPTed, 0 before any ADOPT. Since
 * amendment A7 the engine ADOPTs only {@code genesisId >= 1}; a value {@code <= 0} can only come from an image
 *       recorded before A7, and the writer refuses it (H10) and the producer host stops on it
 *       ({@code ShadowIdentity.requireProducible}).</li>
 *   <li>{@link #journalEnabled}, {@link #journalSeq}: the REPLICATED money-journal arming and its dense seq, never the
 *       node-local sink.</li>
 *   <li>{@link #payloadLayoutHint}: 5, the v5 snapshot layout ({@code BalanceSnapshotCodec.LAYOUT_V5_TAG = -5}
 * ); diagnostic only.</li>
 * </ul>
 * No clock, environment, role or {@code Cluster} reaches it (the applier closure scan covers it).
 */
public final class CoreShadowIdentity implements ShadowIdentitySource {

    /** The snapshot layout the applier's image carries (v5). */
    public static final int PAYLOAD_LAYOUT_V5 = 5;

    private final CoreEngine engine;

    /** @param engine the applier's {@code engine()} */
    public CoreShadowIdentity(final CoreEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine");
    }

    @Override
    public long genesisId() {
        final CoreConfigCommand recorded = engine.coreConfig().recorded();
        return recorded == null ? 0L : recorded.genesisId;
    }

    @Override
    public boolean journalEnabled() {
        return engine.isMoneyJournalEnabled();
    }

    @Override
    public long journalSeq() {
        return engine.getJournalSeq();
    }

    @Override
    public int payloadLayoutHint() {
        return PAYLOAD_LAYOUT_V5;
    }
}
