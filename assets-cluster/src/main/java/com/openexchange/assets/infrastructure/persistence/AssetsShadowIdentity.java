// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.application.engine.AssetsEngine;
import com.openexchange.assets.domain.commands.CoreConfigCommand;
import com.openexchange.cluster.shadow.ShadowIdentitySource;

import java.util.Objects;

/**
 * RCS S3 5.2: the assets engine's replicated identity values for a kit shadow header, read on the apply thread at the
 * same instant as {@code AssetsLogApplier.serialize} (the host's obligation; the kit's post-restore
 * {@code ShadowIdentity.matches} catches a host that reads them at another instant).
 *
 * <ul>
 *   <li>{@link #genesisId}: the nonce the recorded CoreConfig ADOPTed (RC2 S7 7.1 row 2), 0 before any ADOPT. Since RCS
 *       S3 amendment A7 the engine ADOPTs only {@code genesisId >= 1}; a value {@code <= 0} can only come from an image
 *       recorded before A7, and the writer refuses it (H10) and the producer host stops on it
 *       ({@code ShadowIdentity.requireProducible}).</li>
 *   <li>{@link #journalEnabled}, {@link #journalSeq}: the REPLICATED money-journal arming and its dense seq, never the
 *       node-local sink.</li>
 *   <li>{@link #payloadLayoutHint}: 4, the v4 snapshot layout ({@code BalanceSnapshotCodec.LAYOUT_V4_TAG = -4});
 *       diagnostic only.</li>
 * </ul>
 * No clock, environment, role or {@code Cluster} reaches it (the applier closure scan covers it).
 */
public final class AssetsShadowIdentity implements ShadowIdentitySource {

    /** The v4 snapshot layout the applier's image carries. */
    public static final int PAYLOAD_LAYOUT_V4 = 4;

    private final AssetsEngine engine;

    /** @param engine the applier's {@code engine()} */
    public AssetsShadowIdentity(final AssetsEngine engine) {
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
        return PAYLOAD_LAYOUT_V4;
    }
}
