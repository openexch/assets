// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * PostgreSQL-backed money-journal store: one batched, atomic transaction projects a run of
 * {@link MoneyJournalRecord}s into {@code money_journal} (header, one row per journalSeq) and
 * {@code money_journal_leg} (the exploded balance-after facts). Mirrors the OMS
 * PostgresBalanceReadModelStore batched-upsert idiom (setAutoCommit(false) + addBatch +
 * executeBatch + commit/rollback).
 *
 * Idempotent by construction: header inserts are {@code ON CONFLICT (journal_seq) DO NOTHING} and
 * leg inserts are {@code ON CONFLICT (journal_seq, leg_no) DO NOTHING}, so re-projecting an already
 * committed journalSeq (replay overlap, epoch restart, retry after a partial-looking failure)
 * inserts nothing new. Postgres is the projector's authoritative high-water: nothing is skipped
 * on a failure, the whole transaction rolls back and is retried from the (unchanged) PG high-water.
 */
public final class PostgresMoneyJournalStore {

    private static final String INSERT_HEADER = """
            INSERT INTO money_journal
                (journal_seq, movement_type, cluster_time_ms, user_id, asset_id, amount, balance_after,
                 trade_id, market_id, price, quantity, buyer_user_id, seller_user_id, taker_is_buy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (journal_seq) DO NOTHING
            """;

    private static final String INSERT_LEG = """
            INSERT INTO money_journal_leg
                (journal_seq, leg_no, user_id, asset_id, asset_role, balance_after, delta)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (journal_seq, leg_no) DO NOTHING
            """;

    private static final String MAX_SEQ = "SELECT COALESCE(MAX(journal_seq), 0) FROM money_journal";

    private final HikariDataSource dataSource;

    public PostgresMoneyJournalStore(final HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Resume high-water: MAX(journal_seq) already projected, or 0 on an empty ledger. */
    public long highWater() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MAX_SEQ);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    /**
     * Project a run of records in journalSeq order in one transaction: all headers, then all legs
     * (headers first so the leg foreign key resolves). Either the whole batch commits or it rolls
     * back and the caller retries; the high-water never advances past an uncommitted record.
     */
    public void project(final List<MoneyJournalRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement header = conn.prepareStatement(INSERT_HEADER);
                 PreparedStatement leg = conn.prepareStatement(INSERT_LEG)) {
                for (final MoneyJournalRecord r : records) {
                    bindHeader(header, r);
                    header.addBatch();
                }
                header.executeBatch();
                for (final MoneyJournalRecord r : records) {
                    for (final MoneyJournalRecord.Leg l : r.legs()) {
                        bindLeg(leg, r.journalSeq(), l);
                        leg.addBatch();
                    }
                }
                leg.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private static void bindHeader(final PreparedStatement ps, final MoneyJournalRecord r)
            throws SQLException {
        ps.setLong(1, r.journalSeq());
        ps.setShort(2, r.movementType());
        setNullableLong(ps, 3, r.clusterTimeMs());
        setNullableLong(ps, 4, r.userId());
        setNullableInt(ps, 5, r.assetId());
        setNullableLong(ps, 6, r.amount());
        setNullableLong(ps, 7, r.balanceAfter());
        setNullableLong(ps, 8, r.tradeId());
        setNullableInt(ps, 9, r.marketId());
        setNullableLong(ps, 10, r.price());
        setNullableLong(ps, 11, r.quantity());
        setNullableLong(ps, 12, r.buyerUserId());
        setNullableLong(ps, 13, r.sellerUserId());
        if (r.takerIsBuy() == null) {
            ps.setNull(14, Types.BOOLEAN);
        } else {
            ps.setBoolean(14, r.takerIsBuy());
        }
    }

    private static void bindLeg(final PreparedStatement ps, final long journalSeq,
                                final MoneyJournalRecord.Leg l) throws SQLException {
        ps.setLong(1, journalSeq);
        ps.setShort(2, l.legNo());
        ps.setLong(3, l.userId());
        setNullableInt(ps, 4, l.assetId());
        if (l.assetRole() == null) {
            ps.setNull(5, Types.SMALLINT);
        } else {
            ps.setShort(5, l.assetRole());
        }
        ps.setLong(6, l.balanceAfter());
        setNullableLong(ps, 7, l.delta());
    }

    private static void setNullableLong(final PreparedStatement ps, final int idx, final Long v)
            throws SQLException {
        if (v == null) {
            ps.setNull(idx, Types.BIGINT);
        } else {
            ps.setLong(idx, v);
        }
    }

    private static void setNullableInt(final PreparedStatement ps, final int idx, final Integer v)
            throws SQLException {
        if (v == null) {
            ps.setNull(idx, Types.INTEGER);
        } else {
            ps.setInt(idx, v);
        }
    }
}
