// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.projector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Real-Postgres round-trip for {@link PostgresMoneyJournalStore}: header + leg insert and the
 * idempotent ON CONFLICT re-projection. GATED like the OMS marketdata_test integration tests: it
 * runs only when {@code PROJECTOR_PG_TEST_URL} is set (pointing at a database that already has the
 * V001 money_journal schema applied by hand), and is otherwise skipped, so the default
 * {@code mvn test} on a box without Postgres stays green.
 *
 * Uses a high, run-unique journalSeq base and deletes its rows in tearDown, so it leaves no residue
 * in a shared database.
 */
public class PostgresMoneyJournalStoreIntegrationTest {

    private HikariDataSource dataSource;
    private PostgresMoneyJournalStore store;
    private long seqBase;

    @Before
    public void setUp() {
        final String url = System.getenv("PROJECTOR_PG_TEST_URL");
        assumeTrue("set PROJECTOR_PG_TEST_URL (db with V001 applied) to run the projector PG round-trip",
                url != null && !url.isBlank());

        final HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(envOr("PROJECTOR_PG_TEST_USER", "assets"));
        cfg.setPassword(envOr("PROJECTOR_PG_TEST_PASSWORD", ""));
        cfg.setMaximumPoolSize(2);
        cfg.setConnectionTimeout(5000);
        dataSource = new HikariDataSource(cfg);
        store = new PostgresMoneyJournalStore(dataSource);
        // Far above any real journalSeq; run-unique so parallel/rerun executions do not collide.
        seqBase = 9_000_000_000_000L + Math.floorMod(System.nanoTime(), 1_000_000L) * 10L;
    }

    @After
    public void tearDown() throws Exception {
        if (dataSource != null) {
            try (Connection c = dataSource.getConnection()) {
                exec(c, "DELETE FROM money_journal_leg WHERE journal_seq BETWEEN ? AND ?");
                exec(c, "DELETE FROM money_journal WHERE journal_seq BETWEEN ? AND ?");
            } finally {
                dataSource.close();
            }
        }
    }

    @Test
    public void projectInsertsHeaderAndLegsThenIsIdempotentOnReProjection() throws Exception {
        final long depositSeq = seqBase + 1;
        final long settleSeq = seqBase + 2;
        final List<MoneyJournalRecord> batch = List.of(
                MoneyJournalRecord.deposit(depositSeq, 1001L, 2, 500_000L, 1_500_000L, 42L),
                MoneyJournalRecord.settle(settleSeq, 55L, 5, 1_000_000L, 2_000_000L, 200L, 300L, true,
                        11L, 22L, 33L, 44L, 77L));

        store.project(batch);
        assertEquals("deposit -> 1 leg", 1, legCount(depositSeq));
        assertEquals("settle -> 4 legs", 4, legCount(settleSeq));
        assertEquals("two headers", 2, headerCount());
        assertTrue("high-water reflects the projected max", store.highWater() >= settleSeq);

        // Re-project the identical batch: ON CONFLICT DO NOTHING -> nothing new.
        store.project(batch);
        assertEquals(1, legCount(depositSeq));
        assertEquals(4, legCount(settleSeq));
        assertEquals(2, headerCount());
    }

    private int headerCount() throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM money_journal WHERE journal_seq BETWEEN ? AND ?")) {
            ps.setLong(1, seqBase);
            ps.setLong(2, seqBase + 9);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int legCount(final long journalSeq) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM money_journal_leg WHERE journal_seq = ?")) {
            ps.setLong(1, journalSeq);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private void exec(final Connection c, final String sql) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, seqBase);
            ps.setLong(2, seqBase + 9);
            ps.executeUpdate();
        }
    }

    private static String envOr(final String key, final String fallback) {
        final String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }
}
