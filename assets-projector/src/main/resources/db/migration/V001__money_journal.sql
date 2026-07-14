-- SPDX-License-Identifier: Apache-2.0
-- Money-journal projection: the external, queryable Postgres mirror of every APPLIED money
-- movement recorded in the AE money journal (money-journal-schema.xml, schema id=3). One header
-- row per dense journalSeq, plus exploded per-(user, asset) balance-after legs. Applied by hand
-- (there is no Flyway runner). All amounts are 8dp fixed-point int64 -> BIGINT.

-- Header: one row per journalSeq. movement_type is the journal templateId (1=opening, 2=deposit,
-- 3=withdraw, 4=settle). cluster_time_ms is NULL for the opening epoch. The deposit/withdraw/opening
-- columns (user_id, asset_id, amount, balance_after) and the settle columns (trade_id .. taker_is_buy)
-- are mutually exclusive by movement_type; the unused ones stay NULL.
CREATE TABLE money_journal (
    journal_seq     BIGINT PRIMARY KEY,
    movement_type   SMALLINT NOT NULL,
    cluster_time_ms BIGINT,
    user_id         BIGINT,
    asset_id        INT,
    amount          BIGINT,
    balance_after   BIGINT,
    trade_id        BIGINT,
    market_id       INT,
    price           BIGINT,
    quantity        BIGINT,
    buyer_user_id   BIGINT,
    seller_user_id  BIGINT,
    taker_is_buy    BOOLEAN,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Exploded balance-after facts. Deposit/withdraw/opening -> one leg with a real asset_id and a NULL
-- asset_role. Settle -> four legs (buyer/seller x BASE/QUOTE); a settle record carries market_id only
-- (NOT the base/quote assetIds), so settle legs leave asset_id NULL and set asset_role (0=BASE,
-- 1=QUOTE), with asset resolution left to a downstream join on money_journal.market_id. balance_after
-- is the authoritative (available + locked) total; delta is the signed movement where derivable
-- (deposit +amount, withdraw -amount) and NULL otherwise (opening; settle).
CREATE TABLE money_journal_leg (
    journal_seq   BIGINT NOT NULL REFERENCES money_journal(journal_seq),
    leg_no        SMALLINT NOT NULL,
    user_id       BIGINT NOT NULL,
    asset_id      INT,
    asset_role    SMALLINT,
    balance_after BIGINT NOT NULL,
    delta         BIGINT,
    PRIMARY KEY (journal_seq, leg_no)
);

CREATE INDEX idx_money_journal_leg_user_asset ON money_journal_leg(user_id, asset_id);
