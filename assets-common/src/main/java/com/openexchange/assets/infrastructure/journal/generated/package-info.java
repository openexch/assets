/* Generated SBE (Simple Binary Encoding) message codecs.*/
/**
 * AE money journal schema: every APPLIED money movement (deposit, withdrawal, settle) plus the opening-balance epoch, appended on the deterministic service thread and recorded per node as a replayable Aeron Archive stream. journalSeq is dense (starts at 1, no gaps, no dupes) and is the FIRST field of every message so readers can extract it at a fixed offset. Amounts are 8dp fixed-point int64. balanceAfter fields are the (available + locked) TOTAL for that (user, asset): the journal has no hold visibility (a hold moves available to locked without changing the total), so totals are the only self-consistent balance representation. Rejects and no-op dedupes journal nothing. NO hold/release events by design.
 */
package com.openexchange.assets.infrastructure.journal.generated;
