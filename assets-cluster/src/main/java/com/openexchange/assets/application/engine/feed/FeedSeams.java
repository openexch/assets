// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.feed;

/**
 * RC2 S10' 4R.2 RC-5 (contract S2-6): the core's negative-control seams for the lane-halt table and the automatic reopen.
 * Every seam switches ONE rule of section 4R off so the unit test that proves the rule fails on its named text.
 *
 * <p>The type and its {@link #on} field are package-private: only code in {@code application.engine.feed} can read a seam,
 * and only a test in that package (src/test) can switch one. Nothing reads a system property, an environment variable or
 * a config key; the production jar carries the enum with every seam {@code false} and no caller that could change it.
 * {@code FeedSeamsTest.feedSeamsAllOffByDefault} pins the default.</p>
 */
enum FeedSeams {
    /** RC-3: the reopen clears every gate bit (ADMIN OPEN's {@code clearAll}) instead of BRIDGE_HALT alone. */
    AUTO_REOPEN_CLEARS_ALL,
    /** RC-2: a GAP FeedHalt from a second identity while a record is open is DUPLICATE and forgotten (the S7 single slot). */
    AUTO_REOPEN_SINGLE_SLOT,
    /** RC-1: section 12 is written with no lane records (the holder set and the overflow as they are). */
    LANE_TABLE_NOT_SNAPSHOTTED,
    /** RC-3: the overflow holder does not keep the bit (an overflow reads as "everything cleared"). */
    OVERFLOW_READS_CLEARED,
    /** RC-3: a SOURCE_SCHEMA / SETTLE_REFUSED reach-clear releases its holder (any reason reopens). */
    AUTO_REOPEN_ANY_REASON,
    /** RC-3: the NONGAP_AWAITING_OPEN holder does not keep the bit when the last GAP record clears. */
    AUTO_REOPEN_OVER_AWAITING_OPEN,
    /** RC-2: a replace from another session takes the new {@code seen} instead of {@code max(old, new)}. */
    REPLACE_TAKES_LAST_SEEN,
    /** RC-1: section 12 accepts a version byte this build does not know (read as version 1). */
    UNKNOWN_SECTION_VERSION_ACCEPTED,
    /** Review FIX-1: a second core-detected GAP is suppressed (the S7 rule) instead of merged into the identity-0 record. */
    CORE_GAP_SUPPRESSED;

    /** Package-private and mutable only from this package: false in every production process. */
    boolean on;
}
