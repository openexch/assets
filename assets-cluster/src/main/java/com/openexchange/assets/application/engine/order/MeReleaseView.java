// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.application.engine.order;

/**
 * RC2 S8 section 2: one MeCommandRelease (67), its root and up to {@link #CAP} entries in primitive arrays. Reusable and
 * mutable (zero allocation on the demuxer path); read during one apply only.
 */
public final class MeReleaseView {

    /** The frozen v7 cap of entries per frame (RC2 S1 4.3). */
    public static final int CAP = 48;

    public long egressSeq;
    public long journalSeq;
    public long meEpoch;
    public long releaseSeq;
    public long ledgerSize;
    public long ledgerCapacity;
    public long tombstoneRanges;
    public int count;
    public final long[] commandIdHigh = new long[CAP];
    public final long[] idLowFloor = new long[CAP];
    public final int[] scope = new int[CAP];
    public final int[] result = new int[CAP];
    public final int[] removed = new int[CAP];

    public MeReleaseView reset() {
        egressSeq = 0; journalSeq = 0; meEpoch = 0; releaseSeq = 0; ledgerSize = 0; ledgerCapacity = 0; tombstoneRanges = 0;
        count = 0;
        return this;
    }

    /** Append one entry; returns this. */
    public MeReleaseView add(final long high, final long floor, final int scopeCode, final int resultCode, final int removedCount) {
        if (count == CAP) {
            throw new IllegalStateException("a 67 carries at most " + CAP + " entries");
        }
        commandIdHigh[count] = high;
        idLowFloor[count] = floor;
        scope[count] = scopeCode;
        result[count] = resultCode;
        removed[count] = removedCount;
        count++;
        return this;
    }
}
