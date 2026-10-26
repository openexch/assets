// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.generated.CircuitBreakerDecoder;
import com.openexchange.assets.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderDecoder;
import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateDecoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * RC1 S8 (harness B5 adversarial): turns a VALID encoded frame into a hostile one. Every mutant carries its buffer and
 * its declared length separately, and the buffer keeps stale bytes beyond the declared length (the original frame's
 * tail plus garbage), which is what a reused receive buffer looks like: a decoder that reads past the frame finds
 * plausible bytes, never zeros.
 *
 * <p>Categories: MUST_DROP mutants are malformed by construction and must be dropped by the fence (a counter moves,
 * nothing else). OBSERVED mutants may or may not stay well-formed (a bit flip can produce another valid command); they
 * are judged in a sandbox. EQUIVALENT mutants are well-formed re-encodings of the same command (wider blocks, trailing
 * bytes); when the gate admits one, its effect must equal the original's.</p>
 */
final class FrameMutator {

    static final int HEADER = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int GROUP_HEADER = 4;
    private static final int SCHEMA_ID = MessageHeaderDecoder.SCHEMA_ID;
    private static final int VERSION = MessageHeaderDecoder.SCHEMA_VERSION;
    private static final int STALE_TAIL = 96;

    enum Judgement { MUST_DROP, OBSERVED, EQUIVALENT }

    enum Category {
        TRUNCATE(Judgement.MUST_DROP),
        LENGTH_BEYOND_BUFFER(Judgement.MUST_DROP),
        BLOCK_SHORT(Judgement.MUST_DROP),
        BLOCK_BEYOND_FRAME(Judgement.MUST_DROP),
        WRONG_SCHEMA_ID(Judgement.MUST_DROP),
        WRONG_VERSION(Judgement.MUST_DROP),
        TEMPLATE_UNKNOWN(Judgement.MUST_DROP),
        GROUP_COUNT_BEYOND_FRAME(Judgement.MUST_DROP),
        GROUP_BLOCK_SHORT(Judgement.MUST_DROP),
        INVALID_ENUM(Judgement.MUST_DROP),
        INVALID_ENUM_WIDE_BLOCK(Judgement.MUST_DROP),
        TEMPLATE_KNOWN_SWAP(Judgement.OBSERVED),
        BITFLIP_HEADER(Judgement.OBSERVED),
        BITFLIP_BODY(Judgement.OBSERVED),
        WIDE_BLOCK_VALID(Judgement.EQUIVALENT),
        TRAILING_BYTES(Judgement.EQUIVALENT);

        final Judgement judgement;

        Category(final Judgement judgement) {
            this.judgement = judgement;
        }
    }

    /** A hostile frame: dispatch {@code buffer[0, length)}; {@code original} is the valid frame it came from. */
    record Mutant(Category category, byte[] buffer, int length, byte[] original, String note) { }

    /** An enum/raw-validated field: offset within the root block (or group entry), width 1 or 4, valid inclusive range. */
    private record Field(String name, int offset, int width, int min, int max) { }

    private static final int T_BATCH = MeJournalTradeBatchDecoder.TEMPLATE_ID;
    private static final int T_CONFIG = CoreConfigDecoder.TEMPLATE_ID;
    private static final int[] KNOWN = knownTemplates();

    private final Random rnd;

    FrameMutator(final Random rnd) {
        this.rnd = rnd;
    }

    private static int[] knownTemplates() {
        final List<Integer> ids = new ArrayList<>();
        for (int t = 0; t < 65_536; t++) {
            if (AssetsSbeDemuxer.minimumBlockLength(t) >= 0) {
                ids.add(t);
            }
        }
        return ids.stream().mapToInt(Integer::intValue).toArray();
    }

    static int templateOf(final byte[] f) {
        return u16(f, 2);
    }

    private static int u16(final byte[] b, final int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8;
    }

    private static void put16(final byte[] b, final int at, final int v) {
        b[at] = (byte) v;
        b[at + 1] = (byte) (v >>> 8);
    }

    private static void put32(final byte[] b, final int at, final int v) {
        for (int i = 0; i < 4; i++) {
            b[at + i] = (byte) (v >>> (8 * i));
        }
    }

    private static List<Field> rootFields(final int template) {
        return switch (template) {
            case DepositDecoder.TEMPLATE_ID -> List.of(new Field("assetId", DepositDecoder.assetIdEncodingOffset(), 4, 0, 5));
            case WithdrawDecoder.TEMPLATE_ID -> List.of(new Field("assetId", WithdrawDecoder.assetIdEncodingOffset(), 4, 0, 5));
            case CreateOrderDecoder.TEMPLATE_ID -> List.of(new Field("side", CreateOrderDecoder.sideEncodingOffset(), 1, 0, 1),
                    new Field("orderType", CreateOrderDecoder.orderTypeEncodingOffset(), 1, 0, 6),
                    new Field("timeInForce", CreateOrderDecoder.timeInForceEncodingOffset(), 1, 0, 3));
            case MeJournalTradeDecoder.TEMPLATE_ID -> List.of(new Field("takerIsBuy", MeJournalTradeDecoder.takerIsBuyEncodingOffset(), 1, 0, 1));
            case MeJournalTerminalDecoder.TEMPLATE_ID -> List.of(new Field("status", MeJournalTerminalDecoder.statusEncodingOffset(), 1, 2, 5));
            case MeCommandOutcomeDecoder.TEMPLATE_ID -> List.of(
                    new Field("commandKind", MeCommandOutcomeDecoder.commandKindEncodingOffset(), 1, 0, 2),
                    new Field("result", MeCommandOutcomeDecoder.resultEncodingOffset(), 4, 0, 6));
            case CircuitBreakerDecoder.TEMPLATE_ID -> List.of(new Field("action", CircuitBreakerDecoder.actionEncodingOffset(), 1, 0, 1));
            case OrderEntryGateDecoder.TEMPLATE_ID -> List.of(new Field("state", OrderEntryGateDecoder.stateEncodingOffset(), 1, 0, 1),
                    new Field("reason", OrderEntryGateDecoder.reasonEncodingOffset(), 1, 0, 6));
            case RegisterSessionDecoder.TEMPLATE_ID -> List.of(
                    new Field("sessionClass", RegisterSessionDecoder.sessionClassEncodingOffset(), 1, 0, 6),
                    new Field("resumable", RegisterSessionDecoder.resumableEncodingOffset(), 1, 0, 1));
            default -> List.of();
        };
    }

    private static int groupCount(final int template) {
        return template == T_BATCH ? 1 : template == T_CONFIG ? 2 : 0;
    }

    /** {groupHeaderOffset, entryBlock, count} for every group of the frame, from the frame's own dimensions. */
    static List<int[]> groups(final byte[] f) {
        final List<int[]> out = new ArrayList<>();
        int at = HEADER + u16(f, 0);
        for (int g = 0; g < groupCount(templateOf(f)); g++) {
            final int block = u16(f, at);
            final int count = u16(f, at + 2);
            out.add(new int[] {at, block, count});
            at += GROUP_HEADER + block * count;
        }
        return out;
    }

    /** Can {@code c} be applied to frame {@code f}? */
    static boolean applicable(final Category c, final byte[] f) {
        final int t = templateOf(f);
        return switch (c) {
            case GROUP_COUNT_BEYOND_FRAME, GROUP_BLOCK_SHORT -> groupCount(t) > 0;
            case BLOCK_SHORT -> AssetsSbeDemuxer.minimumBlockLength(t) > 0;
            case INVALID_ENUM, INVALID_ENUM_WIDE_BLOCK -> !rootFields(t).isEmpty() || groupCount(t) > 0;
            default -> true;
        };
    }

    /** The frame re-encoded with every block (root and group entries) wider by {@code w} zero bytes: same command. */
    static byte[] widen(final byte[] f, final int w) {
        return widen(f, w, w);
    }

    /** The frame with the root block wider by {@code wRoot} and every group entry wider by {@code w}: same command. */
    static byte[] widen(final byte[] f, final int wRoot, final int w) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] header = Arrays.copyOf(f, HEADER);
        final int block = u16(f, 0);
        put16(header, 0, block + wRoot);
        out.writeBytes(header);
        out.write(f, HEADER, block);
        out.writeBytes(new byte[wRoot]);
        int at = HEADER + block;
        for (int g = 0; g < groupCount(templateOf(f)); g++) {
            final int gb = u16(f, at);
            final int count = u16(f, at + 2);
            final byte[] gh = new byte[GROUP_HEADER];
            put16(gh, 0, gb + w);
            put16(gh, 2, count);
            out.writeBytes(gh);
            for (int i = 0; i < count; i++) {
                out.write(f, at + GROUP_HEADER + i * gb, gb);
                out.writeBytes(new byte[w]);
            }
            at += GROUP_HEADER + gb * count;
        }
        out.write(f, at, f.length - at);
        return out.toByteArray();
    }

    /** {@code frame} followed by stale bytes: the original again, then garbage. */
    private byte[] withStaleTail(final byte[] frame, final byte[] original) {
        final byte[] b = Arrays.copyOf(frame, frame.length + original.length + STALE_TAIL);
        System.arraycopy(original, 0, b, frame.length, original.length);
        for (int i = frame.length + original.length; i < b.length; i++) {
            b[i] = (byte) rnd.nextInt(256);
        }
        return b;
    }

    private Mutant mutant(final Category c, final byte[] mutated, final byte[] original, final String note) {
        return new Mutant(c, withStaleTail(mutated, original), mutated.length, original, note);
    }

    /** Every strictly shorter length of {@code f}, with the full original still in the buffer behind the cut. */
    List<Mutant> truncations(final byte[] f) {
        final List<Mutant> out = new ArrayList<>();
        final byte[] buffer = withStaleTail(f, f);
        for (int len = 0; len < f.length; len++) {
            out.add(new Mutant(Category.TRUNCATE, buffer, len, f, "length " + len + " of " + f.length));
        }
        return out;
    }

    private int invalidValue(final Field fd) {
        if (fd.width() == 1) {
            int v;
            do {
                v = rnd.nextInt(256);
            } while (v >= fd.min() && v <= fd.max());
            return v;
        }
        return rnd.nextBoolean() ? -1 - rnd.nextInt(1_000_000) : fd.max() + 1 + rnd.nextInt(1_000_000);
    }

    /** Poison one raw-validated byte (root enum, batch entry flag, or a CoreConfig symbol/code character). */
    private String poison(final byte[] m) {
        final int t = templateOf(m);
        final List<Field> root = rootFields(t);
        if (!root.isEmpty()) {
            final Field fd = root.get(rnd.nextInt(root.size()));
            final int v = invalidValue(fd);
            if (fd.width() == 1) {
                m[HEADER + fd.offset()] = (byte) v;
            } else {
                put32(m, HEADER + fd.offset(), v);
            }
            return fd.name() + "=" + v;
        }
        final List<int[]> gs = groups(m);
        if (t == T_BATCH) {
            final int[] g = gs.get(0);
            if (g[2] == 0) {
                return "empty batch (nothing to poison)";
            }
            final int k = rnd.nextInt(g[2]);
            final int v = invalidValue(new Field("takerIsBuy", 0, 1, 0, 1));
            m[g[0] + GROUP_HEADER + k * g[1] + MeJournalTradeBatchDecoder.TradesDecoder.takerIsBuyEncodingOffset()] = (byte) v;
            return "trades[" + k + "].takerIsBuy=" + v;
        }
        final boolean market = rnd.nextBoolean() && gs.get(0)[2] > 0;
        final int[] g = market ? gs.get(0) : gs.get(1);
        final int k = rnd.nextInt(g[2]);
        final int fieldAt = g[0] + GROUP_HEADER + k * g[1]
                + (market ? CoreConfigDecoder.MarketsDecoder.symbolEncodingOffset() : CoreConfigDecoder.AssetsDecoder.codeEncodingOffset());
        int len = 0;
        while (len < (market ? 16 : 8) && m[fieldAt + len] != 0) {
            len++;
        }
        final int j = rnd.nextInt(Math.max(1, len));
        final int v = rnd.nextBoolean() ? 1 + rnd.nextInt(0x20) : 0x7F + rnd.nextInt(0x81);
        m[fieldAt + j] = (byte) v;
        return (market ? "markets[" : "assets[") + k + "]." + (market ? "symbol[" : "code[") + j + "]=0x" + Integer.toHexString(v);
    }

    /** Independent root and entry widenings in 8-byte steps (root 0..128, entries 0..32), never both zero. */
    private int[] widths() {
        int root;
        int entry;
        do {
            root = 8 * rnd.nextInt(17);
            entry = 8 * rnd.nextInt(5);
        } while (root == 0 && entry == 0);
        return new int[] {root, entry};
    }

    Mutant mutate(final Category c, final byte[] f) {
        final byte[] m = f.clone();
        final int min = AssetsSbeDemuxer.minimumBlockLength(templateOf(f));
        switch (c) {
            case LENGTH_BEYOND_BUFFER: {
                final int extra = 1 + rnd.nextInt(64);
                return new Mutant(c, f.clone(), f.length + extra, f, "length " + (f.length + extra) + " over a " + f.length + " byte buffer");
            }
            case BLOCK_SHORT: {
                final int v = rnd.nextInt(min);
                put16(m, 0, v);
                return mutant(c, m, f, "blockLength " + v + " < " + min);
            }
            case BLOCK_BEYOND_FRAME: {
                final int v = Math.min(65_535, f.length - HEADER + 1 + rnd.nextInt(512));
                put16(m, 0, v);
                return mutant(c, m, f, "blockLength " + v + " > frame body " + (f.length - HEADER));
            }
            case WRONG_SCHEMA_ID: {
                int v;
                do {
                    v = rnd.nextInt(65_536);
                } while (v == SCHEMA_ID);
                put16(m, 4, v);
                return mutant(c, m, f, "schemaId " + v);
            }
            case WRONG_VERSION: {
                int v;
                do {
                    v = rnd.nextBoolean() ? rnd.nextInt(16) : rnd.nextInt(65_536);
                } while (v == VERSION);
                put16(m, 6, v);
                return mutant(c, m, f, "version " + v);
            }
            case TEMPLATE_UNKNOWN: {
                int v;
                do {
                    v = rnd.nextBoolean() ? rnd.nextInt(256) : rnd.nextInt(65_536);
                } while (AssetsSbeDemuxer.minimumBlockLength(v) >= 0);
                put16(m, 2, v);
                return mutant(c, m, f, "templateId " + v);
            }
            case TEMPLATE_KNOWN_SWAP: {
                int v;
                do {
                    v = KNOWN[rnd.nextInt(KNOWN.length)];
                } while (v == templateOf(f));
                put16(m, 2, v);
                return mutant(c, m, f, "templateId " + templateOf(f) + " -> " + v);
            }
            case GROUP_COUNT_BEYOND_FRAME: {
                final List<int[]> gs = groups(f);
                final int[] g = gs.get(rnd.nextInt(gs.size()));
                final int remaining = f.length - g[0] - GROUP_HEADER;
                final int v = (int) Math.min(65_535L, (long) remaining / g[1] + 1 + rnd.nextInt(2_000));
                put16(m, g[0] + 2, v);
                return mutant(c, m, f, "group@" + g[0] + " count " + g[2] + " -> " + v + " (entry " + g[1] + " B, " + remaining + " B left)");
            }
            case GROUP_BLOCK_SHORT: {
                final List<int[]> gs = groups(f);
                final int[] g = gs.get(rnd.nextInt(gs.size()));
                final int v = rnd.nextInt(g[1]);
                put16(m, g[0], v);
                return mutant(c, m, f, "group@" + g[0] + " blockLength " + g[1] + " -> " + v);
            }
            case INVALID_ENUM:
                return mutant(c, m, f, poison(m));
            case INVALID_ENUM_WIDE_BLOCK: {
                final int[] wd = widths();
                final byte[] w = widen(f, wd[0], wd[1]);
                return mutant(c, w, f, "root +" + wd[0] + " B, entries +" + wd[1] + " B, " + poison(w));
            }
            case BITFLIP_HEADER: {
                final StringBuilder note = new StringBuilder("flips");
                for (int i = 0, n = 1 + rnd.nextInt(2); i < n; i++) {
                    final int bit = rnd.nextInt(HEADER * 8);
                    m[bit >>> 3] ^= (byte) (1 << (bit & 7));
                    note.append(' ').append(bit);
                }
                return mutant(c, m, f, note.toString());
            }
            case BITFLIP_BODY: {
                final StringBuilder note = new StringBuilder("flips");
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    final int bit = HEADER * 8 + rnd.nextInt((f.length - HEADER) * 8);
                    m[bit >>> 3] ^= (byte) (1 << (bit & 7));
                    note.append(' ').append(bit);
                }
                return mutant(c, m, f, note.toString());
            }
            case WIDE_BLOCK_VALID: {
                final int[] wd = widths();
                final byte[] w = widen(f, wd[0], wd[1]);
                return mutant(c, w, f, "root +" + wd[0] + " B, entries +" + wd[1] + " B");
            }
            case TRAILING_BYTES: {
                final byte[] t = Arrays.copyOf(f, f.length + 1 + rnd.nextInt(48));
                for (int i = f.length; i < t.length; i++) {
                    t[i] = (byte) rnd.nextInt(256);
                }
                return mutant(c, t, f, (t.length - f.length) + " trailing bytes");
            }
            default:
                throw new IllegalArgumentException("category " + c);
        }
    }
}
