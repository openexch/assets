// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.feedharness;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * RC2 S9h 3.4: the harness file formats, all little-endian, every reader strict (a bad magic, an unknown entry type, a
 * short length, a truncation or trailing bytes refuse the whole file with {@link Refused}).
 *
 * <ul>
 *   <li>FJR1 {@code 46 4A 52 31}, {@code u32 entryCount}, entries {@code u8 type}: 1 RECORD {@code u16 length} + bytes
 *       (length >= 8), 2 FLUSH;</li>
 *   <li>FFR1 {@code 46 46 52 31}, {@code u32 frameCount}, {@code u16 length} + bytes per money v7 frame;</li>
 *   <li>FLOG {@code 46 4C 4F 47}, {@code u32 count}, entries {@code u8 kind} (1 FRAME, 2 SESSION_OPEN, 3 SESSION_CLOSE,
 *       4 SWEEP), {@code i64 sessionId}, {@code i64 timestamp}, {@code u32 length}, bytes;</li>
 *   <li>FJC1 {@code 46 4A 43 31}, entries to the end of the file {@code u8 type}: 1 RECORDING {@code i64 ordinal},
 *       2 FRAME {@code u16 length} + bytes, 3 STOP.</li>
 * </ul>
 */
public final class FeedFiles {

    public static final byte[] FJR_MAGIC = {0x46, 0x4A, 0x52, 0x31};
    public static final byte[] FFR_MAGIC = {0x46, 0x46, 0x52, 0x31};
    public static final byte[] FLOG_MAGIC = {0x46, 0x4C, 0x4F, 0x47};
    public static final byte[] FJC_MAGIC = {0x46, 0x4A, 0x43, 0x31};

    /** An input file the harness refuses (CLI exit 65, rule H2). */
    public static final class Refused extends RuntimeException {
        public Refused(final String message) {
            super(message);
        }
    }

    /** One FJR entry: a schema 4 record, or a FLUSH marker ({@code bytes == null}). */
    public record Entry(byte[] bytes) {
        public static final Entry FLUSH = new Entry(null);

        public static Entry record(final byte[] bytes) {
            return new Entry(bytes);
        }

        public boolean isFlush() {
            return bytes == null;
        }
    }

    /** One FLOG entry. */
    public record LogEntry(int kind, long sessionId, long timestamp, byte[] bytes) {
        public static final int FRAME = 1;
        public static final int SESSION_OPEN = 2;
        public static final int SESSION_CLOSE = 3;
        public static final int SWEEP = 4;
    }

    /** One FJC entry. */
    public record CaptureEntry(int type, long ordinal, byte[] bytes) {
        public static final int RECORDING = 1;
        public static final int FRAME = 2;
        public static final int STOP = 3;
    }

    private FeedFiles() {
    }

    // ------------------------------------------------------------------ FJR

    public static byte[] encodeFjr(final List<Entry> entries) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(FJR_MAGIC);
        u32(out, entries.size());
        for (final Entry e : entries) {
            if (e.isFlush()) {
                out.write(2);
            } else {
                if (e.bytes().length < 8 || e.bytes().length > 0xFFFF) {
                    throw new IllegalArgumentException("FJR record length " + e.bytes().length);
                }
                out.write(1);
                u16(out, e.bytes().length);
                out.writeBytes(e.bytes());
            }
        }
        return out.toByteArray();
    }

    public static List<Entry> decodeFjr(final byte[] file) {
        final ByteBuffer b = open(file, FJR_MAGIC, "FJR1");
        final long count = need(b, 4, "FJR1 count") ? Integer.toUnsignedLong(b.getInt()) : 0;
        final List<Entry> out = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            need(b, 1, "FJR1 entry " + i);
            final int type = b.get() & 0xFF;
            if (type == 2) {
                out.add(Entry.FLUSH);
            } else if (type == 1) {
                need(b, 2, "FJR1 length " + i);
                final int length = b.getShort() & 0xFFFF;
                if (length < 8) {
                    throw new Refused("FJR1 entry " + i + ": record length " + length + " < 8");
                }
                need(b, length, "FJR1 record " + i);
                final byte[] r = new byte[length];
                b.get(r);
                out.add(Entry.record(r));
            } else {
                throw new Refused("FJR1 entry " + i + ": unknown type " + type);
            }
        }
        trailing(b, "FJR1");
        return out;
    }

    // ------------------------------------------------------------------ FFR

    public static byte[] encodeFfr(final List<byte[]> frames) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(FFR_MAGIC);
        u32(out, frames.size());
        for (final byte[] f : frames) {
            if (f.length > 0xFFFF) {
                throw new IllegalArgumentException("FFR frame length " + f.length);
            }
            u16(out, f.length);
            out.writeBytes(f);
        }
        return out.toByteArray();
    }

    public static List<byte[]> decodeFfr(final byte[] file) {
        final ByteBuffer b = open(file, FFR_MAGIC, "FFR1");
        need(b, 4, "FFR1 count");
        final long count = Integer.toUnsignedLong(b.getInt());
        final List<byte[]> out = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            need(b, 2, "FFR1 length " + i);
            final int length = b.getShort() & 0xFFFF;
            need(b, length, "FFR1 frame " + i);
            final byte[] f = new byte[length];
            b.get(f);
            out.add(f);
        }
        trailing(b, "FFR1");
        return out;
    }

    // ------------------------------------------------------------------ FLOG

    public static byte[] encodeFlog(final List<LogEntry> entries) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(FLOG_MAGIC);
        u32(out, entries.size());
        for (final LogEntry e : entries) {
            out.write(e.kind());
            i64(out, e.sessionId());
            i64(out, e.timestamp());
            final byte[] bytes = e.bytes() == null ? new byte[0] : e.bytes();
            u32(out, bytes.length);
            out.writeBytes(bytes);
        }
        return out.toByteArray();
    }

    public static List<LogEntry> decodeFlog(final byte[] file) {
        final ByteBuffer b = open(file, FLOG_MAGIC, "FLOG");
        need(b, 4, "FLOG count");
        final long count = Integer.toUnsignedLong(b.getInt());
        final List<LogEntry> out = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            need(b, 1 + 8 + 8 + 4, "FLOG entry " + i);
            final int kind = b.get() & 0xFF;
            if (kind < 1 || kind > 4) {
                throw new Refused("FLOG entry " + i + ": unknown kind " + kind);
            }
            final long session = b.getLong();
            final long ts = b.getLong();
            final long length = Integer.toUnsignedLong(b.getInt());
            need(b, length, "FLOG bytes " + i);
            final byte[] bytes = new byte[(int) length];
            b.get(bytes);
            out.add(new LogEntry(kind, session, ts, bytes));
        }
        trailing(b, "FLOG");
        return out;
    }

    // ------------------------------------------------------------------ FJC

    public static byte[] encodeFjc(final List<CaptureEntry> entries) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(FJC_MAGIC);
        for (final CaptureEntry e : entries) {
            out.write(e.type());
            switch (e.type()) {
                case CaptureEntry.RECORDING -> i64(out, e.ordinal());
                case CaptureEntry.FRAME -> {
                    u16(out, e.bytes().length);
                    out.writeBytes(e.bytes());
                }
                case CaptureEntry.STOP -> { }
                default -> throw new IllegalArgumentException("FJC type " + e.type());
            }
        }
        return out.toByteArray();
    }

    public static List<CaptureEntry> decodeFjc(final byte[] file) {
        final ByteBuffer b = open(file, FJC_MAGIC, "FJC1");
        final List<CaptureEntry> out = new ArrayList<>();
        int i = 0;
        while (b.hasRemaining()) {
            final int type = b.get() & 0xFF;
            switch (type) {
                case CaptureEntry.RECORDING -> {
                    need(b, 8, "FJC1 ordinal " + i);
                    out.add(new CaptureEntry(type, b.getLong(), null));
                }
                case CaptureEntry.FRAME -> {
                    need(b, 2, "FJC1 length " + i);
                    final int length = b.getShort() & 0xFFFF;
                    need(b, length, "FJC1 frame " + i);
                    final byte[] f = new byte[length];
                    b.get(f);
                    out.add(new CaptureEntry(type, 0L, f));
                }
                case CaptureEntry.STOP -> out.add(new CaptureEntry(type, 0L, null));
                default -> throw new Refused("FJC1 entry " + i + ": unknown type " + type);
            }
            i++;
        }
        return out;
    }

    // ------------------------------------------------------------------ SUMS and atomic writes

    public static String sha256Hex(final byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@code sha256sum} format, names sorted: {@code <hex>  <name>\n}. */
    public static String sums(final Map<String, byte[]> files) {
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, byte[]> e : new TreeMap<>(files).entrySet()) {
            sb.append(sha256Hex(e.getValue())).append("  ").append(e.getKey()).append('\n');
        }
        return sb.toString();
    }

    /** Parses a {@code SHA256SUMS} file into name -> hex (strict: two spaces, 64 lower-case hex). */
    public static Map<String, String> parseSums(final String text) {
        final Map<String, String> out = new TreeMap<>();
        for (final String line : text.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            if (line.length() < 67 || line.charAt(64) != ' ' || line.charAt(65) != ' '
                    || !line.substring(0, 64).matches("[0-9a-f]{64}")) {
                throw new Refused("SHA256SUMS line malformed: " + line);
            }
            out.put(line.substring(66), line.substring(0, 64));
        }
        return out;
    }

    /**
     * Verifies every file a SUMS names in {@code dir} (and that the SUMS itself hashes to {@code sumsSha256} when
     * given). Returns the verified file contents by name.
     */
    public static Map<String, byte[]> verifySums(final Path dir, final String sumsSha256) throws IOException {
        final Path sumsPath = dir.resolve("SHA256SUMS");
        if (!Files.isRegularFile(sumsPath)) {
            throw new Refused("no SHA256SUMS in " + dir);
        }
        final byte[] sums = Files.readAllBytes(sumsPath);
        if (sumsSha256 != null && !sha256Hex(sums).equals(sumsSha256)) {
            throw new Refused("SHA256SUMS of " + dir + " is " + sha256Hex(sums) + ", pinned " + sumsSha256);
        }
        final Map<String, byte[]> out = new TreeMap<>();
        for (final Map.Entry<String, String> e : parseSums(new String(sums, StandardCharsets.US_ASCII)).entrySet()) {
            final Path p = dir.resolve(e.getKey());
            if (!Files.isRegularFile(p)) {
                throw new Refused("SHA256SUMS names a missing file " + e.getKey());
            }
            final byte[] bytes = Files.readAllBytes(p);
            if (!sha256Hex(bytes).equals(e.getValue())) {
                throw new Refused(e.getKey() + " hashes to " + sha256Hex(bytes) + ", SHA256SUMS says " + e.getValue());
            }
            out.put(e.getKey(), bytes);
        }
        return out;
    }

    /** Adds (or replaces) {@code file}'s line in the {@code SHA256SUMS} of its directory, written atomically. */
    public static void listInSums(final Path file, final byte[] bytes) throws IOException {
        final Path sums = file.resolveSibling("SHA256SUMS");
        final Map<String, String> lines = Files.isRegularFile(sums)
                ? parseSums(Files.readString(sums, StandardCharsets.US_ASCII)) : new TreeMap<>();
        lines.put(file.getFileName().toString(), sha256Hex(bytes));
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> e : new TreeMap<>(lines).entrySet()) {
            sb.append(e.getValue()).append("  ").append(e.getKey()).append('\n');
        }
        writeAtomic(sums, sb.toString().getBytes(StandardCharsets.US_ASCII));
    }

    /** Writes {@code name.tmp} then renames, so a killed run never leaves a partial output file. */
    public static void writeAtomic(final Path target, final byte[] bytes) throws IOException {
        final Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.write(tmp, bytes);
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    // ------------------------------------------------------------------ primitives

    private static ByteBuffer open(final byte[] file, final byte[] magic, final String name) {
        if (file.length < 4 || file[0] != magic[0] || file[1] != magic[1] || file[2] != magic[2] || file[3] != magic[3]) {
            throw new Refused(name + ": bad magic");
        }
        final ByteBuffer b = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        b.position(4);
        return b;
    }

    private static boolean need(final ByteBuffer b, final long n, final String what) {
        if (b.remaining() < n) {
            throw new Refused(what + ": truncated");
        }
        return true;
    }

    private static void trailing(final ByteBuffer b, final String name) {
        if (b.hasRemaining()) {
            throw new Refused(name + ": " + b.remaining() + " trailing bytes");
        }
    }

    private static void u16(final ByteArrayOutputStream out, final int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void u32(final ByteArrayOutputStream out, final long v) {
        for (int i = 0; i < 4; i++) {
            out.write((int) (v >>> (8 * i)) & 0xFF);
        }
    }

    private static void i64(final ByteArrayOutputStream out, final long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) (v >>> (8 * i)) & 0xFF);
        }
    }
}
