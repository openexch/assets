// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 (contract 6.5, review FIX-7): the structural guard that keeps the applier the only owner of replicated state.
 * In {@link AssetsClusteredService}'s source (comments and string literals stripped) every {@code engine.}/
 * {@code engine::} and {@code demuxer.}/{@code demuxer::} access must be a read in the allowlist, and the service must
 * neither build an engine, projector, demuxer or envelope nor call the snapshot codec. The type names
 * {@code SnapshotEnvelope} (the static {@code encodeSnapshot} parameter) and {@code BalanceSnapshotCodec.Decoded} (the
 * restore log line) are allowed; constructor calls, codec calls and the old envelope field are not. By reflection, no
 * public signature of {@link AssetsLogApplier} carries a transport type (G7).
 */
public class ApplierOwnershipGuardTest {

    /**
     * The guarded source; {@code -Drcs.guard.source=<file>} points the same scan at another copy (how the "fails on
     * a9a865d" claim is reproduced: {@code git show a9a865d:<path> > /tmp/ACS.java}).
     */
    static final Path SERVICE_SOURCE = Paths.get(System.getProperty("rcs.guard.source",
            "src/main/java/com/openexchange/assets/infrastructure/persistence/AssetsClusteredService.java"));

    /** Read-only engine accesses the service may keep (metrics, logs, arming, the feed seed). */
    static final Set<String> ENGINE_ALLOWED = Set.of(
            "accountCount", "getLastAppliedTradeId", "getConsumePosition", "getJournalSeq", "isMoneyJournalEnabled",
            "getSettleFaultCount", "getSuppressedFeedReleaseCount", "getExhaustedHoldsReaped", "getSettleRejectCount",
            "getLastLogTime", "sessions().size", "gate().state", "deadlines().size", "deadlines().isEmpty",
            "forEachAccountByUserId", "orders().orders", "metricsView"); // RC2 S8 3.10 item 3: the read-only metrics view
    /** Engine members that are sub-component accessors: the access is judged one call deeper. */
    static final Set<String> ENGINE_COMPONENTS = Set.of("sessions", "gate", "deadlines", "orders");
    static final Set<String> DEMUXER_ALLOWED = Set.of(
            "schemaRejectCount", "decodeRejectCount", "applyErrorCount", "unknownTemplateCount", "unhandledTemplateCount",
            "unauthorisedCount");

    private static final Pattern ACCESS = Pattern.compile(
            "\\b(engine|demuxer)\\s*(?:\\(\\s*\\))?\\s*(?:\\.|::)\\s*([A-Za-z_$][A-Za-z0-9_$]*)(\\s*\\(\\s*\\)\\s*\\.\\s*([A-Za-z_$][A-Za-z0-9_$]*))?");
    /** Reaching the applier's engine or demuxer from the service: only the two alias field declarations may. */
    private static final Pattern APPLIER_ACCESSOR = Pattern.compile(
            "\\b(?:this\\s*\\.\\s*)?applier\\s*(?:\\(\\s*\\))?\\s*\\.\\s*(engine|demuxer)\\s*\\(");
    /** The alias declarations ({@code private final ... = applier.engine();} and {@code ... demuxer();}). */
    static final int APPLIER_ACCESSOR_EXEMPT = 2;
    private static final Pattern[] FORBIDDEN = {
            Pattern.compile("new\\s+(?:[\\w.]+\\.)?AssetsEngine\\s*\\("),
            Pattern.compile("new\\s+(?:[\\w.]+\\.)?SettlementProjector\\s*\\("),
            Pattern.compile("new\\s+(?:[\\w.]+\\.)?AssetsSbeDemuxer\\s*\\("),
            Pattern.compile("\\bBalanceSnapshotCodec\\s*\\.\\s*(?!Decoded\\b)"),
            Pattern.compile("\\bSnapshotEnvelope\\s*\\("),
            Pattern.compile("\\bsnapshotEnvelope\\b"),
            Pattern.compile("=\\s*(engine|demuxer)\\s*;"), // no local alias of either field
    };

    /** Comments, string, char and text-block literals replaced by spaces; newlines kept so line numbers survive. */
    static String strip(final String src) {
        final StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        final int n = src.length();
        while (i < n) {
            final char c = src.charAt(i);
            final char d = i + 1 < n ? src.charAt(i + 1) : '\0';
            if (c == '/' && d == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && d == '*') {
                out.append("  ");
                i += 2;
                while (i < n && !(src.charAt(i) == '*' && i + 1 < n && src.charAt(i + 1) == '/')) {
                    out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                out.append("  ");
                i += 2;
            } else if (c == '"' && src.startsWith("\"\"\"", i)) {
                out.append("   ");
                i += 3;
                while (i < n && !src.startsWith("\"\"\"", i)) {
                    out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                out.append("   ");
                i += 3;
            } else if (c == '"' || c == '\'') {
                out.append(' ');
                i++;
                while (i < n && src.charAt(i) != c) {
                    if (src.charAt(i) == '\\') {
                        out.append(' ');
                        i++;
                    }
                    out.append(' ');
                    i++;
                }
                out.append(' ');
                i++;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static int lineOf(final String text, final int index) {
        int line = 1;
        for (int i = 0; i < index; i++) {
            if (text.charAt(i) == '\n') {
                line++;
            }
        }
        return line;
    }

    /**
     * {@code engine}/{@code demuxer} right after a dot is a name segment (an import, a fully qualified type) unless the
     * dot follows {@code this}, {@code applier} or {@code applier()}: those are accesses to the field or accessor.
     */
    private static boolean qualifiedName(final String code, final int start) {
        int i = skipBack(code, start - 1);
        if (i < 0 || code.charAt(i) != '.') {
            return false;
        }
        i = skipBack(code, i - 1);
        if (i >= 0 && code.charAt(i) == ')') { // applier() .engine
            i = skipBack(code, i - 1);
            if (i < 0 || code.charAt(i) != '(') {
                return true;
            }
            i = skipBack(code, i - 1);
        }
        final int end = i + 1;
        while (i >= 0 && Character.isJavaIdentifierPart(code.charAt(i))) {
            i--;
        }
        final String owner = code.substring(i + 1, end);
        return !(owner.equals("this") || owner.equals("applier"));
    }

    private static int skipBack(final String code, final int from) {
        int i = from;
        while (i >= 0 && Character.isWhitespace(code.charAt(i))) {
            i--;
        }
        return i;
    }

    private static String lineText(final String code, final int index) {
        final int s = code.lastIndexOf('\n', index) + 1;
        final int e = code.indexOf('\n', index);
        return code.substring(s, e < 0 ? code.length() : e).trim();
    }

    /** Applier accessor calls on {@code private final} declaration lines: the alias fields, exempt but counted. */
    static int applierAccessorExemptions(final String source) {
        final String code = strip(source);
        final Matcher a = APPLIER_ACCESSOR.matcher(code);
        int n = 0;
        while (a.find()) {
            if (lineText(code, a.start()).startsWith("private final")) {
                n++;
            }
        }
        return n;
    }

    /** Every violation in {@code source}, as {@code "line N: what"}, in source order. */
    static List<String> violations(final String source) {
        final String code = strip(source);
        final List<String> out = new ArrayList<>();
        final Matcher m = ACCESS.matcher(code);
        while (m.find()) {
            if (qualifiedName(code, m.start())) {
                continue; // a package segment (import, fully qualified type), not the field
            }
            final String owner = m.group(1);
            final boolean accessor = m.group(0).substring(owner.length()).trim().startsWith("(");
            String member = m.group(2);
            if (owner.equals("engine") && ENGINE_COMPONENTS.contains(member) && m.group(4) != null) {
                member = member + "()." + m.group(4);
            }
            final boolean allowed = owner.equals("engine") ? ENGINE_ALLOWED.contains(member) : DEMUXER_ALLOWED.contains(member);
            if (!allowed) {
                out.add("line " + lineOf(code, m.start()) + ": " + owner + (accessor ? "()" : "") + "." + member);
            }
        }
        final Matcher acc = APPLIER_ACCESSOR.matcher(code);
        while (acc.find()) {
            if (!lineText(code, acc.start()).startsWith("private final")) {
                out.add("line " + lineOf(code, acc.start()) + ": forbidden " + acc.group().trim());
            }
        }
        for (final Pattern p : FORBIDDEN) {
            final Matcher f = p.matcher(code);
            while (f.find()) {
                out.add("line " + lineOf(code, f.start()) + ": forbidden " + f.group().trim());
            }
        }
        out.sort((a, b) -> Integer.compare(Integer.parseInt(a.substring(5, a.indexOf(':'))),
                Integer.parseInt(b.substring(5, b.indexOf(':')))));
        return out;
    }

    @Test
    public void theServiceReadsTheEngineOnlyThroughTheAllowlist() throws IOException {
        assertTrue("run from the assets-cluster module directory: " + SERVICE_SOURCE.toAbsolutePath(),
                Files.isRegularFile(SERVICE_SOURCE));
        final String source = Files.readString(SERVICE_SOURCE, StandardCharsets.UTF_8);
        assertEquals("replicated state is mutated only through AssetsLogApplier", List.of(), violations(source));
        assertEquals("only the two alias declarations reach the applier's engine/demuxer", APPLIER_ACCESSOR_EXEMPT,
                applierAccessorExemptions(source));
        assertTrue("the service delegates to an applier", strip(source).contains("new AssetsLogApplier("));
    }

    @Test
    public void theScannerSeesWhatItGuards() {
        final String sample = String.join("\n",
                "class X {",
                "  // engine.sweep(1) in a comment is fine",
                "  String s = \"engine.sweep(1)\";",
                "  void a() { engine.sweep(t); }",
                "  void b() { engine.sessions().open(1, p); engine.sessions().size(); }",
                "  void c() { demuxer.dispatch(1, b, 0, 1, 2); demuxer::schemaRejectCount; }",
                "  final SnapshotEnvelope snapshotEnvelope = new SnapshotEnvelope();",
                "  BalanceSnapshotCodec.Decoded d = BalanceSnapshotCodec.deserialize(b, 0, 1, engine);",
                "  AssetsEngine e = new AssetsEngine(); Object o = new com.x.SettlementProjector(e);",
                "  static int encode(AssetsEngine engine, SnapshotEnvelope envelope) { return 0; }",
                "  Object q = applier.engine(); int k = engine.accountCount(); this.engine.sweep(1);",
                "  com.openexchange.assets.application.engine.gate.OrderEntryGate g; x.demuxer.y();",
                "  void d() { engine().sweep(timestamp); }",
                "  void e() { applier.engine().sessions().open(id, principal); }",
                "  void f() { applier.demuxer().dispatch(-1L, buffer, offset, length, timestamp); }",
                "  void g() { final AssetsEngine e = engine; e.setLogPosition(p); }",
                "  private final AssetsEngine engine = applier.engine(); int r = engine().accountCount();",
                "}");
        assertEquals(List.of(
                "line 4: engine.sweep",
                "line 5: engine.sessions().open",
                "line 6: demuxer.dispatch",
                "line 7: forbidden SnapshotEnvelope(",
                "line 7: forbidden snapshotEnvelope",
                "line 8: forbidden BalanceSnapshotCodec.",
                "line 9: forbidden new AssetsEngine(",
                "line 9: forbidden new com.x.SettlementProjector(",
                "line 11: engine.sweep",
                "line 11: forbidden applier.engine(",
                "line 13: engine().sweep",
                "line 14: engine().sessions().open",
                "line 14: forbidden applier.engine(",
                "line 15: demuxer().dispatch",
                "line 15: forbidden applier.demuxer(",
                "line 16: forbidden = engine;"), violations(sample));
        assertEquals("the alias declaration is exempt and counted", 1, applierAccessorExemptions(sample));
    }

    private static final Set<String> FORBIDDEN_PACKAGES = Set.of("io.aeron.cluster.service", "io.aeron.logbuffer");
    private static final Set<String> FORBIDDEN_TYPES = Set.of(
            "io.aeron.Publication", "io.aeron.ExclusivePublication", "io.aeron.ConcurrentPublication", "io.aeron.Image",
            "io.aeron.cluster.service.Cluster", "io.aeron.cluster.service.ClientSession");
    private static final List<String> ALLOWED_PREFIXES = List.of(
            "org.agrona.", "com.openexchange.assets.", "java.lang.", "io.aeron.cluster.codecs.CloseReason",
            "java.util.concurrent.TimeUnit");

    private static void checkType(final List<String> out, final String where, final Class<?> type) {
        Class<?> t = type;
        while (t.isArray()) {
            t = t.getComponentType();
        }
        if (t.isPrimitive()) {
            return;
        }
        final String name = t.getName();
        final String pkg = t.getPackageName();
        if (FORBIDDEN_PACKAGES.contains(pkg) || FORBIDDEN_TYPES.contains(name)
                || ALLOWED_PREFIXES.stream().noneMatch(name::startsWith)) {
            out.add(where + " uses " + name);
        }
    }

    @Test
    public void noPublicSignatureOfTheApplierCarriesATransportType() {
        final List<String> out = new ArrayList<>();
        final List<Executable> members = new ArrayList<>();
        for (final Method m : AssetsLogApplier.class.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers())) {
                members.add(m);
                checkType(out, m.getName() + " return", m.getReturnType());
            }
        }
        for (final Constructor<?> c : AssetsLogApplier.class.getDeclaredConstructors()) {
            if (Modifier.isPublic(c.getModifiers())) {
                members.add(c);
            }
        }
        for (final Executable e : members) {
            for (final Class<?> p : e.getParameterTypes()) {
                checkType(out, e.getName() + " parameter", p);
            }
        }
        for (final Field f : AssetsLogApplier.class.getDeclaredFields()) {
            if (Modifier.isPublic(f.getModifiers())) {
                checkType(out, f.getName() + " field", f.getType());
            }
        }
        assertEquals("G7: the replica-facing surface is transport-free", List.of(), out);
        assertTrue("the surface was inspected", members.size() > 15);
    }
}
