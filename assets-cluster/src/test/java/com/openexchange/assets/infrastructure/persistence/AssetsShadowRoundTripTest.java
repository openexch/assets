// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.infrastructure.persistence.AeShadowRig.Arm;
import com.openexchange.assets.infrastructure.persistence.AeShadowRig.Log;
import com.openexchange.cluster.StagingArchive;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowAlias;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowImageLoader;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowReplay;
import com.openexchange.cluster.shadow.ShadowTrailer;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RCS S3 11.10, the hand-off format half for the AE: a real AE shadow written through the kit writer at a
 * {@code ClusterAction(SNAPSHOT, 0)} of the S2 parity log, catalogued, deep-verified, loaded, identity-checked and
 * restored, across the local archive and across a real replication, reproduces the baseline's final bytes and event
 * suffix. Negative controls NC-7, NC-11, NC-17 and the P_s binding control, each a host step switched off.
 */
public class AssetsShadowRoundTripTest {

    static final long SEED = AssetsReplayParityTest.SEED;
    static final int FRAMES = AssetsReplayParityTest.FRAMES;
    static final int ACTION_AFTER = 1_200;
    static final int CAPACITY = 4 << 20;

    private static RecordedLogFixture fixture;
    private static Log log;
    private static byte[] baselineFinal;
    private static List<?> baselineSuffix;
    private static ShadowWriter.WriteResult written;
    private static byte[] shadowPayload;
    private static ShadowHeader shadowHeader;
    private static int baselineEventsAtAction;

    static int freeUdpPort() {
        try (DatagramSocket s = new DatagramSocket(0)) {
            return s.getLocalPort();
        } catch (final java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @BeforeClass
    public static void writeLogBaselineAndShadow() {
        fixture = new RecordedLogFixture(null, freeUdpPort());
        log = AeShadowRig.write(fixture, AeShadowRig.story(SEED, FRAMES), ACTION_AFTER, 0, 0);
        assertTrue("the action is on the log", log.actionPosition() > 0);
        // baseline: the whole log from genesis
        final Arm base = new Arm().genesis().apply(log, -1, log.actionPosition());
        baselineEventsAtAction = base.events();
        base.apply(log, log.actionPosition(), Long.MAX_VALUE);
        baselineFinal = base.image();
        baselineSuffix = List.copyOf(base.sink.events().subList(baselineEventsAtAction, base.events()));
        assertEquals(0, base.failFast[0]);
        // the shadow: a second container up to the action, serialized there, written through the kit writer
        final Arm at = new Arm().genesis().apply(log, -1, log.actionPosition());
        assertEquals(log.actionPosition(), at.container.cursor().logPosition());
        final ShadowImageBuffer image = AeShadowRig.image(at, CAPACITY);
        shadowPayload = new byte[image.payloadLength()];
        image.payloadView().getBytes(0, shadowPayload);
        final ShadowIdentitySource id = AeShadowRig.identityOf(at.applier);
        assertEquals("the story ADOPTed the genesis nonce", AeShadowRig.GENESIS, id.genesisId());
        written = AeShadowRig.writer(fixture, new ShadowCounters()).write(image, AeShadowRig.ENGINE, ShadowKind.ALIGNED,
                at.container.cursor(), id);
        assertTrue(written.toString(), written.ok());
    }

    @AfterClass
    public static void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    /** What a host did with one candidate, step by step (8.2 step 5). */
    record Outcome(String refusedAt, byte[] finalBytes, List<?> suffix) {
        boolean accepted() {
            return refusedAt == null;
        }
    }

    /** Host switches: each one turns a mandatory host step off (a negative control), all on in production. */
    record Host(boolean check, boolean loaderCrc, boolean crossCheck) {
        static final Host PRODUCTION = new Host(true, true, true);
    }

    /**
     * The host procedure: load (CRC), identity check (STRICT), restore at the header's P_s from the header's cursor,
     * cross-check the restored identity (optionally through {@code restoredIdentity}), then the tail of the log.
     */
    static Outcome host(final Aeron aeron, final AeronArchive archive, final ShadowVerdicts verdicts,
                        final ShadowEntry entry, final Host h,
                        final java.util.function.Function<AssetsLogApplier, ShadowIdentitySource> restoredIdentity) {
        final UnsafeBuffer into = new UnsafeBuffer(ByteBuffer.allocateDirect(CAPACITY));
        final DirectBuffer payload;
        final int length;
        final ShadowHeader header;
        if (h.loaderCrc()) {
            final ShadowImageLoader.LoadResult r = new ShadowImageLoader(aeron, archive, verdicts,
                    SystemNanoClock.INSTANCE, AeShadowRig.TIMEOUT_NS, new ShadowCounters()).load(entry, into);
            if (!r.ok()) {
                return new Outcome("load " + r.kind() + " " + r.reason(), null, null);
            }
            payload = r.loaded().payload();
            length = r.loaded().length();
            header = r.loaded().header();
        } else {
            // NC-11: the payload taken from the recording without the CRC comparison of 8.2 step 3
            length = rawPayload(aeron, archive, entry, into);
            payload = new UnsafeBuffer(into, 0, length);
            header = entry.header();
        }
        if (h.check()) {
            final ShadowIdentity.Outcome o = ShadowIdentity.check(header, AeShadowRig.member(AeShadowRig.GENESIS,
                    log.terms()));
            if (!ShadowIdentity.accepted(o, ShadowIdentity.Policy.STRICT)) {
                return new Outcome("check " + o, null, null);
            }
        }
        final Arm restored = new Arm();
        try {
            restored.restore(payload, length, header.logPosition(), new LogCursor(header.logPosition(),
                    header.clusterTime(), header.leadershipTermId(), header.termBaseLogPosition()));
        } catch (final RuntimeException e) {
            return new Outcome("restore " + e.getMessage(), null, null);
        }
        if (h.crossCheck() && !ShadowIdentity.matches(header, restoredIdentity.apply(restored.applier))) {
            return new Outcome("cross-check " + ShadowIdentity.mismatch(header,
                    restoredIdentity.apply(restored.applier)), null, null);
        }
        final int mark = restored.events();
        restored.apply(log, header.logPosition(), Long.MAX_VALUE);
        return new Outcome(null, restored.image(), List.copyOf(restored.sink.events().subList(mark,
                restored.events())));
    }

    /** Header, chunks and trailer read straight off the recording; the chunks concatenated into {@code into}. */
    static int rawPayload(final Aeron aeron, final AeronArchive archive, final ShadowEntry e,
                          final MutableDirectBuffer into) {
        final int[] at = {0};
        final int[] index = {0};
        new ShadowReplay(aeron, archive, SystemNanoClock.INSTANCE).readAll(e.recordingId(), e.startPosition(),
                e.stopPosition(), (b, o, l, x) -> {
                    final int i = index[0]++;
                    if (i > 0 && i <= e.header().chunkCount()) {
                        into.putBytes(at[0], b, o, l);
                        at[0] += l;
                    }
                }, SystemNanoClock.INSTANCE.nanoTime() + AeShadowRig.TIMEOUT_NS);
        return at[0];
    }

    static void assertSameEndState(final Outcome o, final String failure) {
        if (!o.accepted()) {
            throw new AssertionError(failure + ": refused at " + o.refusedAt());
        }
        if (!Arrays.equals(baselineFinal, o.finalBytes())) {
            throw new AssertionError("final bytes differ from baseline (" + failure + ")");
        }
        assertEquals("the event suffix after P_s equals the baseline's", baselineSuffix, o.suffix());
    }

    static void assertRefused(final Outcome o, final String failure) {
        if (o.accepted()) {
            throw new AssertionError(failure);
        }
    }

    // ------------------------------------------------------------------------------------------------- tests

    @Test
    public void theRealAeShadowRoundTripsThroughTheArchive() {
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final ShadowCatalog catalog = AeShadowRig.catalog(fixture.aeron(), fixture.archive(), verdicts,
                AeShadowRig.GENESIS);
        final ShadowCatalog.Scan s = catalog.scan();
        final ShadowEntry e = s.entry(written.recordingId());
        assertEquals(ShadowEntry.State.COMPLETE, e.state());
        assertEquals(ShadowKind.ALIGNED, e.header().kindEnum());
        assertEquals(log.actionPosition(), e.header().logPosition());
        final ShadowCatalog.Scan v = catalog.verifyAllComplete();
        final ShadowEntry ve = v.entry(written.recordingId());
        assertEquals(ShadowEntry.State.VERIFIED, ve.state());
        shadowHeader = ve.header();
        assertEquals(ShadowIdentity.Outcome.OK, ShadowIdentity.check(ve.header(),
                AeShadowRig.member(AeShadowRig.GENESIS, log.terms())));
        // the header's identity, decoded independently from section 8 and the scalars of the restored payload
        final AssetsLogApplier scratch = new AssetsLogApplier(() -> { });
        scratch.restore(new UnsafeBuffer(shadowPayload), shadowPayload.length);
        assertEquals("header genesisId == section 8 genesisId", ve.header().genesisId(),
                scratch.engine().coreConfig().recorded().genesisId);
        assertEquals(ve.header().journalEnabledFlag(), scratch.engine().isMoneyJournalEnabled());
        assertEquals(ve.header().journalSeq(), scratch.engine().getJournalSeq());
        assertTrue(ShadowIdentity.matches(ve.header(), AeShadowRig.identityOf(scratch)));
        assertEquals("the v5 layout (RC2 S10' section 12)", 5, ve.header().payloadLayoutHint());
        final Outcome o = host(fixture.aeron(), fixture.archive(), verdicts, ve, Host.PRODUCTION,
                AeShadowRig::identityOf);
        assertSameEndState(o, "the genuine shadow");
        System.out.println("GREEN AE shadow round trip: P_s=" + log.actionPosition() + " payload="
                + shadowPayload.length + " B, chunks=" + ve.header().chunkCount() + ", recording ["
                + ve.startPosition() + ", " + ve.stopPosition() + "), log stop " + log.stop() + ", suffix events "
                + o.suffix().size() + ", final image " + baselineFinal.length + " B");
    }

    @Test
    public void theSameShadowAcrossARealReplication() throws Exception {
        final File dir = Files.createTempDirectory("ae-shadow-staging-").toFile();
        try (StagingArchive staging = StagingArchive.launch(dir)) {
            final long copy = staging.replicate(fixture.controlChannel(), written.recordingId(), Aeron.NULL_VALUE,
                    AeronArchive.NULL_POSITION);
            final Aeron aeron = staging.archive().context().aeron();
            final ShadowVerdicts verdicts = new ShadowVerdicts();
            final ShadowCatalog catalog = AeShadowRig.catalog(aeron, staging.archive(), verdicts, AeShadowRig.GENESIS);
            final ShadowEntry e = catalog.scan().entry(copy);
            assertNotNull(e);
            assertEquals(ShadowCatalog.DeepResult.Kind.VERIFIED, catalog.verifyDeep(e).kind());
            final Outcome o = host(aeron, staging.archive(), verdicts, catalog.scan().entry(copy), Host.PRODUCTION,
                    AeShadowRig::identityOf);
            assertSameEndState(o, "the replicated copy");
            System.out.println("GREEN AE shadow across replication: copy " + copy + " " + e);
        } finally {
            IoUtil.delete(dir, true);
        }
    }

    /** A raw copy of the shadow on the same archive with one header field (or the payload) changed. */
    static ShadowEntry rawCopy(final java.util.function.UnaryOperator<ShadowHeader> headerChange, final byte[] payload) {
        return rawCopy(headerChange, payload, ShadowEntry.State.COMPLETE);
    }

    static ShadowEntry rawCopy(final java.util.function.UnaryOperator<ShadowHeader> headerChange, final byte[] payload,
                               final ShadowEntry.State expected) {
        final ShadowCatalog.Scan s0 = AeShadowRig.catalog(fixture.aeron(), fixture.archive(), new ShadowVerdicts(),
                0).scan();
        final ShadowHeader genuine = s0.entry(written.recordingId()).header();
        final ShadowHeader h = headerChange.apply(genuine);
        final RecordedLogFixture.RawRecording rec = fixture.record("aeron:ipc?term-length=" + AeShadowRig.SHADOW_TERM
                + "|" + ShadowAlias.PARAM + h.alias().format(), ShadowAlias.STREAM_ID);
        final UnsafeBuffer hb = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
        final int headerCrc = h.encode(hb, 0);
        rec.offer(hb, 0, ShadowHeader.LENGTH);
        final UnsafeBuffer pb = new UnsafeBuffer(payload);
        for (int off = 0; off < payload.length; off += h.chunkLength()) {
            rec.offer(pb, off, Math.min(h.chunkLength(), payload.length - off));
        }
        final UnsafeBuffer tb = new UnsafeBuffer(new byte[ShadowTrailer.LENGTH]);
        ShadowTrailer.of(h, headerCrc).encode(tb, 0);
        rec.offer(tb, 0, ShadowTrailer.LENGTH);
        rec.stopRecording();
        final ShadowEntry e = AeShadowRig.catalog(fixture.aeron(), fixture.archive(), new ShadowVerdicts(),
                h.genesisId()).scan().entry(rec.recordingId());
        assertEquals(e.toString(), expected, e.state());
        return e;
    }

    static ShadowHeader with(final ShadowHeader g, final long genesisId, final long logPosition, final long seq,
                             final long payloadDigest) {
        return new ShadowHeader(g.engineId(), g.clusterId(), g.kind(), g.producerKind(), g.producerId(), seq, genesisId,
                logPosition, g.leadershipTermId(), g.termBaseLogPosition(), g.clusterTime(), g.journalEnabled(),
                g.journalSeq(), g.payloadLength(), payloadDigest, g.chunkLength(), g.payloadLayoutHint());
    }

    /** The genuine header as written by another producer (FOLLOWER 1) over {@code payload}. */
    static ShadowHeader fromFollower(final ShadowHeader g, final byte[] payload) {
        return new ShadowHeader(g.engineId(), g.clusterId(), g.kind(), 2, 1, 1, g.genesisId(), g.logPosition(),
                g.leadershipTermId(), g.termBaseLogPosition(), g.clusterTime(), g.journalEnabled(), g.journalSeq(),
                payload.length, sha256First8(payload), g.chunkLength(), g.payloadLayoutHint());
    }

    /** Amendment Q0's digest, computed independently of the kit: the first 8 bytes of SHA-256, big-endian. */
    static long sha256First8(final byte[] bytes) {
        try {
            final byte[] d = java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (d[i] & 0xFF);
            }
            return v;
        } catch (final java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void negativeControlNc7AShadowOfAnotherGenesisIsRefusedByTheIdentityCheck() {
        // a genuine image of ANOTHER cluster (genesis g2 in section 8 and in the header): only the identity check
        // can refuse it here; the post-restore cross-check passes (header and engine agree), so NC-7 switches off
        // exactly one rule (review FIX-3)
        final byte[] g2 = imageOfGenesis(AeShadowRig.GENESIS + 1);
        final ShadowEntry foreign = rawCopy(g -> new ShadowHeader(g.engineId(), g.clusterId(), g.kind(),
                g.producerKind(), g.producerId(), 100, AeShadowRig.GENESIS + 1, g.logPosition(), g.leadershipTermId(),
                g.termBaseLogPosition(), g.clusterTime(), g.journalEnabled(), g.journalSeq(), g2.length,
                sha256First8(g2), g.chunkLength(), g.payloadLayoutHint()), g2);
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final Outcome refused = host(fixture.aeron(), fixture.archive(), verdicts, foreign, Host.PRODUCTION,
                AeShadowRig::identityOf);
        assertNoRestoreOfAnotherGenesis(refused);
        AeShadowRig.nc("NC-7 identity check skipped", "shadow of genesis g2 accepted for g1",
                () -> assertNoRestoreOfAnotherGenesis(host(fixture.aeron(), fixture.archive(), verdicts, foreign,
                        new Host(false, true, true), AeShadowRig::identityOf)));
    }

    /** The shadow image with the recorded CoreConfig's genesisId replaced (an engine of another cluster). */
    static byte[] imageOfGenesis(final long genesisId) {
        final AssetsLogApplier other = new AssetsLogApplier(() -> { });
        other.restore(new UnsafeBuffer(shadowPayload), shadowPayload.length);
        final com.openexchange.assets.domain.commands.CoreConfigCommand r = other.engine().coreConfig().recorded();
        final com.openexchange.assets.domain.commands.CoreConfigCommand c =
                new com.openexchange.assets.domain.commands.CoreConfigCommand();
        c.configVersion = r.configVersion;
        c.behaviorVersion = r.behaviorVersion;
        c.genesisId = genesisId;
        c.markets = r.markets.clone();
        c.assets = r.assets.clone();
        other.engine().coreConfig().restore(c);
        final org.agrona.ExpandableArrayBuffer b = new org.agrona.ExpandableArrayBuffer(shadowPayload.length + 64);
        final int n = other.serialize(b);
        assertEquals("same length", shadowPayload.length, n);
        return Arrays.copyOf(b.byteArray(), n);
    }

    /** GREEN: refused at the identity check; NC-7 (check off): accepted (named); any other refusal fails loudly. */
    static void assertNoRestoreOfAnotherGenesis(final Outcome o) {
        if (o.refusedAt() == null) {
            throw new AssertionError("shadow of genesis g2 accepted for g1");
        }
        if (!o.refusedAt().startsWith("check GENESIS_MISMATCH")) {
            throw new AssertionError("refused for another reason than the identity check: " + o.refusedAt());
        }
    }

    /** A self-consistent but DIFFERENT engine image of the same length: one deposit more, envelope resealed. */
    static byte[] extraDeposit() {
        final AssetsLogApplier other = new AssetsLogApplier(() -> { });
        other.restore(new UnsafeBuffer(shadowPayload), shadowPayload.length);
        final com.openexchange.assets.domain.commands.DepositCommand d =
                new com.openexchange.assets.domain.commands.DepositCommand();
        d.reset();
        d.setUserId(1);
        d.setAssetId(0);
        d.setAmount(7);
        other.engine().applyCommand(com.openexchange.assets.application.engine.AssetsEngine.CMD_DEPOSIT, d, 1L);
        final org.agrona.ExpandableArrayBuffer ob = new org.agrona.ExpandableArrayBuffer(shadowPayload.length + 64);
        final int n = other.serialize(ob);
        assertEquals("same length, different bytes", shadowPayload.length, n);
        final byte[] altered = Arrays.copyOf(ob.byteArray(), n);
        assertTrue(!Arrays.equals(altered, shadowPayload));
        return altered;
    }

    /**
     * NC-11 (11.10) as written, buildable since amendment Q0. The forged extra-deposit image under the genuine header:
     * its CRC32C equals the genuine one (the residue property: why Q0 changed the digest), its SHA-256 digest does
     * not, so the loader refuses it. NC-11: with the digest comparison switched off (the payload taken off the
     * recording unchecked) it restores a different end state.
     */
    @Test
    public void negativeControlNc11APayloadChangedAfterTheHeaderWasSealedIsCorrupt() {
        final byte[] altered = extraDeposit();
        final java.util.zip.CRC32C c1 = new java.util.zip.CRC32C();
        c1.update(altered);
        final java.util.zip.CRC32C c2 = new java.util.zip.CRC32C();
        c2.update(shadowPayload);
        assertEquals("(char) the residue property: equal CRC32C", c2.getValue(), c1.getValue());
        assertTrue("the SHA-256 digests differ", sha256First8(altered) != sha256First8(shadowPayload));
        final ShadowEntry swapped = rawCopy(g -> with(g, g.genesisId(), g.logPosition(), 101, g.payloadDigest()),
                altered);
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final Outcome o = host(fixture.aeron(), fixture.archive(), verdicts, swapped, Host.PRODUCTION,
                AeShadowRig::identityOf);
        assertTrue("refused by the loader's digest: " + o.refusedAt(), o.refusedAt() != null
                && o.refusedAt().startsWith("load CORRUPT payload digest"));
        AeShadowRig.nc("NC-11 loader skips the payload digest", "final bytes differ from baseline",
                () -> assertSameEndState(host(fixture.aeron(), fixture.archive(), new ShadowVerdicts(), swapped,
                        new Host(true, false, true), AeShadowRig::identityOf), "NC-11"));
        // a flipped byte: refused by the loader too; with the loader check skipped only the engine envelope refuses it
        final byte[] flipped = shadowPayload.clone();
        flipped[flipped.length / 2] ^= 0x10;
        final ShadowEntry torn = rawCopy(g -> with(g, g.genesisId(), g.logPosition(), 102, g.payloadDigest()),
                flipped);
        assertTrue(host(fixture.aeron(), fixture.archive(), new ShadowVerdicts(), torn, Host.PRODUCTION,
                AeShadowRig::identityOf).refusedAt().startsWith("load CORRUPT"));
        final Outcome skipped = host(fixture.aeron(), fixture.archive(), new ShadowVerdicts(), torn,
                new Host(true, false, true), AeShadowRig::identityOf);
        assertTrue("with the loader check skipped only the engine envelope stops a torn image: " + skipped.refusedAt(),
                skipped.refusedAt() != null && skipped.refusedAt().startsWith("restore"));
    }

    /** Amendment Q0 on the real AE: a second producer that sealed a different state at P_s is a CONFLICT. */
    @Test
    public void aSecondProducerWithADifferentStateAtTheSamePositionIsAConflict() {
        final byte[] altered = extraDeposit();
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final ShadowEntry follower = rawCopy(g -> fromFollower(g, altered), altered, ShadowEntry.State.CONFLICT);
        try {
            final ShadowCatalog.Scan s = AeShadowRig.catalog(fixture.aeron(), fixture.archive(), verdicts,
                    AeShadowRig.GENESIS).scan();
            final ShadowCatalog.Answer a = s.equalAt(log.actionPosition());
            assertEquals("genuine (R3) and follower (F1) disagree at P_s: " + s.atPosition(log.actionPosition()),
                    ShadowCatalog.Answer.Kind.CONFLICT, a.kind());
            assertTrue(a.shadow().producers().keySet().containsAll(java.util.Set.of("R3", "F1")));
            System.out.println("GREEN Q0 AE: genuine and extra-deposit images at P_s=" + log.actionPosition()
                    + " -> CONFLICT " + a.shadow().copies().size() + " copies; follower " + follower.recordingId());
        } finally {
            fixture.archive().purgeRecording(follower.recordingId()); // the other tests of this class share the archive
        }
    }

    @Test
    public void negativeControlNc17ThePostRestoreCrossCheckIsMandatory() {
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final ShadowEntry genuine = AeShadowRig.catalog(fixture.aeron(), fixture.archive(), verdicts,
                AeShadowRig.GENESIS).scan().entry(written.recordingId());
        // an identity source that reports another genesis after the restore (a host that read it at the wrong instant)
        final java.util.function.Function<AssetsLogApplier, ShadowIdentitySource> lying = a -> {
            final ShadowIdentitySource real = AeShadowRig.identityOf(a);
            return new ShadowIdentitySource() {
                public long genesisId() {
                    return AeShadowRig.GENESIS + 1;
                }

                public boolean journalEnabled() {
                    return real.journalEnabled();
                }

                public long journalSeq() {
                    return real.journalSeq();
                }

                public int payloadLayoutHint() {
                    return real.payloadLayoutHint();
                }
            };
        };
        final Outcome o = host(fixture.aeron(), fixture.archive(), verdicts, genuine, Host.PRODUCTION, lying);
        assertCrossCheckRefused(o);
        AeShadowRig.nc("NC-17 post-restore cross-check skipped",
                "restored engine identity differs from header, accepted",
                () -> assertCrossCheckRefused(host(fixture.aeron(), fixture.archive(), verdicts, genuine,
                        new Host(true, true, false), lying)));
    }

    /** GREEN: refused at the cross-check; NC-17 (cross-check off): accepted (named); any other refusal fails loudly. */
    static void assertCrossCheckRefused(final Outcome o) {
        if (o.refusedAt() == null) {
            throw new AssertionError("restored engine identity differs from header, accepted");
        }
        if (!o.refusedAt().startsWith("cross-check genesisId")) {
            throw new AssertionError("refused for another reason than the cross-check: " + o.refusedAt());
        }
    }

    /**
     * The P_s binding control of 11.10. Measured: the AE end state is byte-identical when P_s is moved back over up
     * to 64 frames (re-applied frames are idempotent in the engine), so the binding does not show in the image. It
     * shows in the events the tail emits: a host that restores at a wrong P_s re-emits the effects of the re-applied
     * frames. The control asserts that the event suffix differs, and prints the image result.
     */
    @Test
    public void theHeaderPositionIsBoundToTheImage() {
        final long[] p = log.positions();
        int i = 0;
        while (p[i] != log.actionPosition()) {
            i++;
        }
        final List<String> tried = new java.util.ArrayList<>();
        for (int k = 2; k <= 64 && i - k >= 1; k *= 2) {
            final long shifted = p[i - k];
            final ShadowEntry moved = rawCopy(g -> with(g, g.genesisId(), shifted, 200 + 0L + tried.size(),
                    g.payloadDigest()), shadowPayload);
            final Outcome o = host(fixture.aeron(), fixture.archive(), new ShadowVerdicts(), moved, Host.PRODUCTION,
                    AeShadowRig::identityOf);
            assertTrue("accepted by every check (the format cannot know the position is wrong): " + o.refusedAt(),
                    o.accepted());
            final boolean bytes = !Arrays.equals(baselineFinal, o.finalBytes());
            final boolean events = !baselineSuffix.equals(o.suffix());
            tried.add(k + ":image " + (bytes ? "differs" : "same") + ",events " + (events ? "differ" : "same"));
            if (bytes || events) {
                System.out.println("NC P_S_SHIFTED -> P_s " + log.actionPosition() + " -> " + shifted + " (" + k
                        + " frames back): " + tried);
                return;
            }
        }
        throw new AssertionError("restore + tail from a shifted P_s produce the same image and events: " + tried);
    }
}
