// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.cluster.NodeReadiness;
import com.openexchange.cluster.log.ClusterLogReader;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * (on the core, A1 to A4; mirrors the ME's MeSessionAuthorityTest): a principal-less session in the
 * log (history from before session principals: the v0.5.0-beta core had no authenticator), a frame from a session id the registry does not hold,
 * or a restored principal-less session halts the node at the log position every member shares. The applier only
 * throws the kit exception (it never fail-fasts, logs or touches readiness); the live service prints the CRITICAL
 * line, fences its callbacks and fail-fasts.
 */
public class CoreSessionAuthorityTest {

    private static final String CRITICAL = "CRITICAL: principal-less session in log: history from before session principals, clean genesis required";
    private static final byte[] FOREIGN = {9, 4, 1, 0, 0, 0, 0, 0, 0, 0};

    private static UnsafeBuffer deposit() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[DepositEncoder.BLOCK_LENGTH + 8]);
        new DepositEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .correlationId(1).userId(7).assetId(0).amount(1_000).requestIdHigh(0).requestIdLow(0);
        return b;
    }

    private static Header header() {
        final Header h = new Header(0, 16);
        h.buffer(new UnsafeBuffer(new byte[64]));
        return h;
    }

    /** A service with a stub cluster (log position 42) and a recording fail-fast. */
    private static final class Rig {
        final CoreClusteredService service = new CoreClusteredService();
        final int[] stops = {0};
        final NodeReadiness readiness;

        Rig() {
            ApplierParityRig.inject(service, "cluster",
                    new ApplierParityRig.StubCluster(Cluster.Role.LEADER, new HashMap<>()));
            service.failFast = () -> stops[0]++;
            readiness = (NodeReadiness) ApplierParityRig.read(service, CoreClusteredService.class, "readiness");
            readiness.started();
            readiness.observe(1, 1, 1, Cluster.Role.LEADER, 100, 100, true, true, 0);
            assertTrue("control: a verified healthy node starts ready", readiness.ready());
        }
    }

    private static String captureErr(final Runnable r) {
        final PrintStream prior = System.err;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setErr(new PrintStream(out, true));
        try {
            r.run();
        } finally {
            System.setErr(prior);
        }
        return out.toString();
    }

    private static void assertHalted(final Rig rig, final Throwable thrown, final String log, final String where) {
        assertNotNull("the node halts: nothing was thrown", thrown);
        assertTrue("halted with the R0 exception, got " + thrown, thrown instanceof IllegalStateException
                && thrown.getMessage() != null && thrown.getMessage().startsWith(CRITICAL + " (" + where + ","));
        assertEquals("failFast ran once", 1, rig.stops[0]);
        assertTrue(log, log.contains(CRITICAL + " (" + where + ", clusterSessionId="));
        assertFalse("readiness closed in the same callback", rig.readiness.ready());
        assertTrue(rig.readiness.describe(), rig.readiness.describe().contains("application-callback-failed"));
    }

    private static void assertSessionOpenHalts(final byte[] principal) {
        final Rig rig = new Rig();
        final Throwable[] thrown = {null};
        final String log = captureErr(() -> {
            try {
                rig.service.onSessionOpen(new ApplierParityRig.StubSession(5, principal), 10);
            } catch (Throwable t) {
                thrown[0] = t;
            }
        });
        assertNull("no registry row for a principal-less open", rig.service.engine().sessions().get(5));
        assertHalted(rig, thrown[0], log, "session open");
        assertTrue(log, log.contains("clusterSessionId=5, logPosition=42)"));
        // fenced: a valid open is not applied either
        final Throwable after = captureThrow(() -> rig.service.onSessionOpen(
                new ApplierParityRig.StubSession(6, SessionedLog.principalOf(SessionClass.ADMIN)), 11));
        assertNotNull("the node is fenced", after);
        assertNull(rig.service.engine().sessions().get(6));
    }

    private static Throwable captureThrow(final Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    // ------------------------------------------------------------------------------------------------- A1

    @Test
    public void principalLessSessionOpenHaltsTheNodeEmptyPrincipal() {
        assertSessionOpenHalts(new byte[0]);
    }

    @Test
    public void principalLessSessionOpenHaltsTheNodeForeignPrincipal() {
        assertSessionOpenHalts(FOREIGN);
    }

    @Test
    public void aMintedPrincipalOpensAsToday() {
        final Rig rig = new Rig();
        rig.service.onSessionOpen(new ApplierParityRig.StubSession(5, SessionedLog.principalOf(SessionClass.GATEWAY)), 10);
        assertNotNull(rig.service.engine().sessions().get(5));
        assertEquals(SessionClass.GATEWAY, rig.service.engine().sessions().get(5).sessionClass());
        assertEquals(0, rig.stops[0]);
        assertTrue(rig.readiness.ready());
    }

    // ------------------------------------------------------------------------------------------------- A1b

    @Test
    public void theApplierOnlyThrowsTheKitException() {
        final int[] configFailFast = {0};
        final CoreLogApplier applier = new CoreLogApplier(() -> configFailFast[0]++);
        applier.genesis();
        final String log = captureErr(() -> {
            final Throwable t = captureThrow(() -> applier.onSessionOpen(77, 6304, 1, new byte[0]));
            assertNotNull(t);
            assertEquals("the kit exception", "com.openexchange.cluster.auth.PrincipalLessSessionException",
                    t.getClass().getName());
            assertEquals(CRITICAL + " (session open, clusterSessionId=77, logPosition=6304)", t.getMessage());
        });
        assertTrue("the applier latches", applier.failed());
        assertEquals("the applier never fail-fasts", 0, configFailFast[0]);
        assertEquals("the applier never logs", "", log);
        assertNull("no registry row", applier.engine().sessions().get(77));
    }

    // ------------------------------------------------------------------------------------------------- A2

    @Test
    public void aFrameFromAnUnknownSessionHaltsTheNode() {
        final Rig rig = new Rig();
        final UnsafeBuffer frame = deposit();
        final int accounts = rig.service.engine().accountCount();
        final Throwable[] thrown = {null};
        final String log = captureErr(() -> {
            try {
                rig.service.onSessionMessage(new ApplierParityRig.StubSession(9,
                        SessionedLog.principalOf(SessionClass.TOOL)), 10, frame, 0, frame.capacity(), header());
            } catch (Throwable t) {
                thrown[0] = t;
            }
        });
        assertHalted(rig, thrown[0], log, "ingress frame");
        assertTrue(log, log.contains("(ingress frame, clusterSessionId=9, logPosition="));
        assertEquals("nothing applied", accounts, rig.service.engine().accountCount());
    }

    // ------------------------------------------------------------------------------------------------- A3

    @Test
    public void aRestoredPrincipalLessRowHaltsTheNode() {
        final CoreLogApplier source = new CoreLogApplier(() -> { });
        source.genesis();
        source.engine().sessions().open(5, new byte[0]); // a pre-fix registry row with no authority
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = source.serialize(image);

        final Rig rig = new Rig();
        final Throwable[] thrown = {null};
        final String log = captureErr(() -> {
            try {
                rig.service.restoreFromImage(image, length);
            } catch (Throwable t) {
                thrown[0] = t;
            }
        });
        assertHalted(rig, thrown[0], log, "restored session");
        assertTrue(log, log.contains(CRITICAL + " (restored session, clusterSessionId=5, logPosition=-1)"));
    }

    @Test
    public void aContainerRestoredPrincipalLessSessionHaltsTheNode() {
        final Rig rig = new Rig();
        final List<ClientSession> restored = List.of(
                new ApplierParityRig.StubSession(3, SessionedLog.principalOf(SessionClass.ADMIN)),
                new ApplierParityRig.StubSession(4, new byte[0]));
        final Cluster cluster = (Cluster) Proxy.newProxyInstance(Cluster.class.getClassLoader(),
                new Class<?>[] {Cluster.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "forEachClientSession" -> {
                            @SuppressWarnings("unchecked")
                            final Consumer<ClientSession> action = (Consumer<ClientSession>) args[0];
                            restored.forEach(action);
                            return null;
                        }
                        case "logPosition" -> {
                            return 819_328L;
                        }
                        case "role" -> {
                            return Cluster.Role.FOLLOWER;
                        }
                        case "idleStrategy" -> {
                            return org.agrona.concurrent.NoOpIdleStrategy.INSTANCE;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    }
                });
        final Throwable[] thrown = {null};
        final String log = captureErr(() -> {
            try {
                // review V6: through onStart (no snapshot image), so the wiring in applyOnStart is what is tested;
                // anything onStart reaches after the check (endpoint, durability) is not stubbed and would throw
                rig.service.onStart(cluster, null);
            } catch (Throwable t) {
                thrown[0] = t;
            }
        });
        assertHalted(rig, thrown[0], log, "restored session");
        assertTrue(log, log.contains("clusterSessionId=4, logPosition=819328)"));
    }

    // ------------------------------------------------------------------------------------------------- A4

    /**
     * A4: the producer is an in-process cluster-log recording (the kit's ClusterLogWriter on a RecordedLogFixture)
     * whose third entry is a SessionOpen with empty principal bytes, the shape of the v0.5.0-beta core log. Two
     * independent members replay it through the kit reader and replay container: both refuse at the same entry, and
     * both CRITICAL texts name one and the same logPosition, the writer's own frame end for that open.
     */
    @Test
    public void twoMembersReplayingAPrincipalLessOpenHaltAtTheSameLogPosition() throws Exception {
        try (RecordedLogFixture fixture = new RecordedLogFixture()) {
            final RecordedLogFixture.Recording rec = fixture.record(64 * 1024, 1408, 0);
            final ClusterLogWriter w = rec.writer();
            final long t0 = 1_700_000_000_000L;
            w.newLeadershipTerm(0, t0, 0, 0, rec.publication().sessionId(), TimeUnit.MILLISECONDS,
                    CoreReplayParityTest.APP_VERSION);
            final byte[] tool = SessionedLog.principalOf(SessionClass.TOOL);
            w.sessionOpen(901, 7_000_001L, t0 + 1, 102, "aeron:udp?endpoint=localhost:20901", tool);
            final UnsafeBuffer frame = deposit();
            final byte[] bytes = new byte[frame.capacity()];
            frame.getBytes(0, bytes);
            w.message(901, t0 + 2, bytes);
            final long principalLessOpen = w.sessionOpen(902, 7_000_002L, t0 + 3, 102,
                    "aeron:udp?endpoint=localhost:20902", new byte[0]);
            w.message(902, t0 + 4, bytes);
            final long stop = rec.stopRecording();

            final String[] refusals = new String[2];
            for (int m = 0; m < 2; m++) {
                final CoreLogApplier applier = new CoreLogApplier(() -> { });
                final ReplayContainer container = new ReplayContainer(CoreReplayParityTest.shadowEngine(applier),
                        CoreReplayParityTest.APP_VERSION, AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR,
                        ShadowTriggerListener.NONE);
                container.genesis();
                final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
                final ClusterLogReader reader = new ClusterLogReader(container, 16, 0);
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                try {
                    while (reader.consumedPosition() < stop && System.nanoTime() < deadline) {
                        if (reader.poll(image, stop) == 0) {
                            Thread.yield();
                        }
                    }
                } catch (final IllegalStateException refused) {
                    // the reader stops LISTENER_FAILED with the container's ENGINE_FAILED behind it; the applier's
                    // kit exception is the root
                    for (Throwable t = refused; t != null; t = t.getCause()) {
                        if (t instanceof com.openexchange.cluster.auth.PrincipalLessSessionException) {
                            refusals[m] = t.getMessage();
                        }
                    }
                }
                assertNotNull("member " + m + " refused the replay with the kit exception", refusals[m]);
                assertTrue("member " + m + ": " + refusals[m], refusals[m].startsWith(CRITICAL));
                assertNull("member " + m + ": no row for the principal-less session",
                        applier.engine().sessions().get(902));
                assertEquals("member " + m + ": the minted session's deposit applied before the refusal", 1_000,
                        applier.engine().account(7).available(0));
            }
            assertEquals("both members refuse at the same entry", refusals[0], refusals[1]);
            assertTrue(refusals[0], refusals[0].contains("(session open, clusterSessionId=902, logPosition="
                    + principalLessOpen + ")"));
        }
    }

}
