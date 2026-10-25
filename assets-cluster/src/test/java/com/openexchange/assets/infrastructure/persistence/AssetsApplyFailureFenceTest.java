// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.domain.AssetsEventSink;
import com.openexchange.assets.domain.RejectReason;
import com.openexchange.assets.domain.SettleRejectReason;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.cluster.NodeReadiness;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S1 (K5 fence at the service boundary, RED-F11/F13): an apply failure inside
 * {@code onSessionMessage} closes readiness IMMEDIATELY, invokes the hookless process stop once, and
 * fences every later callback; the forked-JVM case proves the production seam is {@code Runtime.halt}
 * (exit 1, shutdown hooks NOT run). Port of the ME's {@code ApplyFailureFenceTest} service half.
 */
public class AssetsApplyFailureFenceTest {

    private static UnsafeBuffer deposit() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[DepositEncoder.BLOCK_LENGTH + 8]);
        new DepositEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .correlationId(1).userId(7).assetId(0).amount(1_000).requestIdHigh(0).requestIdLow(0);
        return b;
    }

    private static AssetsEventSink failingSink(final Throwable failure) {
        return new AssetsEventSink() {
            private void fail() {
                if (failure instanceof RuntimeException e) {
                    throw e;
                }
                throw (Error) failure;
            }
            public void onDepositAck(long c, long u, int a, long amt, long n) { }
            public void onWithdrawAck(long c, long u, int a, long amt, long n) { }
            public void onHoldAck(long c, long o, long u, int a, long amt) { }
            public void onHoldReject(long c, long o, long u, int a, long amt, RejectReason r) { }
            public void onBalanceUpdate(long u, int a, long av, long l) { fail(); }
            public void onSettlementApplied(long t, long b, long s) { }
            public void onWithdrawReject(long c, long u, int a, long amt, RejectReason r) { }
            public void onBalanceSnapshotEnd(long c, int n) { }
            public void onHoldSnapshotEntry(long o, long u, int a, long rem) { }
            public void onHoldSnapshotEnd(long c, int n) { }
            public void onFeedPositionReport(long c, long p, long t, boolean j) { }
            public void onSettleFault(long t, long o, long u, int a, long d, long un) { }
            public void onSettleRejected(long t, int m, long p, SettleRejectReason r, long d) { }
            public void onOrderReject(long c, long rh, long rl, long u, long o, int m, int ref, com.openexchange.assets.domain.CoreRejectReason r, int me, long d, long t) { }
            public void onAdminAck(long c, int ref, com.openexchange.assets.domain.AdminResult r, int reason, long cfg) { }
            public void onAdmissionState(int m, com.openexchange.assets.domain.GateState gs, com.openexchange.assets.domain.GateReason gr, boolean b, long at, long lp, boolean o) { }
        };
    }

    private static Object field(final AssetsClusteredService service, final String name) throws Exception {
        final Field f = AssetsClusteredService.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(service);
    }

    private static AssetsClusteredService failingService(final Throwable failure) {
        final AssetsClusteredService service = new AssetsClusteredService();
        service.engine().setEventSink(failingSink(failure));
        // The in-process dispatch has no ClientSession (id -1): give that id a TOOL authority so the
        // command reaches the engine and the fence, not the session-authority reject (RC1 S3).
        service.engine().sessions().open(-1L, com.openexchange.assets.auth.CorePrincipal.anyUser(
                com.openexchange.assets.infrastructure.generated.SessionClass.TOOL, 0).encode());
        return service;
    }

    private static void dispatch(final AssetsClusteredService service) {
        final UnsafeBuffer b = deposit();
        final Header header = new Header(0, 16);
        header.buffer(new UnsafeBuffer(new byte[64]));
        service.onSessionMessage(null, 10, b, 0, b.capacity(), header);
    }

    @Test
    public void serviceFailureStopsAdmissionAndSnapshotsImmediately() throws Exception {
        final IllegalStateException failure = new IllegalStateException("injected publisher");
        final AssetsClusteredService service = failingService(failure);
        final int[] stops = {0};
        service.failFast = () -> stops[0]++;
        final NodeReadiness readiness = (NodeReadiness) field(service, "readiness");
        readiness.started();
        readiness.observe(1, 1, 1, Cluster.Role.LEADER, 100, 100, true, true, 0);
        assertTrue("control: a verified healthy node starts ready", readiness.ready());

        assertSame(failure, assertThrows(IllegalStateException.class, () -> dispatch(service)));
        assertEquals("must stop on the first apply fault", 1, stops[0]);
        assertTrue(readiness.describe(), readiness.describe().contains("application-callback-failed"));
        assertFalse("readiness closes in the SAME callback, not on the next background tick", readiness.ready());
        assertEquals("the credit happened before the failure", 1_000, service.engine().account(7).available(0));

        service.engine().setEventSink(null);
        final int accounts = service.engine().accountCount();
        assertThrows(IllegalStateException.class, () -> dispatch(service));
        assertEquals("a fenced service applies nothing more", accounts, service.engine().accountCount());
        assertEquals("one stop per fault, not one per blocked retry", 1, stops[0]);
        assertThrows("must refuse before touching a snapshot publication", IllegalStateException.class,
                () -> service.onTakeSnapshot(null));
        assertThrows(IllegalStateException.class, () -> service.onTimerEvent(1, 20));
        assertThrows(IllegalStateException.class, () -> service.onRoleChange(Cluster.Role.LEADER));
        assertEquals(0, service.doBackgroundWork(System.nanoTime()));
        assertFalse(readiness.ready());
        assertEquals(0, field(service, "applicationCallbackDepth"));
    }

    @Test
    public void errorAfterMutationIsAlsoAnApplyFailure() throws Exception {
        final AssertionError failure = new AssertionError("injected");
        final AssetsClusteredService service = failingService(failure);
        final int[] stops = {0};
        service.failFast = () -> stops[0]++;
        assertSame(failure, assertThrows(AssertionError.class, () -> dispatch(service)));
        assertEquals(1, stops[0]);
        assertThrows(IllegalStateException.class, () -> service.onTakeSnapshot(null));
    }

    /** RED-F13: the production seam is a hookless halt, exercised in a real forked JVM. */
    @Test
    public void actualApplyFaultUsesHooklessProcessExit() throws Exception {
        final Path dir = Files.createTempDirectory("assets-apply-failure-exit-");
        final Path marker = dir.resolve("hook");
        final Path log = dir.resolve("child.log");
        final Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                getClass().getName(), marker.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue("child must exit promptly", child.waitFor(20, TimeUnit.SECONDS));
            assertEquals(Files.readString(log), 1, child.exitValue());
            assertFalse("a money apply failure must not deadlock in shutdown hooks", Files.exists(marker));
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
            System.out.print(Files.readString(log));
            Files.deleteIfExists(marker);
            Files.deleteIfExists(log);
            Files.deleteIfExists(dir);
        }
    }

    public static void main(final String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.writeString(Path.of(args[0]), "hook ran");
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }));
        dispatch(failingService(new IllegalStateException("injected publisher")));
        // If the seam did not halt, exit 0 so the parent's exit-code assertion fails loudly.
        System.exit(0);
    }
}
