// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.domain.Asset;
import com.openexchange.assets.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.AdminAckDecoder;
import com.openexchange.assets.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.OrderRejectDecoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.generated.SetMoneyJournalEncoder;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.security.CredentialsSupplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC1 S3 (K6, hardening B3.7 cases 1-6) on a real single-member cluster: no credentials means no
 * session; a wrong class is rejected on the wire; a scoped key cannot act for another user; an
 * unregistered production-class session gets UNREGISTERED_SESSION; a non-resumable registration is
 * refused and the session closed; ADMIN registers and operates.
 */
public class AuthenticatedClusterTest {

    private static final int PORT_BASE = 20100; // disjoint from the other cluster tests
    private static final long TIMEOUT_MS = 20_000;

    /** Captures egress frames and session events for one client. */
    private static final class Capture implements EgressListener {
        final List<String> frames = new CopyOnWriteArrayList<>();
        final List<EventCode> events = new CopyOnWriteArrayList<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderRejectDecoder reject = new OrderRejectDecoder();
        private final AdminAckDecoder adminAck = new AdminAckDecoder();
        private final AdmissionStateDecoder admission = new AdmissionStateDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();

        public void onMessage(long sessionId, long timestamp, DirectBuffer buffer, int offset, int length,
                              io.aeron.logbuffer.Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case OrderRejectDecoder.TEMPLATE_ID:
                    reject.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("REJECT ref=" + reject.refTemplateId() + " reason=" + reject.reason()
                            + " corr=" + reject.correlationId() + " user=" + reject.userId());
                    break;
                case AdminAckDecoder.TEMPLATE_ID:
                    adminAck.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("ADMINACK ref=" + adminAck.refTemplateId() + " result=" + adminAck.result()
                            + " reason=" + adminAck.reason() + " corr=" + adminAck.correlationId());
                    break;
                case AdmissionStateDecoder.TEMPLATE_ID:
                    admission.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("ADMISSION market=" + admission.marketId() + " gate=" + admission.gateState()
                            + " reason=" + admission.gateReason());
                    break;
                case DepositAckDecoder.TEMPLATE_ID:
                    depositAck.wrapAndApplyHeader(buffer, offset, header);
                    frames.add("DEPOSITACK corr=" + depositAck.correlationId() + " user=" + depositAck.userId());
                    break;
                default:
                    break;
            }
        }

        public void onSessionEvent(long correlationId, long clusterSessionId, long leadershipTermId,
                                   int leaderMemberId, EventCode code, String detail) {
            events.add(code);
        }

        String await(final AeronCluster client, final String prefix) {
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (System.currentTimeMillis() < deadline) {
                for (final String f : frames) {
                    if (f.startsWith(prefix)) {
                        return f;
                    }
                }
                client.pollEgress();
                idle.idle();
            }
            fail("no egress frame starting with '" + prefix + "' within " + TIMEOUT_MS + " ms; saw " + frames);
            return null;
        }
    }

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();

    private int encodeDeposit(final long corr, final long userId) {
        final DepositEncoder e = new DepositEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).userId(userId).assetId(Asset.USD.id()).amount(FixedPoint.fromDouble(10.0))
                .requestIdHigh(0).requestIdLow(0);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private int encodeSetJournal(final long corr) {
        final SetMoneyJournalEncoder e = new SetMoneyJournalEncoder().wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(corr).enabled(BoolFlag.TRUE);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    private void offer(final AeronCluster client, final int length) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            if (client.offer(ingress, 0, length) > 0) {
                return;
            }
            client.pollEgress();
            idle.idle();
        }
        throw new IllegalStateException("offer not accepted within timeout");
    }

    private static AeronCluster connect(final String aeronDir, final List<String> hosts,
                                        final CredentialsSupplier credentials, final Capture capture) {
        return AeronCluster.connect(new AeronCluster.Context()
                .credentialsSupplier(credentials)
                .egressListener(capture)
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .aeronDirectoryName(aeronDir)
                .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
    }

    @Test
    public void sessionsAreAuthenticatedClassedScopedAndRegistered() {
        TestAuth.install();
        final File tmp = new File(System.getProperty("java.io.tmpdir"), "ae-auth-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final List<String> hosts = List.of("localhost");
        final AssetsClusteredService service = new AssetsClusteredService();
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.baseDir(new File(tmp, "node0"));
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=16m|mtu=8192")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster admin = null;
        AeronCluster bridge = null;
        AeronCluster treasury7 = null;
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true).mtuLength(8192),
                    cfg.archiveContext(), cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

            // 1. No credentials (Aeron's default NullCredentialsSupplier): no session.
            final Capture anon = new Capture();
            try (AeronCluster c = connect(aeronDir, hosts, new io.aeron.security.NullCredentialsSupplier(), anon)) {
                fail("a client without credentials must not get a session, got id " + c.clusterSessionId());
            } catch (final RuntimeException expected) {
                assertTrue("rejected on the wire: " + anon.events, anon.events.contains(EventCode.AUTHENTICATION_REJECTED)
                        || expected.getMessage().toLowerCase().contains("authentication"));
            }
            // 1b. A known key under the wrong class is rejected too.
            final Capture wrong = new Capture();
            try (AeronCluster c = connect(aeronDir, hosts, TestAuth.wrongClass(), wrong)) {
                fail("a key presented under the wrong class must not get a session");
            } catch (final RuntimeException expected) {
                // rejected
            }

            // 2. ADMIN connects, registers, gets AdminAck + the genesis gate state, and operates.
            final Capture adminCap = new Capture();
            admin = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.ADMIN), adminCap);
            TestAuth.register(admin, SessionClass.ADMIN);
            assertTrue(adminCap.await(admin, "ADMINACK ref=80").contains("result=ACCEPTED"));
            assertTrue(adminCap.await(admin, "ADMISSION market=0").contains("gate=SUSPENDED"));
            offer(admin, encodeSetJournal(0xAD1L));
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (!service.engine().isMoneyJournalEnabled() && System.currentTimeMillis() < deadline) {
                admin.pollEgress();
                Thread.onSpinWait();
            }
            assertTrue("ADMIN may arm the journal", service.engine().isMoneyJournalEnabled());

            // 3. BRIDGE: unregistered Deposit -> UNREGISTERED_SESSION; registered Deposit -> UNAUTHORIZED_CLASS.
            final Capture bridgeCap = new Capture();
            bridge = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.BRIDGE), bridgeCap);
            offer(bridge, encodeDeposit(0xB01L, 1));
            assertTrue(bridgeCap.await(bridge, "REJECT ref=1 reason=UNREGISTERED_SESSION corr=" + 0xB01L).contains("user=1"));
            TestAuth.register(bridge, SessionClass.BRIDGE);
            assertTrue(bridgeCap.await(bridge, "ADMINACK ref=80").contains("result=ACCEPTED"));
            offer(bridge, encodeDeposit(0xB02L, 1));
            bridgeCap.await(bridge, "REJECT ref=1 reason=UNAUTHORIZED_CLASS corr=" + 0xB02L);
            assertEquals("nothing applied", 0, service.engine().accountCount());
            assertTrue(service.demuxer().unauthorisedCount() >= 2);
            assertEquals("never a fence event", 0, service.demuxer().applyErrorCount());

            // 4. TREASURY scoped to user 7: user 8 refused NOT_OWNER, user 7 applied.
            final Capture tCap = new Capture();
            treasury7 = connect(aeronDir, hosts, TestAuth.treasuryForUser7(), tCap);
            TestAuth.register(treasury7, SessionClass.TREASURY);
            offer(treasury7, encodeDeposit(0x701L, 8));
            tCap.await(treasury7, "REJECT ref=1 reason=NOT_OWNER corr=" + 0x701L + " user=8");
            offer(treasury7, encodeDeposit(0x702L, 7));
            tCap.await(treasury7, "DEPOSITACK corr=" + 0x702L + " user=7");
            assertEquals(1, service.engine().accountCount());
            assertNotNull(service.engine().account(7));

            // 5. A non-resumable registration is refused and the session is closed by the leader.
            final Capture nr = new Capture();
            final AeronCluster nonResumable = connect(aeronDir, hosts, TestAuth.credentials(SessionClass.FORWARDER), nr);
            try {
                final UnsafeBuffer reg = new UnsafeBuffer(new byte[128]);
                final int len = TestAuth.encodeRegisterSession(reg, 0x55L, SessionClass.FORWARDER, false,
                        true, false, false, false, false, true);
                while (nonResumable.offer(reg, 0, len) <= 0) {
                    nonResumable.pollEgress();
                    Thread.onSpinWait();
                }
                assertTrue(nr.await(nonResumable, "ADMINACK ref=80").contains("result=REJECTED"));
                final long closeDeadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (!nr.events.contains(EventCode.CLOSED) && !nonResumable.isClosed()
                        && System.currentTimeMillis() < closeDeadline) {
                    nonResumable.pollEgress();
                    Thread.onSpinWait();
                }
                assertTrue("the leader closes a session it refused to register: " + nr.events,
                        nr.events.contains(EventCode.CLOSED) || nonResumable.isClosed());
            } finally {
                CloseHelper.quietClose(nonResumable);
            }
        } finally {
            CloseHelper.quietCloseAll(treasury7, bridge, admin, container, mediaDriver);
            IoUtil.delete(tmp, true);
        }
    }
}
