// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.cluster;

import com.openexchange.assets.cluster.CoreRestoreRig.Encoders;
import com.openexchange.assets.cluster.ShadowClusterRig.Client;
import com.openexchange.assets.cluster.ShadowClusterRig.Member;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SessionClass;
import com.openexchange.assets.infrastructure.persistence.ServiceProbe;
import com.openexchange.cluster.auth.ClusterAuthenticator;
import com.openexchange.cluster.replica.BackupEvents;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.replica.ReplicaContexts;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupMediaDriver;
import io.aeron.security.Authenticator;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Who may BackupQuery the AE. Three members with the production {@code ClusterConfig} (the
 * kit authenticator and, with the replica, the AE class table) and an in-JVM {@code ClusterBackup} built by
 * {@link ReplicaContexts} on its own embedded driver, one credential per leg, a full member backup window between
 * legs (MBAT's {@code WINDOW_GAP_MS} pattern) and a query interval above the window:
 *
 * <pre>
 * (a) no credential   no onBackupResponse in 10 s; the authenticators' rejectedCount grows
 * (b) GATEWAY         authenticated (acceptedCount grows), BackupQuery denied: assets_backup_queries_denied_total >= 1
 *                     on a queried member, no response
 * (c) REPLICA         a response and live-log progress
 * (d) an AeronCluster client with the REPLICA credential: Deposit and Hold UNAUTHORIZED_CLASS on every member, a
 *     RegisterSession asserting GATEWAY REJECTED; ledgers unchanged
 * </pre>
 *
 * <p>RED on 98be859: (b) fails {@code GATEWAY received a backup response} because {@code ClusterConfig} installs no
 * authorisation service (Aeron's default allows BackupQuery for any authenticated principal, ABS:45-47). That base run
 * is this row's negative control.</p>
 */
public class ReplicaBackupAuthorityTest {

    private static final int PORT_BASE = 28600; // members 28600/28700/28800, backups at slots 3..5

    @Test(timeout = 600_000)
    public void onlyReplicaMayQueryAndReplicaMayNotIngress() throws Exception {
        try (ReplicaRig r = new ReplicaRig(PORT_BASE, "replica-backup-authority")) {
            final Map<Integer, ClusterAuthenticator> auth = new ConcurrentHashMap<>();
            for (final Member m : r.rig.members) {
                m.afterConfig = mm -> {
                    final Supplier<Authenticator> production = mm.cfg.consensusModuleContext().authenticatorSupplier();
                    mm.cfg.consensusModuleContext().authenticatorSupplier(() -> {
                        final Authenticator a = production.get();
                        auth.put(mm.id, (ClusterAuthenticator)a);
                        return a;
                    });
                };
            }
            r.bootAll();
            final Member leader = r.rig.awaitLeader(null);
            final Member host = r.rig.followerOtherThan(leader, null);
            r.story(host, 2);

            // (a) no credential
            final long rejectedBefore = sum(auth, ClusterAuthenticator::rejectedCount);
            try (Backup b = new Backup(r, ReplicaRig.SLOT, "none", new NullCredentialsSupplier())) {
                r.rig.pumpMs(10_000);
                assertEquals("a backup without a credential got a response", 0, b.events.responses());
                final long rejected = sum(auth, ClusterAuthenticator::rejectedCount) - rejectedBefore;
                assertTrue("no member authenticator rejected the credential-less query", rejected >= 1);
                System.out.println("[backup-authority] no credential: responses=0, authenticator rejections=" + rejected
                        + ", backup resets=" + b.events.resets());
            }
            r.windowGap();

            // (b) GATEWAY: authenticated, then denied by the class table
            final long acceptedBefore = sum(auth, ClusterAuthenticator::acceptedCount);
            final long[] deniedBefore = denied(r);
            try (Backup b = new Backup(r, ReplicaRig.SLOT + 1, "gateway", TestAuth.credentials(SessionClass.GATEWAY))) {
                final long deadline = System.currentTimeMillis() + 15_000;
                r.rig.await(() -> b.events.responses() > 0 || anyDenied(r, deniedBefore)
                        || System.currentTimeMillis() > deadline, 20_000, "pump");
                r.rig.pumpMs(2_000);
                if (b.events.responses() > 0) {
                    throw new AssertionError("GATEWAY received a backup response (supplier member "
                            + b.events.supplierMemberId() + "): any key in the AE table may BackupQuery");
                }
                final long accepted = sum(auth, ClusterAuthenticator::acceptedCount) - acceptedBefore;
                assertTrue("the GATEWAY credential was not authenticated (accepted delta " + accepted + ")",
                        accepted >= 1);
                final long[] deniedNow = denied(r);
                long deniedDelta = 0;
                for (int i = 0; i < deniedNow.length; i++) {
                    deniedDelta = Math.max(deniedDelta, deniedNow[i] - deniedBefore[i]);
                }
                assertTrue("assets_backup_queries_denied_total did not grow on any member", deniedDelta >= 1);
                System.out.println("[backup-authority] GATEWAY: authenticated (accepted +" + accepted + "), denied on the queried"
                        + " member (assets_backup_queries_denied_total +" + deniedDelta + "), responses=0, backup"
                        + " lastFailure=" + b.events.lastFailure());
            }
            r.windowGap();

            // (c) REPLICA: a response and live-log progress
            try (Backup b = new Backup(r, ReplicaRig.SLOT + 2, "replica", TestAuth.credentials(SessionClass.REPLICA))) {
                r.rig.await(() -> b.events.responses() > 0 && b.events.liveLogPosition() > 0, 60_000,
                        () -> "the REPLICA backup got no response and live log: responses=" + b.events.responses()
                                + " live=" + b.events.liveLogPosition() + " lastFailure=" + b.events.lastFailure());
                System.out.println("[backup-authority] REPLICA: responses=" + b.events.responses() + " live recording "
                        + b.events.liveRecordingId() + " at " + b.events.liveLogPosition() + " supplier member "
                        + b.events.supplierMemberId());
            }

            // (d) the REPLICA credential as an ingress client: nothing it sends is applied
            final Client replicaClient = r.rig.connect(SessionClass.REPLICA, host);
            final List<Member> live = r.rig.live();
            final long[] unauthorisedBefore = new long[live.size()];
            final int[] accountsBefore = new int[live.size()];
            for (int i = 0; i < live.size(); i++) {
                unauthorisedBefore[i] = live.get(i).service.demuxer().unauthorisedCount();
                accountsBefore[i] = live.get(i).service.engine().accountCount();
            }
            final Encoders enc = new Encoders();
            replicaClient.offer(enc.buf, enc.deposit(0x7D1L, 4242, CoreRestoreRig.usd(), ShadowClusterRig.ONE));
            final UnsafeBuffer hold = new UnsafeBuffer(new byte[128]);
            final HoldEncoder h = new HoldEncoder().wrapAndApplyHeader(hold, 0, new MessageHeaderEncoder())
                    .correlationId(0x7D2L).orderId(0x7D2L).userId(4242).assetId(CoreRestoreRig.usd())
                    .amount(ShadowClusterRig.ONE).omsManagedRelease(BoolFlag.FALSE);
            replicaClient.offer(hold, MessageHeaderEncoder.ENCODED_LENGTH + h.encodedLength());
            final UnsafeBuffer reg = new UnsafeBuffer(new byte[128]);
            replicaClient.offer(reg, TestAuth.encodeRegisterSession(reg, 0x7D3L, SessionClass.GATEWAY, true, true,
                    true, true, true, true, true, true));
            r.rig.await(() -> {
                for (int i = 0; i < live.size(); i++) {
                    if (live.get(i).service.demuxer().unauthorisedCount() < unauthorisedBefore[i] + 2) {
                        return false;
                    }
                }
                return replicaClient.in.count(f -> f.startsWith("ADMINACK ref=80")) > 0;
            }, 60_000, () -> "REPLICA ingress was not refused on every member: " + replicaClient.in.frames
                    + " first reject " + replicaClient.in.firstReject);
            assertTrue("the RegisterSession asserting GATEWAY from a REPLICA principal was accepted: "
                            + replicaClient.in.frames,
                    replicaClient.in.count(f -> f.startsWith("ADMINACK ref=80 result=REJECTED")) > 0);
            assertTrue("the REPLICA Deposit/Hold reply is UNAUTHORIZED_CLASS: " + replicaClient.in.firstReject,
                    String.valueOf(replicaClient.in.firstReject).contains("reason=UNAUTHORIZED_CLASS"));
            for (int i = 0; i < live.size(); i++) {
                assertEquals("member " + live.get(i).id + ": REPLICA ingress moved the ledger", accountsBefore[i],
                        live.get(i).service.engine().accountCount());
                assertEquals("member " + live.get(i).id + ": unauthorised count", unauthorisedBefore[i] + 2,
                        live.get(i).service.demuxer().unauthorisedCount());
            }
            System.out.println("[backup-authority] REPLICA ingress: Deposit and Hold UNAUTHORIZED_CLASS on every member,"
                    + " RegisterSession(GATEWAY) REJECTED, account counts unchanged");
            System.out.println("GREEN backup-authority");
        }
    }

    private static long sum(final Map<Integer, ClusterAuthenticator> auth,
                            final java.util.function.ToLongFunction<ClusterAuthenticator> f) {
        long n = 0;
        for (final ClusterAuthenticator a : auth.values()) {
            n += f.applyAsLong(a);
        }
        return n;
    }

    private static long[] denied(final ReplicaRig r) {
        final long[] out = new long[r.rig.members.size()];
        for (final Member m : r.rig.live()) {
            out[m.id] = ServiceProbe.backupQueriesDenied(m.service);
        }
        return out;
    }

    private static boolean anyDenied(final ReplicaRig r, final long[] before) {
        final long[] now = denied(r);
        for (int i = 0; i < now.length; i++) {
            if (now[i] > before[i]) {
                return true;
            }
        }
        return false;
    }

    /** A stock ClusterBackup (with archive and driver) from the replica's contexts, one credential. */
    static final class Backup implements AutoCloseable {
        final BackupEvents events = new BackupEvents(SystemNanoClock.INSTANCE);
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        final ClusterBackupMediaDriver driver;

        Backup(final ReplicaRig r, final int slot, final String name, final CredentialsSupplier credentials) {
            final ReplicaConfig cfg = r.builder(slot, "backup-" + name).credentials(credentials)
                    .backupCooldownMs(ReplicaConfig.DEFAULT_BACKUP_COOLDOWN_MS)
                    .backupQueryIntervalMs(ReplicaConfig.DEFAULT_BACKUP_COOLDOWN_MS)
                    .sourceType(ClusterBackup.SourceType.FOLLOWER).build();
            final ErrorHandler eh = errors::add;
            driver = ClusterBackupMediaDriver.launch(ReplicaContexts.driver(cfg).errorHandler(eh),
                    ReplicaContexts.archive(cfg, eh), ReplicaContexts.backup(cfg, events, eh, () -> { }));
        }

        @Override
        public void close() {
            CloseHelper.quietClose(driver);
        }
    }
}
