// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.replica;

import com.openexchange.cluster.auth.ClusterSessionClass;
import com.openexchange.cluster.replica.ReplicaConfig;
import com.openexchange.cluster.shadow.ShadowFloor;
import io.aeron.cluster.ClusterBackup;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Every replica setting: one row per numeric setting at min-1, min, max, max+1 and a non-integer, each refused with
 * its refusal text behind {@value ReplicaSettings#PREFIX}; the enumerated settings; the derived defaults; the boot
 * guards (the one-member FOLLOWER source, the {@code CLUSTER_REPLICA_ADDRESS} precondition, the REPLICA credential,
 * the port collision); the cool-down and query-interval CRITICAL lines below the members' backup window (warnings,
 * never refusals).
 */
public class ReplicaSettingsTest {

    static final String SECRET = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    static final String REPLICA_CREDENTIAL = "9:REPLICA:" + SECRET;

    /** A distributed baseline: three member hosts, the replica on its own host (no port collision possible). */
    static Map<String, String> base() {
        final Map<String, String> m = new HashMap<>();
        m.put("CLUSTER_ADDRESSES", "10.0.0.1,10.0.0.2,10.0.0.3");
        m.put("CLUSTER_REPLICA_ADDRESS", "10.0.0.9");
        m.put("AE_AUTH_CREDENTIAL", REPLICA_CREDENTIAL);
        return m;
    }

    static ReplicaSettings parse(final Map<String, String> values) {
        final Map<String, String> copy = Map.copyOf(values);
        return ReplicaSettings.parse((env, prop) -> copy.get(env));
    }

    static ReplicaSettings with(final String... kv) {
        final Map<String, String> m = base();
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] == null) {
                m.remove(kv[i]);
            } else {
                m.put(kv[i], kv[i + 1]);
            }
        }
        return parse(m);
    }

    static String refusal(final String... kv) {
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> with(kv));
        assertTrue("refusal carries the boot guard prefix: " + e.getMessage(),
                e.getMessage().startsWith(ReplicaSettings.PREFIX));
        return e.getMessage().substring(ReplicaSettings.PREFIX.length());
    }

    private record Range(String key, long min, long max, String[] extra) {
        Range(final String key, final long min, final long max) {
            this(key, min, max, new String[0]);
        }
    }

    private static final Range[] RANGES = {
            new Range("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", 1, 86_400_000L),
            new Range("CLUSTER_PORT_BASE", 1, 65_530),
            new Range("CLUSTER_PORT_STRIDE", 0, 10_000, new String[] {"CLUSTER_REPLICA_SLOT", "1"}),
            new Range("CLUSTER_REPLICA_SLOT", 0, 1000),
            new Range("CLUSTER_ID", 0, Integer.MAX_VALUE),
            new Range("ASSETS_NODE_PORT", 1, 65_535),
            new Range("REPLICA_MEMBER_BACKUP_WINDOW_MS", 1000, 3_600_000L,
                    new String[] {"REPLICA_VERIFY_WAIT_MS", Long.toString(ReplicaSettings.MAX_VERIFY_WAIT_MS)}),
            new Range("REPLICA_BACKUP_COOLDOWN_MS", 100, 3_600_000L,
                    new String[] {"REPLICA_VERIFY_WAIT_MS", Long.toString(ReplicaSettings.MAX_VERIFY_WAIT_MS)}),
            new Range("REPLICA_BACKUP_RESPONSE_TIMEOUT_MS", 100, 600_000L),
            new Range("REPLICA_BACKUP_QUERY_INTERVAL_MS", 100, 86_400_000L,
                    new String[] {"REPLICA_VERIFY_WAIT_MS", Long.toString(ReplicaSettings.MAX_VERIFY_WAIT_MS)}),
            new Range("REPLICA_BACKUP_PROGRESS_TIMEOUT_MS", 1000, 600_000L),
            new Range("REPLICA_IMAGE_LIVENESS_MS", 500, 60_000L),
            new Range("REPLICA_SHADOW_BUFFER_MB", 16, ReplicaConfig.MAX_SHADOW_BUFFER_MB),
            new Range("REPLICA_ALIGNED_WAIT_MS", 1000, 3_600_000L),
            new Range("REPLICA_DRIVER_MTU", 1024, 65_504),
            new Range("SNAPSHOT_STANDBY_INTERVAL_MINUTES", 0, 1440),
            new Range("SHADOW_STANDBY_FALLBACK_MINUTES", 0, 1440),
            new Range("REPLICA_RETRIEVED_SETS_KEEP", 1, 100),
            new Range("REPLICA_READY_MAX_LAG_BYTES", 4096, 1L << 40),
            new Range("REPLICA_READY_SHADOW_AGE_MS", 0, 86_400_000L),
            new Range("REPLICA_READY_STALE_MS", 100, 3_600_000L,
                    new String[] {"REPLICA_LIVENESS_STALE_MS", "3600000"}),
            new Range("REPLICA_LIVENESS_STALE_MS", 100, 3_600_000L, new String[] {"REPLICA_READY_STALE_MS", "100"}),
            new Range("REPLICA_READY_TIME_LAG_MS", 0, 86_400_000L),
            new Range("REPLICA_ATTACH_BACKOFF_MS", 1, ReplicaConfig.MAX_ATTACH_BACKOFF_MS),
            new Range("REPLICA_BOOTSTRAP_WAIT_MS", 1000, 86_400_000L),
            new Range("SHADOW_WRITE_CONNECT_TIMEOUT_MS", 1000, 3_600_000L),
            new Range("SHADOW_WRITE_OFFER_TIMEOUT_MS", 1000, 3_600_000L),
            new Range("SHADOW_WRITE_RECORD_TIMEOUT_MS", 1000, 3_600_000L),
    };

    private static String[] row(final Range r, final String value) {
        final String[] kv = new String[r.extra().length + 2];
        System.arraycopy(r.extra(), 0, kv, 0, r.extra().length);
        kv[kv.length - 2] = r.key();
        kv[kv.length - 1] = value;
        return kv;
    }

    @Test
    public void everyNumericSettingAtItsBoundaries() {
        int rows = 0;
        for (final Range r : RANGES) {
            final ReplicaSettings.Key key = ReplicaSettings.Key.valueOf(r.key());
            final String range = " out of range [" + r.min() + "," + r.max() + "]";
            assertEquals(r.key() + " min", r.min(), with(row(r, Long.toString(r.min()))).numeric(key));
            assertEquals(r.key() + " max", r.max(), with(row(r, Long.toString(r.max()))).numeric(key));
            assertEquals(r.key() + " min-1", r.key() + "=" + (r.min() - 1) + range,
                    refusal(row(r, Long.toString(r.min() - 1))));
            assertEquals(r.key() + " max+1", r.key() + "=" + (r.max() + 1) + range,
                    refusal(row(r, Long.toString(r.max() + 1))));
            assertEquals(r.key() + " non-integer", r.key() + "='12x' is not an integer", refusal(row(r, "12x")));
            rows += 5;
        }
        System.out.println("GREEN settings: " + RANGES.length + " numeric settings, " + rows + " boundary rows");
    }

    private record SizeRange(String key, String min, long minBytes, String max, long maxBytes) {
    }

    private static final SizeRange[] SIZES = {
            new SizeRange("REPLICA_CATCHUP_RCVBUF", "64k", 65_536, "64m", 67_108_864),
            new SizeRange("REPLICA_CATCHUP_RCV_WND", "64k", 65_536, "64m", 67_108_864),
            new SizeRange("REPLICA_CATCHUP_SNDBUF", "64k", 65_536, "64m", 67_108_864),
            new SizeRange("REPLICA_DRIVER_TERM_LENGTH", "64k", 65_536, "1g", 1_073_741_824),
            new SizeRange("SHADOW_TERM_LENGTH", "64k", 65_536, "16m", 16_777_216),
            new SizeRange("TRANSPORT_LOG_TERM_LENGTH", "64k", 65_536, "1g", 1_073_741_824),
    };

    @Test
    public void everySizeSettingAtItsBoundaries() {
        for (final SizeRange s : SIZES) {
            final ReplicaSettings.Key key = ReplicaSettings.Key.valueOf(s.key());
            final String range = " out of range [" + s.minBytes() + "," + s.maxBytes() + "]";
            assertEquals(s.key() + " min", s.minBytes(), with(s.key(), s.min()).numeric(key));
            assertEquals(s.key() + " max", s.maxBytes(), with(s.key(), s.max()).numeric(key));
            assertEquals(s.key() + " min-1", s.key() + "=" + (s.minBytes() - 1) + range,
                    refusal(s.key(), Long.toString(s.minBytes() - 1)));
            assertEquals(s.key() + " max+1", s.key() + "=" + (s.maxBytes() + 1) + range,
                    refusal(s.key(), Long.toString(s.maxBytes() + 1)));
            assertEquals(s.key() + " non-size", s.key() + "='4x' is not a size (bytes, or a k/m/g suffix)",
                    refusal(s.key(), "4x"));
        }
        assertEquals("term lengths are powers of two", "SHADOW_TERM_LENGTH=3m is not a power of two",
                refusal("SHADOW_TERM_LENGTH", "3m"));
        assertEquals("TRANSPORT_LOG_TERM_LENGTH=96k is not a power of two",
                refusal("TRANSPORT_LOG_TERM_LENGTH", "96k"));
        assertEquals("the raw text reaches the channel", "8m", with("REPLICA_CATCHUP_RCVBUF", "8m").catchupRcvbuf());
    }

    @Test
    public void enumeratedSettings() {
        assertEquals(ReplicaConfig.DriverMode.EXTERNAL, with("TRANSPORT_DRIVER_MODE", "external").driverMode());
        assertEquals(ReplicaConfig.DriverMode.EMBEDDED, with("TRANSPORT_DRIVER_MODE", "EMBEDDED").driverMode());
        assertEquals("TRANSPORT_DRIVER_MODE 'shared' (expected external|embedded)",
                refusal("TRANSPORT_DRIVER_MODE", "shared"));
        assertTrue(with("TRANSPORT_IDLE_MODE", "busy_spin").busySpin());
        assertFalse(with("TRANSPORT_IDLE_MODE", "backoff").busySpin());
        assertEquals("TRANSPORT_IDLE_MODE 'yield' (expected busy_spin|backoff)", refusal("TRANSPORT_IDLE_MODE", "yield"));
        assertEquals(ClusterBackup.SourceType.LEADER, with("REPLICA_BACKUP_SOURCE", "leader").sourceType());
        assertEquals(ClusterBackup.SourceType.ANY, with("REPLICA_BACKUP_SOURCE", "ANY").sourceType());
        assertEquals("REPLICA_BACKUP_SOURCE 'member' (expected FOLLOWER|LEADER|ANY)",
                refusal("REPLICA_BACKUP_SOURCE", "member"));
        assertEquals(ShadowFloor.Margin.NONE, with("SHADOW_FLOOR_MARGIN", "none").shared().floorMargin());
    }

    @Test
    public void defaultsAreTheSectionElevenValues() {
        final ReplicaSettings s = with();
        assertEquals(ReplicaConfig.DriverMode.EMBEDDED, s.driverMode());
        assertFalse("backoff", s.busySpin());
        assertEquals(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), s.memberHosts());
        assertEquals(9300, s.numeric(ReplicaSettings.Key.CLUSTER_PORT_BASE));
        assertEquals(0, s.numeric(ReplicaSettings.Key.CLUSTER_PORT_STRIDE));
        assertEquals(3, s.slot());
        assertEquals("node port 9700 + slot", 9703, s.nodePort());
        assertEquals(ClusterBackup.SourceType.FOLLOWER, s.sourceType());
        assertEquals(11_000, s.numeric(ReplicaSettings.Key.REPLICA_MEMBER_BACKUP_WINDOW_MS));
        assertEquals("cool-down = window + 1000", 12_000, s.numeric(ReplicaSettings.Key.REPLICA_BACKUP_COOLDOWN_MS));
        assertEquals(2_000, s.numeric(ReplicaSettings.Key.REPLICA_BACKUP_RESPONSE_TIMEOUT_MS));
        assertEquals(60_000, s.numeric(ReplicaSettings.Key.REPLICA_BACKUP_QUERY_INTERVAL_MS));
        assertEquals(10_000, s.numeric(ReplicaSettings.Key.REPLICA_BACKUP_PROGRESS_TIMEOUT_MS));
        assertEquals(3_000, s.numeric(ReplicaSettings.Key.REPLICA_IMAGE_LIVENESS_MS));
        assertEquals("4m", s.catchupRcvbuf());
        assertEquals("4m", s.catchupRcvWnd());
        assertEquals(512, s.numeric(ReplicaSettings.Key.REPLICA_SHADOW_BUFFER_MB));
        assertEquals(90_000, s.numeric(ReplicaSettings.Key.REPLICA_ALIGNED_WAIT_MS));
        assertEquals(15, s.numeric(ReplicaSettings.Key.SNAPSHOT_STANDBY_INTERVAL_MINUTES));
        assertEquals("fallback = 2 x standby", 30, s.numeric(ReplicaSettings.Key.SHADOW_STANDBY_FALLBACK_MINUTES));
        assertEquals(2, s.numeric(ReplicaSettings.Key.REPLICA_RETRIEVED_SETS_KEEP));
        assertEquals("max lag = TRANSPORT_LOG_TERM_LENGTH", 64L * 1024 * 1024,
                s.numeric(ReplicaSettings.Key.REPLICA_READY_MAX_LAG_BYTES));
        assertEquals("shadow age = 2 x standby", 30L * 60_000, s.numeric(ReplicaSettings.Key.REPLICA_READY_SHADOW_AGE_MS));
        assertEquals(5_000, s.numeric(ReplicaSettings.Key.REPLICA_READY_STALE_MS));
        assertEquals(30_000, s.numeric(ReplicaSettings.Key.REPLICA_LIVENESS_STALE_MS));
        assertEquals("time-lag default", 3_000, s.numeric(ReplicaSettings.Key.REPLICA_READY_TIME_LAG_MS));
        assertEquals("attach backoff default", 250, s.numeric(ReplicaSettings.Key.REPLICA_ATTACH_BACKOFF_MS));
        assertEquals("bootstrap wait default", 120_000, s.numeric(ReplicaSettings.Key.REPLICA_BOOTSTRAP_WAIT_MS));
        assertEquals(4L * 1024 * 1024, s.numeric(ReplicaSettings.Key.SHADOW_TERM_LENGTH));
        assertEquals(10_000, s.numeric(ReplicaSettings.Key.SHADOW_WRITE_CONNECT_TIMEOUT_MS));
        assertEquals(30_000, s.numeric(ReplicaSettings.Key.SHADOW_WRITE_OFFER_TIMEOUT_MS));
        assertEquals(60_000, s.numeric(ReplicaSettings.Key.SHADOW_WRITE_RECORD_TIMEOUT_MS));
        assertEquals(30_000, s.numeric(ReplicaSettings.Key.TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS));
        assertEquals(0, s.numeric(ReplicaSettings.Key.CLUSTER_ID));
        assertEquals("shared shadow keys", 3, s.shared().retainN());
        assertEquals(ShadowFloor.Margin.PREVIOUS_VERIFIED, s.shared().floorMargin());
        assertTrue("aeron dir names the replica slot: " + s.aeronDir(), s.aeronDir().endsWith("-assets-replica3-driver"));
        assertEquals(new File(System.getProperty("user.dir"), "ae-replica3"), s.baseDir());
        assertEquals(ClusterSessionClass.REPLICA, s.credentials().sessionClass());
        assertEquals(List.of(), s.warnings());
    }

    /** The driver MTU, its term lengths and the catch-up so-sndbuf are settings with the old defaults. */
    @Test
    public void driverMtuTermLengthAndCatchupSndbufAreSettings() {
        final ReplicaConfig d = with().config();
        final ReplicaConfig c = with("REPLICA_DRIVER_MTU", "1408", "REPLICA_DRIVER_TERM_LENGTH", "4m",
                "REPLICA_CATCHUP_SNDBUF", "2m").config();
        if (d.driverMtu() != 8192 || d.driverTermLength() != 16 * 1024 * 1024 || !"512k".equals(d.catchupSndbuf())
                || c.driverMtu() != 1408 || c.driverTermLength() != 4 * 1024 * 1024
                || !"2m".equals(c.catchupSndbuf())) {
            throw new AssertionError("REPLICA_DRIVER_MTU / REPLICA_DRIVER_TERM_LENGTH / REPLICA_CATCHUP_SNDBUF"
                    + " do not reach the kit config: defaults mtu=" + d.driverMtu() + " term=" + d.driverTermLength()
                    + " sndbuf=" + d.catchupSndbuf() + "; set mtu=" + c.driverMtu() + " term="
                    + c.driverTermLength() + " sndbuf=" + c.catchupSndbuf());
        }
        assertEquals("REPLICA_DRIVER_MTU=1400 is not a multiple of 32", refusal("REPLICA_DRIVER_MTU", "1400"));
        assertEquals("REPLICA_DRIVER_TERM_LENGTH=3m is not a power of two",
                refusal("REPLICA_DRIVER_TERM_LENGTH", "3m"));
    }

    @Test
    public void derivedDefaultsFollowTheirSources() {
        final ReplicaSettings s = with("SNAPSHOT_STANDBY_INTERVAL_MINUTES", "20", "REPLICA_MEMBER_BACKUP_WINDOW_MS",
                "21000", "TRANSPORT_LOG_TERM_LENGTH", "16m", "CLUSTER_REPLICA_SLOT", "7");
        assertEquals(40, s.numeric(ReplicaSettings.Key.SHADOW_STANDBY_FALLBACK_MINUTES));
        assertEquals(40L * 60_000, s.numeric(ReplicaSettings.Key.REPLICA_READY_SHADOW_AGE_MS));
        assertEquals(22_000, s.numeric(ReplicaSettings.Key.REPLICA_BACKUP_COOLDOWN_MS));
        assertEquals(16L * 1024 * 1024, s.numeric(ReplicaSettings.Key.REPLICA_READY_MAX_LAG_BYTES));
        assertEquals(9707, s.nodePort());
        // a derived value is capped at its setting's maximum, never refused for a value the operator did not write
        final ReplicaSettings day = with("SNAPSHOT_STANDBY_INTERVAL_MINUTES", "1440");
        assertEquals(1440, day.numeric(ReplicaSettings.Key.SHADOW_STANDBY_FALLBACK_MINUTES));
        assertEquals(86_400_000L, day.numeric(ReplicaSettings.Key.REPLICA_READY_SHADOW_AGE_MS));
        final ReplicaSettings off = with("SNAPSHOT_STANDBY_INTERVAL_MINUTES", "0");
        assertEquals("standby off: 2 x 0", 0, off.numeric(ReplicaSettings.Key.SHADOW_STANDBY_FALLBACK_MINUTES));
        assertEquals(0, off.numeric(ReplicaSettings.Key.REPLICA_READY_SHADOW_AGE_MS));
        // an explicit value wins over the derivation
        assertEquals(5, with("SHADOW_STANDBY_FALLBACK_MINUTES", "5").numeric(
                ReplicaSettings.Key.SHADOW_STANDBY_FALLBACK_MINUTES));
    }

    /** The verify wait and the stream ids are settings; the term default is the constant. */
    @Test
    public void verifyWaitAndStreamIdsAreSettings() {
        final ReplicaConfig d = with().config();
        assertEquals(ReplicaConfig.DEFAULT_VERIFY_WAIT_MS, d.verifyWaitMs());
        assertEquals(180_000, d.verifyWaitMs());
        assertEquals(100, d.logStreamId());
        assertEquals(java.util.Set.of(106, 107), d.snapshotStreamIds());
        assertEquals(ReplicaConfig.DEFAULT_DRIVER_TERM_LENGTH, d.driverTermLength());
        final ReplicaConfig c = with("REPLICA_VERIFY_WAIT_MS", "140000", "REPLICA_LOG_STREAM_ID", "200",
                "REPLICA_SNAPSHOT_STREAM_IDS", "206, 207").config();
        if (c.verifyWaitMs() != 140_000 || c.logStreamId() != 200
                || !java.util.Set.of(206, 207).equals(c.snapshotStreamIds())) {
            throw new AssertionError("settings not carried: verifyWaitMs=" + c.verifyWaitMs() + " logStreamId="
                    + c.logStreamId() + " snapshotStreamIds=" + c.snapshotStreamIds());
        }
        final String[][] refused = {
                {"REPLICA_VERIFY_WAIT_MS", "999", "REPLICA_VERIFY_WAIT_MS=999 out of range [1000,176400000]"},
                {"REPLICA_VERIFY_WAIT_MS", "176400001", "REPLICA_VERIFY_WAIT_MS=176400001 out of range [1000,176400000]"},
                {"REPLICA_LOG_STREAM_ID", "x", "REPLICA_LOG_STREAM_ID='x' is not an integer"},
                {"REPLICA_LOG_STREAM_ID", "0", "REPLICA_LOG_STREAM_ID=0 out of range [1,2147483647]"},
                {"REPLICA_SNAPSHOT_STREAM_IDS", "106,x", "REPLICA_SNAPSHOT_STREAM_IDS='106,x' is not a list of stream ids"},
                {"REPLICA_SNAPSHOT_STREAM_IDS", "106,100", "REPLICA_SNAPSHOT_STREAM_IDS=106,100 names the log stream 100"},
        };
        final java.util.List<String> wrong = new java.util.ArrayList<>();
        for (final String[] r : refused) {
            try {
                final String text = refusal(r[0], r[1]);
                if (!text.equals(r[2])) {
                    wrong.add(r[0] + "=" + r[1] + " -> '" + text + "' (expected '" + r[2] + "')");
                }
            } catch (final AssertionError notRefused) {
                wrong.add(r[0] + "=" + r[1] + " not refused");
            }
        }
        if (!wrong.isEmpty()) {
            throw new AssertionError("refusals wrong: " + wrong);
        }
    }

    /**
     * REPLICA_LOG_STREAM_ID and REPLICA_SNAPSHOT_STREAM_IDS are refused at boot when they name one of
     * the replica's own streams (shadow alias, shadow replay, follower replay). One row per reserved id and setting.
     */
    @Test
    public void reservedReplicaStreamIdsAreRefusedAtBoot() {
        final Object[][] reserved = {
                {com.openexchange.cluster.shadow.ShadowAlias.STREAM_ID, "shadow alias"},
                {com.openexchange.cluster.shadow.ShadowAlias.REPLAY_STREAM_ID, "shadow replay"},
                {ReplicaConfig.DEFAULT_REPLAY_STREAM_ID, "follower replay"},
        };
        final java.util.List<String> wrong = new java.util.ArrayList<>();
        for (final Object[] r : reserved) {
            final int id = (Integer)r[0];
            final String own = ", the replica's own " + r[1] + " stream";
            final String[][] rows = {
                    {"REPLICA_LOG_STREAM_ID", Integer.toString(id), "REPLICA_LOG_STREAM_ID names " + id + own},
                    {"REPLICA_SNAPSHOT_STREAM_IDS", "106," + id, "REPLICA_SNAPSHOT_STREAM_IDS names " + id + own},
            };
            for (final String[] row : rows) {
                try {
                    final String text = refusal(row[0], row[1]);
                    if (!text.equals(row[2])) {
                        wrong.add(row[0] + "=" + row[1] + " -> '" + text + "' (expected '" + row[2] + "')");
                    }
                } catch (final AssertionError notRefused) {
                    wrong.add(row[0] + "=" + row[1] + " not refused (expected '" + row[2] + "')");
                }
            }
        }
        if (!wrong.isEmpty()) {
            throw new AssertionError("reserved replica stream ids not refused at boot: " + wrong);
        }
    }

    @Test
    public void followerWithOneMemberIsRefused() {
        assertEquals("REPLICA_BACKUP_SOURCE=FOLLOWER needs >= 2 members; use ANY",
                refusal("CLUSTER_ADDRESSES", "10.0.0.1"));
        assertEquals(ClusterBackup.SourceType.ANY, with("CLUSTER_ADDRESSES", "10.0.0.1", "REPLICA_BACKUP_SOURCE",
                "ANY").sourceType());
        assertEquals(ClusterBackup.SourceType.LEADER, with("CLUSTER_ADDRESSES", "10.0.0.1", "REPLICA_BACKUP_SOURCE",
                "LEADER").sourceType());
        assertEquals(2, with("CLUSTER_ADDRESSES", "10.0.0.1,10.0.0.2").memberHosts().size());
    }

    @Test
    public void replicaAddressDefaultsToLocalhostOnlyWhenEveryMemberIsLocal() {
        assertEquals("CLUSTER_REPLICA_ADDRESS is required when members are not local",
                refusal("CLUSTER_REPLICA_ADDRESS", null));
        assertEquals("CLUSTER_REPLICA_ADDRESS is required when members are not local",
                refusal("CLUSTER_REPLICA_ADDRESS", null, "CLUSTER_ADDRESSES", "localhost,10.0.0.2,127.0.0.1"));
        final ReplicaSettings local = with("CLUSTER_REPLICA_ADDRESS", null, "CLUSTER_ADDRESSES",
                "localhost,127.0.0.1,localhost", "CLUSTER_PORT_STRIDE", "100");
        assertEquals("localhost", local.replicaAddress());
        assertEquals("10.0.0.9", with().replicaAddress());
    }

    @Test
    public void replicaPortsMustNotCollideWithAMemberOnTheSameHost() {
        assertEquals("CLUSTER_REPLICA_SLOT=1 collides with member 1 on localhost (port 9401); use a slot >= 3",
                refusal("CLUSTER_ADDRESSES", "localhost,localhost,localhost", "CLUSTER_REPLICA_ADDRESS", "localhost",
                        "CLUSTER_PORT_STRIDE", "100", "CLUSTER_REPLICA_SLOT", "1"));
        assertEquals("CLUSTER_REPLICA_SLOT=3 collides with member 0 on 127.0.0.1 (port 9301); co-located processes"
                        + " need CLUSTER_PORT_STRIDE > 0",
                refusal("CLUSTER_ADDRESSES", "127.0.0.1,localhost,localhost", "CLUSTER_REPLICA_ADDRESS", "localhost"));
        // a member on another host never collides, whatever the stride
        assertEquals(1, with("CLUSTER_REPLICA_SLOT", "1").slot());
        assertEquals("replica port 65605 (CLUSTER_PORT_BASE=65300 + slot 3 x CLUSTER_PORT_STRIDE=100 + 5) is above 65535",
                refusal("CLUSTER_PORT_BASE", "65300", "CLUSTER_PORT_STRIDE", "100"));
    }

    @Test
    public void credentialMustBeAReplicaCredential() {
        assertEquals("AE_AUTH_CREDENTIAL (-Dassets.auth.credential) is not set; this client cannot authenticate to the"
                + " assets cluster", refusal("AE_AUTH_CREDENTIAL", null));
        assertEquals("AE_AUTH_CREDENTIAL claims class GATEWAY (keyId 9); this client must present [REPLICA]",
                refusal("AE_AUTH_CREDENTIAL", "9:GATEWAY:" + SECRET));
        assertTrue(refusal("AE_AUTH_CREDENTIAL", "9:REPLICA").startsWith(
                "AE_AUTH_CREDENTIAL (-Dassets.auth.credential) is malformed: "));
        final String shown = with().toString();
        assertFalse("the secret never reaches a log line: " + shown, shown.contains(SECRET));
    }

    @Test
    public void clusterAddressesAreRequiredAndNonEmpty() {
        assertEquals("CLUSTER_ADDRESSES is required (one host per member)", refusal("CLUSTER_ADDRESSES", null));
        assertEquals("CLUSTER_ADDRESSES has an empty entry: '10.0.0.1,,10.0.0.3'",
                refusal("CLUSTER_ADDRESSES", "10.0.0.1,,10.0.0.3"));
        assertEquals(List.of("10.0.0.1", "10.0.0.2"), with("CLUSTER_ADDRESSES", " 10.0.0.1 , 10.0.0.2 ").memberHosts());
    }

    @Test
    public void livenessMustNotBeShorterThanReadiness() {
        assertEquals("REPLICA_LIVENESS_STALE_MS=4000 must be >= REPLICA_READY_STALE_MS=5000",
                refusal("REPLICA_LIVENESS_STALE_MS", "4000"));
    }

    @Test
    public void sharedShadowKeysAreRcsSettingsWithTheMemberTexts() {
        assertEquals("SHADOW_RETAIN_N=0 out of range [1,1000]", refusal("SHADOW_RETAIN_N", "0"));
        assertEquals("SHADOW_SCAN_PERIOD_MS='x' is not an integer", refusal("SHADOW_SCAN_PERIOD_MS", "x"));
        assertEquals(5, with("SHADOW_RETAIN_N", "5").shared().retainN());
    }

    @Test
    public void coolDownAndQueryIntervalBelowTheWindowAreCriticalNotRefused() {
        final ReplicaSettings s = with("REPLICA_BACKUP_COOLDOWN_MS", "1000");
        assertEquals(1000, s.numeric(ReplicaSettings.Key.REPLICA_BACKUP_COOLDOWN_MS));
        assertEquals(1, s.warnings().size());
        assertTrue(s.warnings().get(0), s.warnings().get(0).startsWith("CRITICAL: REPLICA_BACKUP_COOLDOWN_MS=1000"));
        assertTrue(s.warnings().get(0), s.warnings().get(0).contains("below the members' backup window"));
        System.out.println("CRITICAL line: " + s.warnings().get(0));
        final ReplicaSettings q = with("REPLICA_BACKUP_QUERY_INTERVAL_MS", "5000");
        assertEquals(1, q.warnings().size());
        assertTrue(q.warnings().get(0), q.warnings().get(0).startsWith("CRITICAL: REPLICA_BACKUP_QUERY_INTERVAL_MS=5000"));
        // the window is a setting: a member running a 60 s session timeout moves the line
        final ReplicaSettings wide = with("REPLICA_MEMBER_BACKUP_WINDOW_MS", "61000", "REPLICA_BACKUP_COOLDOWN_MS",
                "12000");
        assertEquals(2, wide.warnings().size());
        assertTrue(wide.warnings().get(0).contains("REPLICA_BACKUP_COOLDOWN_MS=12000"));
        assertTrue(wide.warnings().get(1).contains("REPLICA_BACKUP_QUERY_INTERVAL_MS=60000"));
    }

    @Test
    public void baseDirOnTmpfsIsWarned() {
        final ReplicaSettings s = with("BASE_DIR", "/dev/shm/replica");
        assertEquals(new File("/dev/shm/replica"), s.baseDir());
        assertEquals(1, s.warnings().size());
        assertTrue(s.warnings().get(0), s.warnings().get(0).startsWith("WARNING: ASSETS ENGINE REPLICA STATE ON TMPFS"));
    }

    @Test
    public void theKitConfigCarriesEverySetting() {
        final ReplicaConfig cfg = with("REPLICA_BACKUP_SOURCE", "ANY", "CLUSTER_REPLICA_SLOT", "4", "REPLICA_BACKUP_COOLDOWN_MS",
                "15000", "REPLICA_BACKUP_QUERY_INTERVAL_MS", "30000", "TRANSPORT_DRIVER_MODE", "external", "AERON_DIR",
                "/dev/shm/aeron-ext", "BASE_DIR", "/var/lib/ae-replica").config();
        assertEquals(ClusterBackup.SourceType.ANY, cfg.sourceType());
        assertEquals(4, cfg.slot());
        assertEquals(9704, cfg.nodePort());
        assertEquals(15_000, cfg.backupCooldownMs());
        assertEquals(30_000, cfg.backupQueryIntervalMs());
        assertEquals(11_000, cfg.memberBackupWindowMs());
        assertEquals(ReplicaConfig.DriverMode.EXTERNAL, cfg.driverMode());
        assertEquals("/dev/shm/aeron-ext", cfg.aeronDir());
        assertEquals(new File("/var/lib/ae-replica"), cfg.baseDir());
        assertEquals(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), cfg.memberHosts());
        assertEquals("10.0.0.9", cfg.replicaAddress());
        assertEquals("the assets engine id", 1, cfg.engineId());
        assertTrue("no test hooks in a production config", cfg.negativeControls().isEmpty());
    }
    /**
     * The stall bound cannot be shorter than one backup cycle. The default is 180 000 (it passes
     * with the default query interval and cool-down); a value below 2 x {@code REPLICA_BACKUP_QUERY_INTERVAL_MS} +
     * {@code REPLICA_BACKUP_COOLDOWN_MS} is refused with a text naming the three values.
     */
    @Test
    public void aVerifyWaitShorterThanOneBackupCycleIsRefused() {
        final java.util.List<String> wrong = new java.util.ArrayList<>();
        final long d = with().config().verifyWaitMs();
        if (d != 180_000) {
            wrong.add("default REPLICA_VERIFY_WAIT_MS=" + d + " (expected 180000)");
        }
        final String[][] rows = {
                {"REPLICA_VERIFY_WAIT_MS=131999 must be >= 2 x REPLICA_BACKUP_QUERY_INTERVAL_MS=60000 +"
                        + " REPLICA_BACKUP_COOLDOWN_MS=12000 = 132000 (the stall bound cannot be shorter than one"
                        + " backup cycle)", "REPLICA_VERIFY_WAIT_MS", "131999"},
                {"REPLICA_VERIFY_WAIT_MS=74999 must be >= 2 x REPLICA_BACKUP_QUERY_INTERVAL_MS=30000 +"
                        + " REPLICA_BACKUP_COOLDOWN_MS=15000 = 75000 (the stall bound cannot be shorter than one"
                        + " backup cycle)", "REPLICA_VERIFY_WAIT_MS", "74999", "REPLICA_BACKUP_QUERY_INTERVAL_MS",
                        "30000", "REPLICA_BACKUP_COOLDOWN_MS", "15000"},
        };
        for (final String[] row : rows) {
            final String[] kv = java.util.Arrays.copyOfRange(row, 1, row.length);
            try {
                final String text = refusal(kv);
                if (!text.equals(row[0])) {
                    wrong.add("'" + text + "' (expected '" + row[0] + "')");
                }
            } catch (final AssertionError accepted) {
                wrong.add("accepted " + java.util.Arrays.toString(kv) + " (expected '" + row[0] + "')");
            }
        }
        if (!wrong.isEmpty()) {
            throw new AssertionError("a verify wait shorter than one backup cycle was not refused: " + wrong);
        }
        assertEquals(132_000, with("REPLICA_VERIFY_WAIT_MS", "132000").config().verifyWaitMs());
    }
}
