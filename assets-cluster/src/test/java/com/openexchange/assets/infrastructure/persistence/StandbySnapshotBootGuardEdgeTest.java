// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RCS S1 review (added by the independent verifier): the guard rows the slice's own test does not reach. G-R0 (no
 * cluster dir on the context), G-R2 (a present but unreadable {@code recording.log}: a directory, and a file whose
 * STANDBY entry Aeron's own parser rejects), G-R3 on a LONE standby entry (which Aeron would ignore as an incomplete
 * group, so the guard is stricter than the consensus module by design), the case-insensitive G-A2 parse, and the
 * G-A2-before-G-R3 order.
 */
public class StandbySnapshotBootGuardEdgeTest {

    private static final String PROP = StandbySnapshotBootGuard.ACCEPT_PROPERTY;

    private Path tmp;
    private String savedProp;

    @Before
    public void setUp() throws Exception {
        tmp = Files.createTempDirectory("assets-standby-guard-edge-");
        savedProp = System.getProperty(PROP);
        System.clearProperty(PROP);
    }

    @After
    public void tearDown() throws Exception {
        if (savedProp == null) {
            System.clearProperty(PROP);
        } else {
            System.setProperty(PROP, savedProp);
        }
        try (Stream<Path> s = Files.walk(tmp)) {
            s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static ConsensusModule.Context contextOn(final File clusterDir) {
        return new ConsensusModule.Context().acceptStandbySnapshots(false).clusterDir(clusterDir);
    }

    private File clusterDir() {
        final File dir = tmp.resolve(ClusterConfig.CLUSTER_SUB_DIR).toFile();
        assertTrue(dir.mkdirs());
        return dir;
    }

    @Test
    public void aContextWithoutAClusterDirIsRefused() {
        final ConsensusModule.Context bare = new ConsensusModule.Context().acceptStandbySnapshots(false);
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StandbySnapshotBootGuard.enforce(bare));
        assertTrue(r.getMessage(), r.getMessage().contains("clusterDir is not set"));
    }

    @Test
    public void aRecordingLogThatCannotBeOpenedIsRefusedWithTheCause() {
        final File dir = clusterDir();
        assertTrue(new File(dir, RecordingLog.RECORDING_LOG_FILE_NAME).mkdir()); // present, not a regular file
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StandbySnapshotBootGuard.enforce(contextOn(dir)));
        assertTrue(r.getMessage(), r.getMessage().contains("cannot read"));
        assertNotNull("the open failure rides as the cause", r.getCause());
    }

    @Test
    public void aStandbyEntryAeronsParserRejectsIsRefusedWithTheCause() throws Exception {
        final File dir = clusterDir();
        // One STANDBY_SNAPSHOT entry with an empty endpoint: RecordingLog.Entry throws on load.
        final ByteBuffer entry = ByteBuffer.allocate(RecordingLog.ENDPOINT_OFFSET + 4).order(ByteOrder.LITTLE_ENDIAN);
        entry.putLong(RecordingLog.RECORDING_ID_OFFSET, 7);
        entry.putLong(RecordingLog.LOG_POSITION_OFFSET, 4096);
        entry.putInt(RecordingLog.SERVICE_ID_OFFSET, 0);
        entry.putInt(RecordingLog.ENTRY_TYPE_OFFSET, RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT);
        entry.putInt(RecordingLog.ENDPOINT_OFFSET, 0);
        Files.write(dir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME), entry.array());
        assertThrows("Aeron itself refuses this file", RuntimeException.class, () -> new RecordingLog(dir, false).close());

        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StandbySnapshotBootGuard.enforce(contextOn(dir)));
        assertTrue(r.getMessage(), r.getMessage().contains("cannot read"));
        assertNotNull(r.getCause());
    }

    @Test
    public void aLoneStandbyEntryAeronWouldIgnoreIsStillRefused() throws Exception {
        final File dir = clusterDir();
        try (RecordingLog log = new RecordingLog(dir, true)) {
            log.appendStandbySnapshot(7, 0, 0, 4096, 2_000, 0, "localhost:20599"); // no term, no serviceId -1 twin
        }
        try (RecordingLog log = new RecordingLog(dir, false)) {
            assertTrue("Aeron: an incomplete group is not a replication candidate",
                    log.latestStandbySnapshots(1).isEmpty());
        }
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StandbySnapshotBootGuard.enforce(contextOn(dir)));
        assertTrue(r.getMessage(), r.getMessage().contains("1 STANDBY_SNAPSHOT recording-log entry in"));
        assertTrue(r.getMessage(), r.getMessage().contains("recordingId=7") && r.getMessage().contains("isValid=true"));
    }

    @Test
    public void theAcceptPropertyIsParsedCaseInsensitivelyAndCheckedBeforeTheLog() throws Exception {
        final File dir = clusterDir();
        try (RecordingLog log = new RecordingLog(dir, true)) {
            log.appendStandbySnapshot(7, 0, 0, 4096, 2_000, 0, "localhost:20599");
        }
        System.setProperty(PROP, "TRUE");
        final StandbySnapshotBootGuard.Refused r = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StandbySnapshotBootGuard.enforce(contextOn(dir)));
        assertTrue("G-A2 is reported, not G-R3: " + r.getMessage(), r.getMessage().contains(PROP));
        System.setProperty(PROP, "yes"); // not a boolean: Boolean.getBoolean is false, so G-R3 is reached
        final StandbySnapshotBootGuard.Refused standby = assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StandbySnapshotBootGuard.enforce(contextOn(dir)));
        assertTrue(standby.getMessage(), standby.getMessage().contains("STANDBY_SNAPSHOT"));
    }

    @Test
    public void aMissingClusterDirectoryItselfIsGenesis() {
        final File dir = tmp.resolve("never-created").resolve(ClusterConfig.CLUSTER_SUB_DIR).toFile();
        StandbySnapshotBootGuard.enforce(contextOn(dir)); // no throw: G-R1
        assertEquals(List.of(), listOf(dir));
    }

    private static List<String> listOf(final File dir) {
        final String[] names = dir.list();
        return names == null ? List.of() : List.of(names);
    }
}
