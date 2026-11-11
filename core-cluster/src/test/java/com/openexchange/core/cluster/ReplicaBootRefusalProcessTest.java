// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Boot refusals on the production entry point in a child JVM: the exit status and the
 * stderr a process manager sees, and nothing launched (no Aeron directory, no replica state directory).
 * <ul>
 *   <li>{@code CORE_ROLE=replica} with the argument {@code member}: exit 1,
 *       {@code CORE_ROLE=replica conflicts with the argument 'member'}; control: the same alias with the argument
 *       {@code replica} is no conflict and the child reaches the settings (refused there for its missing
 *       {@code CLUSTER_ADDRESSES}, never with the conflict text);</li>
 *   <li>members on non-local hosts without {@code CLUSTER_REPLICA_ADDRESS}: exit 1,
 *       {@code CORE REPLICA BOOT GUARD: CLUSTER_REPLICA_ADDRESS is required when members are not local}, before any
 *       Aeron component; control: local members without the address pass that guard and the child is refused at the
 *       next one (the credential), never with the address text.</li>
 * </ul>
 */
public class ReplicaBootRefusalProcessTest {

    static final String CONFLICT = "CORE_ROLE=replica conflicts with the argument 'member'";
    static final String ADDRESS = "CORE REPLICA BOOT GUARD: CLUSTER_REPLICA_ADDRESS is required when members are not"
            + " local";
    static final String NO_ADDRESSES = "CORE REPLICA BOOT GUARD: CLUSTER_ADDRESSES is required";
    static final String NO_CREDENTIAL = "CORE REPLICA BOOT GUARD: CORE_AUTH_CREDENTIAL";
    static final String REPLICA_CREDENTIAL = "8:REPLICA:" + "88".repeat(32);
    static final long EXIT_MS = 60_000;

    private Path dir;

    @After
    public void cleanUp() throws IOException {
        if (dir != null && Files.exists(dir)) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private Path dir() throws IOException {
        if (dir == null) {
            dir = Files.createTempDirectory("replica-boot-refusal-refusals");
        }
        return dir;
    }

    private Map<String, String> replicaEnv(final String hosts, final boolean credential) throws IOException {
        final Map<String, String> env = new HashMap<>();
        env.put("CLUSTER_ADDRESSES", hosts);
        env.put("AERON_DIR", dir().resolve("aeron-must-not-exist").toString());
        env.put("BASE_DIR", dir().resolve("state-must-not-exist").toString());
        env.put("TRANSPORT_DRIVER_MODE", "embedded");
        if (credential) {
            env.put("CORE_AUTH_CREDENTIAL", REPLICA_CREDENTIAL);
        }
        return env;
    }

    private void assertNothingLaunched(final ReplicaChild c) throws IOException {
        assertFalse("an Aeron directory was created" + c.tails(), Files.exists(dir().resolve("aeron-must-not-exist")));
        assertFalse("a replica state directory was created" + c.tails(),
                Files.exists(dir().resolve("state-must-not-exist")));
        assertFalse("the replica process started" + c.tails(), c.out().contains("[REPLICA] starting"));
    }

    @Test(timeout = 180_000)
    public void aRoleAliasThatConflictsWithTheArgumentExitsOne() throws Exception {
        final Map<String, String> env = replicaEnv("localhost,localhost,localhost", true);
        env.put("CORE_ROLE", "replica");
        final ReplicaChild c = ReplicaChild.start(dir(), "role-conflict", env, List.of("member"));
        assertEquals("exit status" + c.tails(), 1, c.awaitExit(EXIT_MS));
        assertTrue("stderr names the conflict" + c.tails(), c.err().contains(CONFLICT));
        assertNothingLaunched(c);
        System.out.println("GREEN role conflict: exit 1, " + ReplicaChild.line(c.err(), CONFLICT));

        // control: the same alias and a matching argument is no conflict; the settings refuse the missing addresses
        final Map<String, String> ctl = new HashMap<>(env);
        ctl.remove("CLUSTER_ADDRESSES");
        final ReplicaChild k = ReplicaChild.start(dir(), "role-match-control", ctl, List.of("replica"));
        assertEquals("control exit status" + k.tails(), 1, k.awaitExit(EXIT_MS));
        assertFalse("the conflict text without a conflict" + k.tails(), k.err().contains("conflicts with the argument"));
        assertTrue("the control reached the settings" + k.tails(), k.err().contains(NO_ADDRESSES));
        System.out.println("NC ROLE_MATCH -> " + ReplicaChild.line(k.err(), NO_ADDRESSES));
    }

    @Test(timeout = 180_000)
    public void nonLocalMembersWithoutAReplicaAddressExitOneBeforeAnyAeronComponent() throws Exception {
        final ReplicaChild c = ReplicaChild.start(dir(), "no-replica-address",
                replicaEnv("10.0.0.1,10.0.0.2,10.0.0.3", true), List.of("replica"));
        assertEquals("exit status" + c.tails(), 1, c.awaitExit(EXIT_MS));
        assertTrue("stderr names the precondition" + c.tails(), c.err().contains(ADDRESS));
        assertNothingLaunched(c);
        System.out.println("GREEN replica address: exit 1, " + ReplicaChild.line(c.err(), ADDRESS));

        // control: local members need no address; the next guard (the credential) refuses instead
        final Map<String, String> local = replicaEnv("localhost,localhost,localhost", false);
        local.put("CLUSTER_PORT_STRIDE", "100"); // co-located: the collision guard needs a stride
        final ReplicaChild k = ReplicaChild.start(dir(), "local-members-control", local, List.of("replica"));
        assertEquals("control exit status" + k.tails(), 1, k.awaitExit(EXIT_MS));
        assertFalse("the address text for local members" + k.tails(), k.err().contains("CLUSTER_REPLICA_ADDRESS"));
        assertTrue("the control passed the address guard" + k.tails(), k.err().contains(NO_CREDENTIAL));
        assertNothingLaunched(k);
        System.out.println("NC LOCAL_MEMBERS_NEED_NO_ADDRESS -> " + ReplicaChild.line(k.err(), NO_CREDENTIAL));
    }

    /**
     * An unwritable mismatch dump dir (a path under a regular file) is refused at boot with the guard prefix, exit 1,
     * before any Aeron component. Control: the same settings with a writable dir pass that guard (and the child then
     * starts, so it is stopped by the test).
     */
    @Test(timeout = 180_000)
    public void anUnwritableMismatchDirExitsOneBeforeAnyAeronComponent() throws Exception {
        final Path file = Files.createFile(dir().resolve("plain-file"));
        final String bad = file.resolve("mismatch").toString();
        final Map<String, String> env = replicaEnv("localhost,localhost,localhost", true);
        env.put("CLUSTER_PORT_STRIDE", "100");
        env.put("REPLICA_MISMATCH_DIR", bad);
        final ReplicaChild c = ReplicaChild.start(dir(), "mismatch-dir", env, List.of("replica"));
        final String text = "CORE REPLICA BOOT GUARD: REPLICA_MISMATCH_DIR is not writable: " + bad;
        assertEquals("exit status" + c.tails(), 1, c.awaitExit(EXIT_MS));
        assertTrue("stderr names the dir" + c.tails(), c.err().contains(text));
        assertNothingLaunched(c);
        System.out.println("GREEN mismatch dir: exit 1, " + ReplicaChild.line(c.err(), text));

        // control: a writable dir passes the guard; the child goes on to launch (stopped here)
        final Map<String, String> ok = new HashMap<>(env);
        ok.put("REPLICA_MISMATCH_DIR", dir().resolve("writable-mismatch").toString());
        ok.put("CORE_NODE_PORT", Integer.toString(ReplicaRig.freeTcp()));
        final ReplicaChild k = ReplicaChild.start(dir(), "mismatch-dir-control", ok, List.of("replica"));
        try {
            k.awaitLine("[REPLICA] starting", EXIT_MS);
            assertFalse("the dir text for a writable dir" + k.tails(), k.err().contains("REPLICA_MISMATCH_DIR is not"));
            assertTrue("the writable dir was created at boot", Files.isDirectory(dir().resolve("writable-mismatch")));
            System.out.println("NC WRITABLE_MISMATCH_DIR -> the child passed the guard and started");
        } finally {
            k.destroyForcibly();
        }
    }
}
