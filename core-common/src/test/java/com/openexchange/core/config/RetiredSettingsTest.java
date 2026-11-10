// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.config;

import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The retired-name table: a retired environment variable, system property or flag that is set yields the one refusal
 * line naming the old and the new name; the new names alone yield none.
 */
public class RetiredSettingsTest {

    private static final Map<String, String> NONE = Map.of();

    private static String refusal(final Map<String, String> env, final Map<String, String> props) {
        return RetiredSettings.refusal("core-cluster.jar", env::get, props::get);
    }

    @Test
    public void aRetiredEnvironmentVariableIsRefusedNamingTheNewName() {
        assertEquals("FATAL: core-cluster.jar refused to start: the environment variable ASSETS_NODE_PORT is retired;"
                + " it is now CORE_NODE_PORT (rename the setting; the old name is never read)",
                refusal(Map.of("ASSETS_NODE_PORT", "9790"), NONE));
    }

    @Test
    public void aRetiredSystemPropertyIsRefusedNamingTheNewName() {
        assertEquals("FATAL: core-cluster.jar refused to start: the system property rcs.mode is retired;"
                + " it is now snapshot.mode (rename the setting; the old name is never read)",
                refusal(NONE, Map.of("rcs.mode", "shadow")));
    }

    @Test
    public void anEmptyRetiredValueStillCountsAsSet() {
        assertTrue(refusal(Map.of("AE_AUTH_CREDENTIAL", ""), NONE).contains("AE_AUTH_CREDENTIAL is retired"));
    }

    @Test
    public void aRetiredFlagIsRefusedNamingTheNewFlag() {
        final String r = RetiredSettings.flagRefusal("core-loadgen.jar", new String[]{"--duration", "5", "--ae-port-base=9300"});
        assertEquals("FATAL: core-loadgen.jar refused to start: the command-line flag --ae-port-base is retired;"
                + " it is now --core-port-base (rename the setting; the old name is never read)", r);
        assertNull(RetiredSettings.flagRefusal("core-loadgen.jar", new String[]{"--core-only", "--core-hosts", "a"}));
    }

    @Test
    public void theNewNamesAloneAreNotRefused() {
        final Map<String, String> env = new java.util.HashMap<>();
        final Map<String, String> props = new java.util.HashMap<>();
        for (final RetiredSettings.Rename r : RetiredSettings.TABLE) {
            (r.kind() == RetiredSettings.Kind.ENV ? env : props).put(r.replacement(), "x");
        }
        assertNull(refusal(env, props));
    }

    @Test
    public void everyEntryOfTheTableIsRefusedAndNamesItsReplacement() {
        for (final RetiredSettings.Rename r : RetiredSettings.TABLE) {
            final String line = switch (r.kind()) {
                case ENV -> refusal(Map.of(r.retired(), "1"), NONE);
                case PROPERTY -> refusal(NONE, Map.of(r.retired(), "1"));
                case FLAG -> RetiredSettings.flagRefusal("core-cluster.jar", new String[]{r.retired()});
            };
            assertTrue(r + ": " + line, line != null && line.contains(" " + r.retired() + " is retired; it is now "
                    + r.replacement() + " "));
            assertFalse("one line: " + line, line.contains("\n"));
        }
    }

    @Test
    public void theTableMapsDistinctOldNamesOntoDistinctNewNames() {
        final Set<String> retired = new HashSet<>();
        final Set<String> replacements = new HashSet<>();
        for (final RetiredSettings.Rename r : RetiredSettings.TABLE) {
            assertTrue("duplicate retired name " + r.retired(), retired.add(r.kind() + r.retired()));
            assertTrue("two old names onto " + r.replacement(), replacements.add(r.kind() + r.replacement()));
            assertFalse(r + " maps a name onto itself", r.retired().equals(r.replacement()));
        }
        for (final RetiredSettings.Rename r : RetiredSettings.TABLE) {
            assertFalse(r.replacement() + " is also retired", retired.contains(r.kind() + r.replacement()));
        }
    }
}
