// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.infrastructure.persistence;

import com.openexchange.assets.determinism.AssetsEngineEvent;
import com.openexchange.assets.domain.EventKind;
import com.openexchange.assets.infrastructure.generated.AmendOrderDecoder;
import com.openexchange.assets.infrastructure.generated.CancelOrderDecoder;
import com.openexchange.assets.infrastructure.generated.CircuitBreakerDecoder;
import com.openexchange.assets.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.assets.infrastructure.generated.CreateOrderDecoder;
import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.MarketTickDecoder;
import com.openexchange.assets.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.assets.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.assets.infrastructure.generated.OrderEntryGateDecoder;
import com.openexchange.assets.infrastructure.generated.QuarantineOrderDecoder;
import com.openexchange.assets.infrastructure.generated.RegisterSessionDecoder;
import com.openexchange.assets.infrastructure.generated.RequestOrderSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestPendingDispatchDecoder;
import com.openexchange.assets.infrastructure.generated.RiskConfigSetDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RC1 S8, harness B4 test 3 (the ECR pattern at the FRAME level): two independent replicas
 * ({@link FrameReplica}: AssetsEngine + SettlementProjector + AssetsSbeDemuxer + RecordingAssetsSink) fed the same
 * ~300 ENCODED frames of a coherent lifecycle story ({@link CoreFrameStory}, production encoders, advancing log time)
 * must render identical event streams and serialize byte-identical v4 + envelope images after every 25 frames; and a
 * third replica restored from replica 1's image at ANY cut and fed the rest must converge to replica 1's final bytes
 * and to replica 1's events after the cut. This covers what the typed DSL runner skips: the frame gate, the decoders,
 * the session-class checks and the per-session authority that rides the snapshot.
 */
public class ReplicaFrameEquivalenceTest {

    private static final long SEED = 42L;
    private static final int FRAMES = 300;
    private static final int CHECK_EVERY = 25;

    private static List<CoreFrameStory.Step> story;

    @BeforeClass
    public static void buildStory() {
        final long t0 = System.nanoTime();
        story = CoreFrameStory.build(SEED, FRAMES);
        System.out.println("[FRAME-EQ] story seed=" + SEED + " steps=" + story.size() + " frames="
                + story.stream().filter(s -> s.kind() == CoreFrameStory.Kind.FRAME).count()
                + " buildMs=" + (System.nanoTime() - t0) / 1_000_000);
    }

    private static String where(final int i) {
        final CoreFrameStory.Step s = story.get(i);
        return "step " + i + " (" + s.kind() + " template=" + s.templateId() + " ts=" + s.timestamp() + ")";
    }

    @Test
    public void twoIndependentReplicasRenderTheSameEventsAndBytesEveryTwentyFiveFrames() {
        final long t0 = System.nanoTime();
        final FrameReplica a = new FrameReplica();
        final FrameReplica b = new FrameReplica();
        int frames = 0;
        int checked = 0;
        int checks = 0;
        for (int i = 0; i < story.size(); i++) {
            a.apply(story.get(i));
            b.apply(story.get(i));
            if (story.get(i).kind() == CoreFrameStory.Kind.FRAME) {
                frames++;
            }
            if ((story.get(i).kind() == CoreFrameStory.Kind.FRAME && frames % CHECK_EVERY == 0) || i == story.size() - 1) {
                assertEquals("event count at " + where(i), a.eventCount(), b.eventCount());
                assertEquals("events at " + where(i), a.rendered(checked), b.rendered(checked));
                checked = a.eventCount();
                assertArrayEquals("snapshot image at " + where(i), a.image(), b.image());
                checks++;
            }
        }
        for (final FrameReplica r : new FrameReplica[] {a, b}) {
            assertEquals("no apply fault on a well-formed story", 0, r.demuxer.applyErrorCount());
            assertEquals("no ingress drop on a well-formed story", 0, r.dropTotal());
            assertEquals("the compiled tables match the declared config", 0, r.failFastCalls);
        }
        assertEquals(a.engine.getUnauthorisedCount(), b.engine.getUnauthorisedCount());
        assertTrue("a checkpoint every 25 frames", checks >= FRAMES / CHECK_EVERY);
        System.out.println("[FRAME-EQ] twoReplicas checks=" + checks + " events=" + a.eventCount() + " imageBytes="
                + a.image().length + " ms=" + (System.nanoTime() - t0) / 1_000_000);
    }

    @Test
    public void replicaRestoredFromTheImageAtEveryCutConvergesToTheUninterruptedRun() {
        final long t0 = System.nanoTime();
        final FrameReplica reference = new FrameReplica();
        final byte[][] images = new byte[story.size() + 1][];
        final int[] eventsAt = new int[story.size() + 1];
        for (int i = 0; i < story.size(); i++) {
            images[i] = reference.image();
            eventsAt[i] = reference.eventCount();
            reference.apply(story.get(i));
        }
        images[story.size()] = reference.image();
        eventsAt[story.size()] = reference.eventCount();
        final byte[] finalImage = images[story.size()];
        long restoreNanos = 0;
        for (int cut = 0; cut <= story.size(); cut++) {
            final long r0 = System.nanoTime();
            final FrameReplica restored = FrameReplica.restore(images[cut]);
            restoreNanos += System.nanoTime() - r0;
            assertArrayEquals("a restored image re-serializes identically at cut " + cut, images[cut], restored.image());
            for (int i = cut; i < story.size(); i++) {
                restored.apply(story.get(i));
            }
            assertEquals("cross-check after restore at cut " + cut, 0, restored.failFastCalls);
            assertEquals("apply faults after restore at cut " + cut, 0, restored.demuxer.applyErrorCount());
            assertEquals("events after cut " + cut + " (" + (cut < story.size() ? where(cut) : "end") + ")",
                    reference.rendered(eventsAt[cut]), restored.rendered(0));
            assertArrayEquals("final bytes after restore at cut " + cut, finalImage, restored.image());
        }
        System.out.println("[FRAME-EQ] everyCut cuts=" + (story.size() + 1) + " restoreTotalMs=" + restoreNanos / 1_000_000
                + " ms=" + (System.nanoTime() - t0) / 1_000_000);
    }

    @Test
    public void storyCoversEveryTemplateAndLifecyclePath() {
        final Set<Integer> templates = new TreeSet<>();
        for (final CoreFrameStory.Step s : story) {
            if (s.kind() == CoreFrameStory.Kind.FRAME) {
                templates.add(s.templateId());
            }
        }
        for (final int t : new int[] {DepositDecoder.TEMPLATE_ID, WithdrawDecoder.TEMPLATE_ID, CreateOrderDecoder.TEMPLATE_ID,
                CancelOrderDecoder.TEMPLATE_ID, AmendOrderDecoder.TEMPLATE_ID, RequestOrderSnapshotDecoder.TEMPLATE_ID,
                MarketTickDecoder.TEMPLATE_ID, MeJournalTradeDecoder.TEMPLATE_ID, MeJournalTradeBatchDecoder.TEMPLATE_ID,
                MeJournalTerminalDecoder.TEMPLATE_ID, MeCommandOutcomeDecoder.TEMPLATE_ID, RequestPendingDispatchDecoder.TEMPLATE_ID,
                CoreConfigDecoder.TEMPLATE_ID, RiskConfigSetDecoder.TEMPLATE_ID, CircuitBreakerDecoder.TEMPLATE_ID,
                OrderEntryGateDecoder.TEMPLATE_ID, QuarantineOrderDecoder.TEMPLATE_ID, RegisterSessionDecoder.TEMPLATE_ID}) {
            assertTrue("template " + t + " in the story " + templates, templates.contains(t));
        }
        final FrameReplica r = CoreFrameStory.replay(story);
        final Map<EventKind, Integer> kinds = new EnumMap<>(EventKind.class);
        final Map<String, Integer> rejects = new TreeMap<>();
        final Set<Integer> orderTypes = new TreeSet<>();
        int dispatches = 0;
        for (final AssetsEngineEvent e : r.sink.events()) {
            if (e instanceof AssetsEngineEvent.OrderEvent oe) {
                kinds.merge(oe.kind(), 1, Integer::sum);
            } else if (e instanceof AssetsEngineEvent.OrderReject rj) {
                rejects.merge(rj.reason().name(), 1, Integer::sum);
            } else if (e instanceof AssetsEngineEvent.MeDispatch) {
                dispatches++;
            }
        }
        for (final CoreFrameStory.Step s : story) {
            if (s.templateId() == CreateOrderDecoder.TEMPLATE_ID) {
                orderTypes.add(s.bytes()[CoreFrameEncoders.HEADER + CreateOrderDecoder.orderTypeEncodingOffset()] & 0xFF);
            }
        }
        final var o = r.engine.orders();
        final var cfg = r.engine.coreConfig();
        System.out.println("[FRAME-EQ] coverage templates=" + templates + " orderTypes=" + orderTypes + " kinds=" + kinds
                + " rejects=" + rejects + " dispatches=" + dispatches + " triggered=" + o.triggeredCount() + " slices="
                + o.sliceCount() + " staleTicks=" + o.staleTicks() + " staleTerminal=" + o.staleTerminalCount()
                + " staleOutcome=" + o.staleOutcomeCount() + " redundantOutcome=" + o.redundantOutcomeCount()
                + " implicitAck=" + o.implicitAckCount() + " cancelRefused=" + o.cancelRefusedCount() + " faults="
                + o.orderFaultCount() + " cfg(adopt/dup/stale/switch)=" + cfg.adoptedCount() + "/" + cfg.duplicateCount()
                + "/" + cfg.staleCount() + "/" + cfg.switchCount() + " unauthorised=" + r.engine.getUnauthorisedCount()
                + " liveOrders=" + o.orders().size() + " accounts=" + r.engine.accountCount());
        assertEquals("every order type 0..6 is created", Set.of(0, 1, 2, 3, 4, 5, 6), orderTypes);
        for (final EventKind k : new EventKind[] {EventKind.ACCEPTED, EventKind.DISPATCHED, EventKind.ACK_NEW, EventKind.FILL,
                EventKind.CANCELLED, EventKind.EXPIRED, EventKind.REJECTED, EventKind.REPLACED, EventKind.TRIGGERED,
                EventKind.SLICE_SUBMITTED, EventKind.QUARANTINED}) {
            assertTrue("lifecycle path " + k + " reached: " + kinds, kinds.getOrDefault(k, 0) > 0);
        }
        assertTrue("CoreConfig adopt, duplicate, stale and the v2 switch", cfg.adoptedCount() == 1 && cfg.duplicateCount() >= 1
                && cfg.staleCount() >= 1 && cfg.switchCount() == 1);
        assertTrue("unauthorised traffic is refused by class/scope/registration", r.engine.getUnauthorisedCount() >= 1);
        assertTrue("stale ticks and stale terminals appear", o.staleTicks() > 0 && o.staleTerminalCount() > 0);
        assertEquals(0, r.dropTotal());
        assertEquals(0, r.demuxer.applyErrorCount());
    }
}
