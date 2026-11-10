// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.feedharness;

import com.openexchange.core.infrastructure.persistence.CoreApplyRunner;

import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** The core-side commands of the CLI ({@code story}, {@code apply}, {@code serve}, T1). */
final class HarnessCommands {

    private HarnessCommands() {
    }

    static int run(final String[] args, final InputStream in, final PrintStream out, final PrintStream err) throws Exception {
        switch (args[0]) {
            case "story":
                return story(args, err);
            case "apply":
                return apply(args, out);
            case "serve":
                if (args.length != 1) {
                    throw new FeedHarnessMain.Usage("serve (requests on stdin, replies on stdout)");
                }
                return new CoreServe().loop(in, out);
            case "t1-journal":
                return T1Journal.main(args, in, out, err);
            case "t1-core":
                return T1Core.main(args, out, err);
            default:
                throw new FeedHarnessMain.Usage("unknown command " + args[0] + "\n" + FeedHarnessMain.usage());
        }
    }

    static final String STORY_USAGE = "story <seed> <rounds> <out-dir> [--burst <n> | --long-round <records>]";

    /** {@code story <seed> <rounds> <out-dir> [--burst <n> | --long-round <records>]} (6.4; the options:). */
    static int story(final String[] args, final PrintStream err) throws Exception {
        if (args.length != 4 && args.length != 6) {
            throw new FeedHarnessMain.Usage(STORY_USAGE);
        }
        final long seed;
        final int rounds;
        FeedStory.Options options = FeedStory.Options.NONE;
        try {
            seed = Long.parseLong(args[1]);
            rounds = Integer.parseInt(args[2]);
            if (args.length == 6) {
                final int n = Integer.parseInt(args[5]);
                options = switch (args[4]) {
                    case "--burst" -> FeedStory.Options.burst(n);
                    case "--long-round" -> FeedStory.Options.longRound(n);
                    default -> throw new FeedHarnessMain.Usage(STORY_USAGE + ": unknown option " + args[4]);
                };
                if (n < 1) {
                    throw new FeedHarnessMain.Usage(STORY_USAGE + ": " + args[4] + " needs a positive count");
                }
                options.pairTrades(); // range check: a usage error, not an internal one
            }
        } catch (IllegalArgumentException e) { // NumberFormatException included
            throw new FeedHarnessMain.Usage(STORY_USAGE + ": " + e.getMessage());
        }
        if (rounds < FeedStory.MIN_ROUNDS) {
            throw new FeedHarnessMain.Usage("story: rounds >= " + FeedStory.MIN_ROUNDS);
        }
        final long t0 = System.nanoTime();
        final FeedStory.Story s = FeedStory.build(seed, rounds, options);
        final long built = System.nanoTime();
        FeedStory.write(s, Path.of(args[3]));
        err.println("story " + seed + ": " + rounds + " rounds, " + s.length() + " records, clean snapshot " + s.expectedSha256());
        if (!s.info().isEmpty()) {
            err.println("story " + seed + ": " + s.info() + "; built in " + (built - t0) / 1_000_000 + " ms, written in "
                    + (System.nanoTime() - built) / 1_000_000 + " ms");
        }
        return FeedHarnessMain.OK;
    }

    /**
     * {@code apply <core.flog> <feed.ffr-or-flog> <out.json>} (6.2): a bare core replays the core log, then the feed (an FFR
     * on BRIDGE session 701, opened and registered if the core log did not; or an FLOG with its own sessions). The FFR
     * frames carry the core log's last time. Prints and writes the Result.
     */
    static int apply(final String[] args, final PrintStream out) throws Exception {
        if (args.length != 4) {
            throw new FeedHarnessMain.Usage("apply <core.flog> <feed.ffr-or-flog> <out.json>");
        }
        final CoreApplyRunner r = CoreApplyRunner.empty();
        long last = 0;
        for (final FeedFiles.LogEntry e : FeedFiles.decodeFlog(Files.readAllBytes(Path.of(args[1])))) {
            r.apply(e);
            last = Math.max(last, e.timestamp());
        }
        final byte[] feed = Files.readAllBytes(Path.of(args[2]));
        if (feed.length >= 4 && feed[0] == FeedFiles.FLOG_MAGIC[0] && feed[1] == FeedFiles.FLOG_MAGIC[1]
                && feed[2] == FeedFiles.FLOG_MAGIC[2] && feed[3] == FeedFiles.FLOG_MAGIC[3]) {
            for (final FeedFiles.LogEntry e : FeedFiles.decodeFlog(feed)) {
                r.apply(e);
            }
        } else {
            final List<byte[]> frames = FeedFiles.decodeFfr(feed);
            final var a = r.engine().sessions().get(CoreApplyRunner.BRIDGE_SESSION);
            if (a == null || !a.isRegistered()) {
                r.openBridge(CoreApplyRunner.BRIDGE_SESSION, CoreApplyRunner.BRIDGE_IDENTITY, last);
            }
            for (final byte[] f : frames) {
                r.apply(CoreApplyRunner.BRIDGE_SESSION, f, last);
            }
        }
        final String json = r.result().toJson();
        FeedFiles.writeAtomic(Path.of(args[3]), (json + "\n").getBytes(StandardCharsets.US_ASCII));
        out.println(json);
        return FeedHarnessMain.OK;
    }
}
