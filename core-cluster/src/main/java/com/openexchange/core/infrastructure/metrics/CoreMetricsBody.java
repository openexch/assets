// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Prometheus exposition for a core node, served by cluster-kit's {@code NodeEndpoint}
 * ({@code /metrics} next to {@code /health} and {@code /ready}). The matching engine's
 * {@code NodeMetricsServer} minus the HTTP server, which the endpoint already owns.
 *
 * <p>Counter and gauge suppliers are registered at startup and evaluated per scrape on the
 * endpoint's thread; they must read plain longs or Aeron counters and never take a lock the service
 * thread can hold. {@link CoreNodeMetrics} state is read after {@link CoreNodeMetrics#acquire()}.</p>
 */
public final class CoreMetricsBody {

    private final CoreNodeMetrics metrics;
    private final Map<String, LongSupplier> counters = new LinkedHashMap<>();
    private final Map<String, LongSupplier> gauges = new LinkedHashMap<>();
    private final Map<String, Labelled> labelledGauges = new LinkedHashMap<>();

    /** One gauge family with one label: {@code name{label="v"} value} per entry, one HELP and TYPE line. */
    private record Labelled(String help, String label, String[] values, LongSupplier[] suppliers) { }

    public CoreMetricsBody(final CoreNodeMetrics metrics) {
        this.metrics = metrics;
    }

    /** Monotonic counter; name must end in _total per Prometheus convention. */
    public CoreMetricsBody counter(final String name, final String help, final LongSupplier value) {
        counters.put(name + " " + help, value);
        return this;
    }

    public CoreMetricsBody gauge(final String name, final String help, final LongSupplier value) {
        gauges.put(name + " " + help, value);
        return this;
    }

    /**
     * A gauge family with one label ({@code core_sessions{class="BRIDGE"}}); {@code values[i]} is
     * read from {@code suppliers[i]}. A family name must not also be registered unlabelled.
     */
    public CoreMetricsBody labelledGauge(final String name, final String help, final String label, final String[] values,
                                           final LongSupplier[] suppliers) {
        if (values.length != suppliers.length) {
            throw new IllegalArgumentException(name + ": " + values.length + " label values for " + suppliers.length + " suppliers");
        }
        labelledGauges.put(name, new Labelled(help, label, values.clone(), suppliers.clone()));
        return this;
    }

    /** Register a group of series (one appended chain call per registrar, critic 1.2 seam 1). */
    public CoreMetricsBody include(final java.util.function.Consumer<CoreMetricsBody> registrar) {
        registrar.accept(this);
        return this;
    }

    public String render() {
        metrics.acquire(); // volatile read: makes service-thread plain writes visible
        final StringBuilder sb = new StringBuilder(8192);

        for (final Map.Entry<String, LongSupplier> e : counters.entrySet()) {
            appendSeries(sb, e.getKey(), "counter", e.getValue().getAsLong());
        }
        for (final Map.Entry<String, LongSupplier> e : gauges.entrySet()) {
            appendSeries(sb, e.getKey(), "gauge", e.getValue().getAsLong());
        }
        for (final Map.Entry<String, Labelled> e : labelledGauges.entrySet()) {
            final Labelled l = e.getValue();
            sb.append("# HELP ").append(e.getKey()).append(' ').append(l.help()).append('\n');
            sb.append("# TYPE ").append(e.getKey()).append(" gauge\n");
            for (int i = 0; i < l.values().length; i++) {
                sb.append(e.getKey()).append('{').append(l.label()).append("=\"").append(l.values()[i]).append("\"} ")
                        .append(l.suppliers()[i].getAsLong()).append('\n');
            }
        }

        appendSeries(sb, "core_cluster_role Cluster role code (0 follower, 1 candidate, 2 leader)",
                "gauge", metrics.role());
        appendSeries(sb, "core_member_id Cluster member id", "gauge", metrics.memberId());
        appendSeries(sb, "core_log_position Cluster log position of the last ingress message this node applied "
                + "(-1 = none since start)", "gauge", metrics.lastLogPosition());
        appendSeries(sb, "core_apply_sample_shift Apply timing samples 1 in 2^shift messages (0 = every message; "
                + "CORE_APPLY_SAMPLE_SHIFT)", "gauge", metrics.sampleShift());

        appendHistogram(sb, "core_apply_dispatch_seconds",
                "Service-thread cost of the deterministic apply of one ingress message (decode + ledger mutation + "
                        + "egress enqueue; sampled per core_apply_sample_shift)", metrics.dispatch());
        appendHistogram(sb, "core_apply_drain_seconds",
                "Service-thread cost of the egress drain that follows each applied message (leader only does work; "
                        + "followers return at once)", metrics.drain());
        appendHistogram(sb, "core_timer_drain_seconds",
                "Service-thread cost of the idle-path drain belt (10 ms backstop timer, leader only)",
                metrics.timerDrain());

        sb.append("# HELP core_apply_template_dispatch_seconds_sum Summed apply cost per money-schema templateId\n");
        sb.append("# TYPE core_apply_template_dispatch_seconds_sum counter\n");
        for (int t = 0; t < CoreNodeMetrics.TEMPLATE_SLOTS; t++) {
            if (metrics.templateCount(t) > 0) {
                sb.append("core_apply_template_dispatch_seconds_sum{template=\"").append(t).append("\"} ")
                        .append(seconds(metrics.templateSumNanos(t))).append('\n');
            }
        }
        sb.append("# HELP core_apply_template_dispatch_count Applied messages per money-schema templateId\n");
        sb.append("# TYPE core_apply_template_dispatch_count counter\n");
        for (int t = 0; t < CoreNodeMetrics.TEMPLATE_SLOTS; t++) {
            if (metrics.templateCount(t) > 0) {
                sb.append("core_apply_template_dispatch_count{template=\"").append(t).append("\"} ")
                        .append(metrics.templateCount(t)).append('\n');
            }
        }
        sb.append("# HELP core_apply_template_dispatch_max_seconds Worst apply cost per money-schema templateId\n");
        sb.append("# TYPE core_apply_template_dispatch_max_seconds gauge\n");
        for (int t = 0; t < CoreNodeMetrics.TEMPLATE_SLOTS; t++) {
            if (metrics.templateCount(t) > 0) {
                sb.append("core_apply_template_dispatch_max_seconds{template=\"").append(t).append("\"} ")
                        .append(seconds(metrics.templateMaxNanos(t))).append('\n');
            }
        }

        final long snapMs = metrics.lastSnapshotMs();
        appendSeries(sb, "core_snapshot_age_seconds Seconds since the last cluster snapshot this node took "
                + "(-1 = never)", "gauge", snapMs == 0 ? -1 : (System.currentTimeMillis() - snapMs) / 1000);
        appendSeries(sb, "core_snapshot_last_bytes Serialized ledger size of the last snapshot (-1 = never)",
                "gauge", metrics.lastSnapshotBytes());
        appendSeries(sb, "core_snapshot_last_chunks Aeron messages the last snapshot was offered as (-1 = never)",
                "gauge", metrics.lastSnapshotChunks());
        appendSeries(sb, "core_snapshot_last_log_position Cluster log position of the last snapshot (-1 = never)",
                "gauge", metrics.lastSnapshotLogPosition());
        appendSecondsGauge(sb, "core_snapshot_last_serialize_seconds",
                "Service-thread time to serialize the ledger for the last snapshot (-1 = never)",
                metrics.lastSnapshotSerializeNanos());
        appendSecondsGauge(sb, "core_snapshot_last_offer_seconds",
                "Service-thread time to offer the last snapshot's chunks (-1 = never)",
                metrics.lastSnapshotOfferNanos());
        appendSecondsGauge(sb, "core_snapshot_last_record_seconds",
                "Wait until the archive had ingested the last snapshot (-1 = never)",
                metrics.lastSnapshotRecordNanos());
        return sb.toString();
    }

    private static void appendHistogram(final StringBuilder sb, final String name, final String help,
                                        final CoreNodeMetrics.Histogram h) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" histogram\n");
        long cumulative = 0;
        for (int i = 0; i < CoreNodeMetrics.BUCKET_LE.length; i++) {
            cumulative += h.bucketCount(i);
            sb.append(name).append("_bucket{le=\"").append(CoreNodeMetrics.BUCKET_LE[i]).append("\"} ")
                    .append(cumulative).append('\n');
        }
        cumulative += h.bucketCount(CoreNodeMetrics.BUCKETS - 1);
        sb.append(name).append("_bucket{le=\"+Inf\"} ").append(cumulative).append('\n');
        sb.append(name).append("_sum ").append(seconds(h.sumNanos())).append('\n');
        sb.append(name).append("_count ").append(h.count()).append('\n');
        // Not part of the histogram type: the exact worst case, which a bucket cannot give.
        sb.append("# HELP ").append(name).append("_max Worst observed value (exact)\n");
        sb.append("# TYPE ").append(name).append("_max gauge\n");
        sb.append(name).append("_max ").append(seconds(h.maxNanos())).append('\n');
    }

    private static void appendSecondsGauge(final StringBuilder sb, final String name, final String help,
                                           final long nanos) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append(' ').append(nanos < 0 ? "-1" : seconds(nanos)).append('\n');
    }

    private static void appendSeries(final StringBuilder sb, final String nameAndHelp, final String type,
                                     final long value) {
        final int space = nameAndHelp.indexOf(' ');
        final String name = nameAndHelp.substring(0, space);
        final String help = nameAndHelp.substring(space + 1);
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static String seconds(final long nanos) {
        return Double.toString(nanos / 1e9);
    }
}
