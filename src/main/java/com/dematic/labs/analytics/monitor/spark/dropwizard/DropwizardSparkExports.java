/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark.dropwizard;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.prometheus.client.dropwizard.DropwizardExports;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * had to clone and modify from io.prometheus.client.dropwizard.DropwizardExports as there was no way to
 * override static method sanitizeName
 */
public class DropwizardSparkExports extends io.prometheus.client.Collector implements io.prometheus.client.Collector.Describable{

    private MetricRegistry registry;
    private static final Logger LOGGER = Logger.getLogger(DropwizardExports.class.getName());

    /**
     * Only reason this class had to be cloned as this method was declared static
     * Replace all unsupported chars with '_'.
     *
     * @param dropwizardName original metric name.
     * @return the sanitized metric name.
     */
    public static String sanitizeMetricName(String dropwizardName){
        String shortenedName=dropwizardName;
        // remove prefix of app name and executor id
        if (dropwizardName.startsWith("application")) {
            String[] names=dropwizardName.split("\\.");
            shortenedName=dropwizardName.substring(names[0].length()+names[1].length()+2);
        }
        return shortenedName.replaceAll("[^a-zA-Z0-9:_]", "_").toLowerCase();
    }


    /**
     * @param registry a metric registry to export in prometheus.
     */
    public DropwizardSparkExports(MetricRegistry registry) {
        this.registry = registry;
    }

    /**
     * Export counter as Prometheus <a href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
     */
    List<MetricFamilySamples> fromCounter(String dropwizardName, Counter counter) {
        String name = sanitizeMetricName(dropwizardName);
        MetricFamilySamples.Sample sample = new MetricFamilySamples.Sample(name, new ArrayList<String>(), new ArrayList<String>(),
                new Long(counter.getCount()).doubleValue());
        return Arrays.asList(new MetricFamilySamples(name, Type.GAUGE, getHelpMessage(dropwizardName, counter), Arrays.asList(sample)));
    }

    /**
     * The original io.prometheus.client.dropwizard.DropwizardExports metricName causes GBs of warnings
     * since the help names are different for the same metric.
     *
     * @param metricName
     * @param metric
     * @return No good way to get the help message beyond name so we return null.
     */
    private static String getHelpMessage(String metricName, Metric metric){
        return "";
    }

    /**
     * Export gauge as a prometheus gauge.
     */
    List<MetricFamilySamples> fromGauge(String dropwizardName, Gauge gauge) {
        String name = sanitizeMetricName(dropwizardName);
        Object obj = gauge.getValue();
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
            LOGGER.log(Level.FINE, String.format("Invalid type for Gauge %s: %s", name,
                    obj.getClass().getName()));
            return new ArrayList<MetricFamilySamples>();
        }
        MetricFamilySamples.Sample sample = new MetricFamilySamples.Sample(name,
                new ArrayList<String>(), new ArrayList<String>(), value);
        return Arrays.asList(new MetricFamilySamples(name, Type.GAUGE, getHelpMessage(dropwizardName, gauge), Arrays.asList(sample)));
    }

    /**
     * Export a histogram snapshot as a prometheus SUMMARY.
     *
     * @param dropwizardName metric name.
     * @param snapshot the histogram snapshot.
     * @param count the total sample count for this snapshot.
     * @param factor a factor to apply to histogram values.
     *
     */
    List<MetricFamilySamples> fromSnapshotAndCount(String dropwizardName, Snapshot snapshot, long count, double factor, String helpMessage) {
        String name = sanitizeMetricName(dropwizardName);
        List<MetricFamilySamples.Sample> samples = Arrays.asList(
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.5"), snapshot.getMedian() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.75"), snapshot.get75thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.95"), snapshot.get95thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.98"), snapshot.get98thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.99"), snapshot.get99thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.999"), snapshot.get999thPercentile() * factor),
                new MetricFamilySamples.Sample(name + "_count", new ArrayList<String>(), new ArrayList<String>(), count)
        );
        return Arrays.asList(
                new MetricFamilySamples(name, Type.SUMMARY, helpMessage, samples)
        );
    }

    /**
     * Convert histogram snapshot.
     */
    List<MetricFamilySamples> fromHistogram(String dropwizardName, Histogram histogram) {
        return fromSnapshotAndCount(dropwizardName, histogram.getSnapshot(), histogram.getCount(), 1.0,
                getHelpMessage(dropwizardName, histogram));
    }

    /**
     * Export Dropwizard Timer as a histogram. Use TIME_UNIT as time unit.
     */
    List<MetricFamilySamples> fromTimer(String dropwizardName, Timer timer) {
        return fromSnapshotAndCount(dropwizardName, timer.getSnapshot(), timer.getCount(),
                1.0D / TimeUnit.SECONDS.toNanos(1L), getHelpMessage(dropwizardName, timer));
    }

    /**
     * Export a Meter as as prometheus COUNTER.
     */
    List<MetricFamilySamples> fromMeter(String dropwizardName, Meter meter) {
        String name = sanitizeMetricName(dropwizardName);
        return Arrays.asList(
                new MetricFamilySamples(name + "_total", Type.COUNTER, getHelpMessage(dropwizardName, meter),
                        Arrays.asList(new MetricFamilySamples.Sample(name + "_total",
                                new ArrayList<String>(),
                                new ArrayList<String>(),
                                meter.getCount())))

        );
    }



    @Override
    public List<MetricFamilySamples> collect() {
        ArrayList<MetricFamilySamples> mfSamples = new ArrayList<MetricFamilySamples>();
        for (SortedMap.Entry<String, Gauge> entry : registry.getGauges().entrySet()) {
            mfSamples.addAll(fromGauge(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Counter> entry : registry.getCounters().entrySet()) {
            mfSamples.addAll(fromCounter(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Histogram> entry : registry.getHistograms().entrySet()) {
            mfSamples.addAll(fromHistogram(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Timer> entry : registry.getTimers().entrySet()) {
            mfSamples.addAll(fromTimer(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Meter> entry : registry.getMeters().entrySet()) {
            mfSamples.addAll(fromMeter(entry.getKey(), entry.getValue()));
        }
        return mfSamples;
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return new ArrayList<MetricFamilySamples>();
    }
}
