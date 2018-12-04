/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.dematic.labs.analytics.monitor.spark.dropwizard.DropwizardSparkExports;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;


/**
 * Prometheus adapter- this gets the standard JMX reporter not what is in the Metric Registry
 */
public class PrometheusMetricsReporter extends ScheduledReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsReporter.class);
    private PrometheusConfig promConfig;
    private final MetricRegistry registry;
    private DropwizardSparkExports dropwizardExports;

    public PrometheusMetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.registry = registry;
        dropwizardExports = new DropwizardSparkExports(registry);

        initializePrometheus(name);
    }

    /**
     * We have to find the executor id and unfortunately the only place we have it available is in the metric name such
     * as application_1495476089339_0003.5.jvm.heap.committed but dots will get scrubbed as underscore
     * <p>
     * the Spark conf is just private within the MetricsSystem object org.apache.spark.metrics.MetricsSystem:
     */
    private void initializePrometheus(String appName) {
        promConfig = new PrometheusConfig(appName);
        registry.addListener(new MetricRegistryListener.Base() {
            @Override
            public void onGaugeAdded(String name, Gauge<?> gauge) {
                if (!promConfig.getGroupingKey().containsKey(PrometheusConfig.LABEL_EXECUTOR)) {
                    if (name.startsWith("application")) {
                        String[] names = name.split("\\.");
                        String executor = names[1];
                        promConfig.getGroupingKey().put(PrometheusConfig.LABEL_EXECUTOR, executor);
                    }
                }
            }
        });
    }


    protected PrometheusConfig getPrometheusConfig() {
        return promConfig;
    }


    /**
     * standard jvm makes graphing easier
     */
    private void pushStandardMetricsToPrometheusGateway() {
        PushGateway pg = new PushGateway(promConfig.getPushGatewayHost());
        try {
            pg.pushAdd(promConfig.getCollectorRegistry(), PrometheusConfig.JOB_NAME, promConfig.getGroupingKey());
        } catch (Exception e) {
            LOGGER.error("Error writing metrics to " + promConfig + " with error  \n" + e.getMessage());
        }
    }

    @Override
    public void report() {
        pushStandardMetricsToPrometheusGateway();

        PushGateway pg = new PushGateway(promConfig.getPushGatewayHost());
        try {
            CollectorRegistry sparkCollectorRegistry = new CollectorRegistry(false);
            LOGGER.info("Pushing dropwizard metrics to gateway " );
            // causes metric to be checked twice here.
            dropwizardExports.collect();
            sparkCollectorRegistry.register(dropwizardExports);
            pg.pushAdd(sparkCollectorRegistry, PrometheusConfig.JOB_NAME, promConfig.getGroupingKey());
        } catch (Exception e) {
            LOGGER.error("Error writing metrics to " + promConfig + " with error  \n" + e.getMessage());
        }
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
          throw new RuntimeException("ScheduledReporter should NOT call this method since we have overridden report()");
    }
}
