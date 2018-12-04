/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Instantiated by Spark adding to a metrics property file.
 */
public class PrometheusMetricsSink implements Sink {
    private static final long SINK_DEFAULT_PERIOD = 10;
    private static final TimeUnit SINK_DEFAULT_UNIT = TimeUnit.SECONDS;
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsSink.class);

    PrometheusMetricsReporter reporter;

    private long pollPeriod;
    private TimeUnit pollUnit;


    public PrometheusMetricsSink(Properties property,
                                 MetricRegistry registry,
                                 SecurityManager securityMgr) {
        // todo parameterize if this thing works
        pollPeriod = SINK_DEFAULT_PERIOD;
        pollUnit= SINK_DEFAULT_UNIT;

        LOGGER.info("PrometheusMetricsSink creating with:" + property);
        LOGGER.info(registry.toString());

        String app_name= System.getProperty(MonitorConsts.SPARK_DRIVER_KEY, "undefined");

        reporter = new PrometheusMetricsReporter(registry,app_name,null,
                TimeUnit.MILLISECONDS,
                TimeUnit.SECONDS);
    }

    @Override
    public void start() {
        //long period, TimeUnit unit)
        LOGGER.info("Starting PrometheusMetricsReporter at intervals of " + pollPeriod + " " + pollUnit);
        reporter.start(pollPeriod, pollUnit);
    }

    @Override
    public void stop() {
        reporter.stop();
    }

    @Override
    public void report() {
        reporter.report();
    }
}
