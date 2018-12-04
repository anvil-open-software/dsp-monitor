/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.util.Utils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 *  Meant to be run manually for now since I don't have a wasy way of bringing up a push gateway on local
 *  and we should not put it into our actual monitor.
 *
 * Provide valid push gateway host if you want this to run, i.e.
 *   -Ddematiclabs.monitor.pushGateway.address=10.x.x.x:9091
 */
public final class PrometheusSparkListenerIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusSparkListenerIntegrationTest.class);
    private String getPrometheusHost() {
        return System.getProperty(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY);
    }


    @Test
    public void instantiatePrometheusMetricsSink() {
        try {

            System.setProperty(MonitorConsts.SPARK_CLUSTER_ID, "sink-test");
            SparkConf conf = new SparkConf();
            org.apache.spark.SecurityManager securityManager= new SecurityManager(conf,null);

            MetricRegistry registry= new MetricRegistry();
            Properties properties = new Properties();
            // this way of instantiating from Spark code was failing in the driver so I put it here
            Object sink = Utils.classForName(PrometheusMetricsSink.class.getName())
                    .getConstructor(Properties.class, MetricRegistry.class, SecurityManager.class)
                    .newInstance(properties, registry, securityManager);

            if (getPrometheusHost() != null) {

                PrometheusMetricsSink prometheusMetricsSink = (PrometheusMetricsSink) sink;
                String jvm_heap_metric="application_1495474513333_0001.5.jvm.heap.committed";

                Gauge<Integer> gauge = () -> 5423543;

                registry.register(jvm_heap_metric,gauge);
                prometheusMetricsSink.report();
                Assert.assertEquals("5", prometheusMetricsSink.reporter.getPrometheusConfig()
                        .getGroupingKey().get(PrometheusConfig.LABEL_EXECUTOR));

            }

        } catch (Exception e) {
            fail("Instantiation failed, make sure you are using org.apache.spark.SecurityManager "+e);
        }
    }


    /**
     * For making sure the timer does run by default 10 seconds
     */
    @Test
    @Ignore
    public void testTimerForPrometheusMetricsSink() {

            System.setProperty(MonitorConsts.SPARK_CLUSTER_ID, "reporter-timer-test");
            SparkConf conf = new SparkConf();
            org.apache.spark.SecurityManager securityManager= new SecurityManager(conf,null);
            if (getPrometheusHost() != null) {
                MetricRegistry registry= new MetricRegistry();

                Properties properties = new Properties();
                PrometheusMetricsSink prometheusMetricsSink = new PrometheusMetricsSink(properties,
                        registry, securityManager);
                String jvm_heap_metric="testTimerForPrometheusMetricsSink.timer";

                Gauge<Integer> gauge = new Gauge<Integer>() {
                    int runCount=0;
                    @Override
                    public Integer getValue() {
                        return runCount++;
                    }
                };
                int sleepMillis=35000;
                registry.register(jvm_heap_metric,gauge);
                prometheusMetricsSink.start();
                // should report no more than 3 times
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Assert.assertEquals(Integer.valueOf(2*Math.floorDiv(sleepMillis,10000)), gauge.getValue());
            }
    }
    @Test
    public void pushExecutorTestStreamingStats() {
        if (getPrometheusHost() != null) {
            MetricRegistry registry = new MetricRegistry();
            System.setProperty(MonitorConsts.SPARK_CLUSTER_ID, "async-test");
            PrometheusMetricsReporter reporter = new PrometheusMetricsReporter(registry,"pushExecutorTestStreamingStats",
                    null,
                    TimeUnit.MILLISECONDS,
                    TimeUnit.SECONDS );

            reporter.report();
        } else {
            LOGGER.warn("Set " + MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY + "if you want this to run");
        }
    }

    @Test
    public void pushAsyncQueryListenerStats() {
        if (getPrometheusHost() != null) {
            // just test out machinery
            SparkConf conf = new SparkConf();
            System.setProperty(MonitorConsts.SPARK_CLUSTER_ID, "local-test");
            PrometheusStreamingQueryListener queryListener =
                    new PrometheusStreamingQueryListener(conf,"pushAsyncQueryListenerStats");
            // need either to write a structured streaming query to run or create mock data
            // was not trivial to create a mock results due to complex structures needed for progress stats
            queryListener.setAddSparkQueryStats(false);
            for (int i = 0; i < 6; i++) {
                queryListener.onQueryProgress(null);
            }
        } else {
            LOGGER.warn("Set " + MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY + "if you want this to run");
        }
    }

}