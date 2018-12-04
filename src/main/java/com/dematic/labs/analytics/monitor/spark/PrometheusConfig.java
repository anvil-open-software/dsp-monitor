/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;


/**
 * Stores configuration for prometheus push gateway integration
 */

public class PrometheusConfig {
    public static String LABEL_INSTANCE = "instance";
    public static String LABEL_DRIVER_NAME = "driver";
    public static String LABEL_CLUSTER_ID = "cluster_id";
    public static String LABEL_JOB_ID = "job_id";
    public static String JOB_NAME = "spark-push-gateway";
    public static String SPARK_METRIC_PREFIX = "spark_structured_streaming_";
    public static String LABEL_EXECUTOR = "executor";

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusConfig.class);
    private String push_gateway_host;
    private String appName;

    private Map<String, String> groupingKey = new HashMap<String, String>();
    private CollectorRegistry collectorRegistry;

    /**
     * @param app_name
     */
    public PrometheusConfig(String app_name) {

        this.push_gateway_host = System.getProperty(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY);
        this.appName = app_name;
        collectorRegistry = CollectorRegistry.defaultRegistry;
        collectorRegistry.clear();

        groupingKey.put(PrometheusConfig.LABEL_DRIVER_NAME, app_name);

        // graceful default so callers can set parameters later
        groupingKey.put(PrometheusConfig.LABEL_CLUSTER_ID, System.getProperty(MonitorConsts.SPARK_CLUSTER_ID, "undefined"));

        groupingKey.put(PrometheusConfig.LABEL_INSTANCE, getLocalAddress().getHostAddress());

        // graceful default so callers can set parameters later
        if (System.getProperty(MonitorConsts.SPARK_DRIVER_UNIQUE_RUN_ID)!=null) {
            groupingKey.put(PrometheusConfig.LABEL_JOB_ID, System.getProperty(MonitorConsts.SPARK_DRIVER_UNIQUE_RUN_ID));
        }

        // NOTE MonitorConsts.SPARK_EXECUTOR_ID is only available inside SparkConf which is not available here...
        // add all standard jvm collectors - memory, gc, machine, etc..
        DefaultExports.initialize();
        LOGGER.info("Prometheus Metrics initialized with " + this.toString());
    }

    /**
     * @return the internal 10.x.x.x address
     * Taken from https://stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java
     */
    private static InetAddress getLocalAddress() {
        try {
            Enumeration<NetworkInterface> b = NetworkInterface.getNetworkInterfaces();
            while (b.hasMoreElements()) {
                for (InterfaceAddress f : b.nextElement().getInterfaceAddresses())
                    if (f.getAddress().isSiteLocalAddress())
                        return f.getAddress();
            }
        } catch (SocketException e) {
            LOGGER.error("Metric will not have instance label assigned as we can't get internal instance address \n" + e);
        }
        return null;
    }

    public String getPushGatewayHost() {
        return push_gateway_host;
    }

    public CollectorRegistry getCollectorRegistry() {
        return collectorRegistry;
    }

    public Map<String, String> getGroupingKey() {
        return groupingKey;
    }

    public String getMetricsURLBase() {
        return "http://" + push_gateway_host + "/metrics/job/";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrometheusConfig that = (PrometheusConfig) o;

        if (!push_gateway_host.equals(that.push_gateway_host)) return false;
        if (!appName.equals(that.appName)) return false;
        if (!groupingKey.equals(that.groupingKey)) return false;
        return collectorRegistry.equals(that.collectorRegistry);

    }

    @Override
    public int hashCode() {
        int result = push_gateway_host.hashCode();
        result = 31 * result + appName.hashCode();
        result = 31 * result + groupingKey.hashCode();
        result = 31 * result + collectorRegistry.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PrometheusConfig{" +
                "push_gateway_host='" + push_gateway_host + '\'' +
                ", appName='" + appName + '\'' +
                ", url=" + getMetricsURLBase() +
                ", grouping_key=" + groupingKey +
                ", collectorRegistry=" + collectorRegistry + '}';
    }
}
