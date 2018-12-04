/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

/**
 * Keep track of all parameters here
 */
public interface MonitorConsts {

    String SPARK_CLUSTER_ID="dematiclabs.spark.cluster_id";
    String SPARK_DRIVER_KEY="dematiclabs.spark.driver.key";
    String SPARK_QUERY_MONITOR_PUSH_GATEWAY = "dematiclabs.monitor.pushGateway.address";
    String SPARK_QUERY_MONITOR_COLLECTOR_JOB_NAME = "dematiclabs.monitor.collector.job.name";

    // optional for segregating metric sets without time
    String SPARK_DRIVER_UNIQUE_RUN_ID= "dematiclabs.spark.driver.unique.run.id";

    //spark variables- these only exist inside the conf
    String SPARK_EXECUTOR_ID="spark.executor.id";
    String SPARK_APP_NAME= "spark.app.name";

    // query duration hash map key
    String SPARK_DURATION_KEY_TRIGGER_EXECUTION="triggerExecution";
    String SPARK_DURATION_KEY_GET_OFFSET="getOffset";
    String SPARK_DURATION_KEY_GET_BATCH="getBatch";
    String SPARK_DURATION_KEY_QUERY_PLANNING="queryPlanning";
}
