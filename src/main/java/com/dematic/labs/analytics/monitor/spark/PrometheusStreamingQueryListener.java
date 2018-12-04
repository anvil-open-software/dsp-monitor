/* Copyright 2018, Dematic, Corp.
   Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT */

package com.dematic.labs.analytics.monitor.spark;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.dematic.labs.analytics.monitor.spark.PrometheusConfig.SPARK_METRIC_PREFIX;

/**
 * Uses prometheus java client https://github.com/prometheus/client_java
 * to register structured streaming query stats to a push gateway.
 *
 * Example query result in json looks like this:

 [2017-09-01 17:00:31,048] INFO Streaming query made progress: {
 "id" : "fa8c066b-be6a-4188-a463-1a59d51fb4e6",
 "runId" : "e027b2ac-c489-42a6-8411-e97163bb04c4",
 "name" : "aggregate over time",
 "timestamp" : "2017-09-01T17:00:29.206Z",
 "numInputRows" : 74470,
 "inputRowsPerSecond" : 35906.46094503375,
 "processedRowsPerSecond" : 40450.841933731666,
 "durationMs" : {
 "addBatch" : 1801,
 "getBatch" : 14,
 "getOffset" : 2,
 "queryPlanning" : 9,
 "triggerExecution" : 1841,
 "walCommit" : 13
 },
 "eventTime" : {
 "avg" : "2017-09-01T17:00:28.174Z",
 "max" : "2017-09-01T17:00:29.203Z",
 "min" : "2017-09-01T17:00:18.572Z",
 "watermark" : "2017-09-01T16:00:27.128Z"
 },
 "stateOperators" : [ {
 "numRowsTotal" : 26500,
 "numRowsUpdated" : 100
 } ],
 "sources" : [ {
 "description" : "KafkaSource[Subscribe[ss_StructuredStreamingSignalAggregation]]",
 "startOffset" : {
 "ss_StructuredStreamingSignalAggregation" : {
 "8" : 282407978,
 "11" : 282407617,
 "2" : 282407506,
 "5" : 282407658,
 "14" : 282407718,
 "13" : 282407712,
 "4" : 282408067,
 "7" : 282407625,
 "1" : 282407640,
 "10" : 282407648,
 "9" : 282407860,
 "3" : 282407482,
 "12" : 282407399,
 "6" : 282407514,
 "0" : 282407697
 }
 },
 "endOffset" : {
 "ss_StructuredStreamingSignalAggregation" : {
 "8" : 282412835,
 "11" : 282412590,
 "2" : 282412464,
 "5" : 282412610,
 "14" : 282412589,
 "13" : 282412692,
 "4" : 282412873,
 "7" : 282412648,
 "1" : 282412587,
 "10" : 282412654,
 "9" : 282412784,
 "3" : 282412542,
 "12" : 282412479,
 "6" : 282412586,
 "0" : 282412658
 }
 },
 "numInputRows" : 74470,
 "inputRowsPerSecond" : 35906.46094503375,
 "processedRowsPerSecond" : 40450.841933731666
 } ],

 */

public class PrometheusStreamingQueryListener extends StreamingQueryListener {

    private PrometheusConfig promConfig;

    // collectors for spark streaming interactive query stats
    private Counter total_batches;
    private Counter total_input_rows;
    private Counter completed_non_empty_jobs;
    private Gauge num_input_rows_for_job;
    private Gauge processedRowsPerSecond;
    private Gauge inputRowsPerSecond;
    private Gauge query_total_duration_ms;
    private Gauge query_duration_get_batch_ms;
    private Gauge query_duration_get_offset_ms;

    private boolean addSparkQueryStats = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusStreamingQueryListener.class);

    public PrometheusStreamingQueryListener(SparkConf conf, String spark_app_name) {
        // SparkConf holds spark variables and not system. keep around so I don't have to change Michael's code again.
        promConfig= new PrometheusConfig(spark_app_name);

        total_batches = Counter.build().name(SPARK_METRIC_PREFIX + "batches_total")
                    .help("Total number of batches.").register();

        completed_non_empty_jobs = Counter.build().name(SPARK_METRIC_PREFIX + "completed_non_empty_jobs")
                .help("Total number of non-zero jobs.").register();

        total_input_rows = Counter.build().name(SPARK_METRIC_PREFIX + "input_rows_total")
                .help("Total number of input events.").register();

        num_input_rows_for_job = Gauge.build().name(SPARK_METRIC_PREFIX + "num_input_rows_for_job")
                .help("Input rows fetched per job").register();
        inputRowsPerSecond = Gauge.build().name(SPARK_METRIC_PREFIX + "input_rows_per_second")
                .help("Input rows fetched per second").register();
        processedRowsPerSecond = Gauge.build().name(SPARK_METRIC_PREFIX + "processed_rows_per_second")
                .help("Rows processed per second").register();

        query_total_duration_ms = Gauge.build().name(SPARK_METRIC_PREFIX + "query_duration_ms")
                .help("Total duration.").register();

        query_duration_get_batch_ms = Gauge.build().name(SPARK_METRIC_PREFIX + "query_duration_get_batch_ms")
                .help("Duration to get batch in ms.").register();

        query_duration_get_offset_ms = Gauge.build().name(SPARK_METRIC_PREFIX + "query_duration_get_offset_ms")
                .help("Duration to get offset in ms.").register();
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {

    }

    public void setAddSparkQueryStats(boolean addSparkQueryStats) {
        this.addSparkQueryStats = addSparkQueryStats;
    }



    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        PushGateway pg = new PushGateway(promConfig.getPushGatewayHost());
        try {
            total_batches.inc();
            if (addSparkQueryStats) {
                LOGGER.info("Preparing spark query metrics for prometheus with batch " + event.progress().batchId()) ;
                total_input_rows.inc(event.progress().numInputRows());
                num_input_rows_for_job.set(event.progress().numInputRows());

                // rates
                processedRowsPerSecond.set(event.progress().processedRowsPerSecond());
                inputRowsPerSecond.set(event.progress().inputRowsPerSecond());

                if (event.progress().numInputRows()>0) {
                    completed_non_empty_jobs.inc();
                }

                // see https://stackoverflow.com/questions/45642904/spark-structured-streaming-multiple-sinks
                //triggerExecution= getOffset + getBatch + addBatch
                Map<String,Long> durationByStage= event.progress().durationMs();
                if (durationByStage != null) {
                    if (durationByStage.containsKey(MonitorConsts.SPARK_DURATION_KEY_TRIGGER_EXECUTION)) {
                        query_total_duration_ms.set(durationByStage.get(MonitorConsts.SPARK_DURATION_KEY_TRIGGER_EXECUTION));
                    }
                    if (durationByStage.containsKey(MonitorConsts.SPARK_DURATION_KEY_GET_BATCH)) {
                        query_duration_get_batch_ms.set(durationByStage.get(MonitorConsts.SPARK_DURATION_KEY_GET_BATCH));
                    }

                    if (durationByStage.containsKey(MonitorConsts.SPARK_DURATION_KEY_GET_OFFSET)) {
                        query_duration_get_offset_ms.set(durationByStage.get(MonitorConsts.SPARK_DURATION_KEY_GET_OFFSET));
                    }

                }
            }

            pg.pushAdd(promConfig.getCollectorRegistry(), PrometheusConfig.JOB_NAME, promConfig.getGroupingKey());
        } catch (IOException e) {
            LOGGER.error("Error writing prometheus spark query metrics to " + promConfig + " with error  \n" + e.getMessage());
        }

    }


    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {

    }


}
