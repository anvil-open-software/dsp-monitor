Copyright 2018, Dematic, Corp.

## Functionality

Custom metrics integration for Spark executors and Spark asynchronous query listener for structured streaming.

Spark version support: 2.x, last tested with 2.3

### Usage for JMX Listener on Driver and Executors

Add to Spark metrics configuration:

```
# enable Prometheus sink which writes to a push gateway
*.sink.prometheus.class=com.dematic.labs.analytics.monitor.spark.PrometheusMetricsSink
```

See our example Spark metrics config:
http://gitlab.ops.cld/Dematiclabs/devops/blob/gcp-automation/ansible/roles/analytics/roles/spark/files/conf/prometheus-metrics.properties

Pass in spark driver launch parameters:

```-Ddematiclabs.monitor.pushGateway.address=10.x.x.x:9091
  -Ddematiclabs.spark.cluster_id=$YARN_CLUSTER_ID
  -Ddematiclabs.spark.driver.key=$SPARK_DRIVER_KEY
```

You can test to see if the metrics were pushed by going directly to your monitor, i.e. http://10.x.x.x:9091
### Usage for spark streaming asynchronous query listener

Add dependency to project maven pom.xml:
```
 <dependency>
            <groupId>com.dematic.labs.dsp.monitor</groupId>
            <artifactId>dsp-monitor</artifactId>
            <version>0.0.2-SNAPSHOT</version>

            <exclusions>
                <exclusion>
                    <groupId>io.prometheus</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>io.dropwizard.metrics</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

Add CassandraStreamingQueryListener to driver:

```
 // add query statistic listener to enable monitoring of queries
    if (sys.props.contains(DriverConsts.SPARK_QUERY_STATISTICS)) {
      spark.streams.addListener(new CassandraStreamingQueryListener(APP_NAME, cassandraKeyspace,
        spark.sparkContext.getConf))
    }

```

See example in com.dematic.labs.analytics.diagnostics.spark.drivers.StructuredStreamingSignalAggregation