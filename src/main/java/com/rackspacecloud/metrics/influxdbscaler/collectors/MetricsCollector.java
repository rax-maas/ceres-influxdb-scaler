package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import com.rackspacecloud.metrics.influxdbscaler.providers.InfluxDBInstancesUpdater;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class collects all of the metrics from the InfluxDB instances
 */
public class MetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    /**
     * 'influxDB' is responsible for writing InfluxDB stateful instances metrics data into
     * the local metrics database.
     * 'localMetricsDatabase' database and 'localMetricsRetPolicy' retention policy
     * contain all of the metrics data from InfluxDB stateful instances.
     */
    private InfluxDBFactory influxDBFactory;
    private String localMetricsUrl;
    private String localMetricsDatabase;
    private String localMetricsRetPolicy;

    /**
     * instancesStats collects all of the stats for given InfluxDB instance using "show stats" api
     */
    private Map<String, List<InfluxDBMetricsCollection.InfluxDBMetrics>> instancesStats;

    List<InfluxDBInstance> influxDBInstances;

    private InfluxDBHelper influxDBHelper;
    private InfluxDBInstancesUpdater updater;

    public MetricsCollector(
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            InfluxDBInstancesUpdater updater,
            InfluxDBFactory influxDBFactory,
            String localMetricsUrl,
            String localMetricsDatabase,
            String localMetricsRetPolicy) {

        this.influxDBHelper = influxDBHelper;
        this.updater = updater;
        this.localMetricsUrl = localMetricsUrl;
        this.localMetricsDatabase = localMetricsDatabase;
        this.localMetricsRetPolicy = localMetricsRetPolicy;
        this.influxDBFactory = influxDBFactory;

        influxDBInstances = this.updater.update(statefulSetProvider);

        instancesStats = new HashMap<>();

        influxDBInstances.forEach(item -> {
            String url = item.getUrl();
            instancesStats.put(url, new ArrayList<>());
        });
    }

    public Set<String> getInfluxDBInstanceUrls() {
        return instancesStats.keySet();
    }

    /**
     * This is the entry method that collects all of the metrics for all of the InfluxDB influxDBInstances in the system.
     * @throws Exception
     */
    @Scheduled(cron = "${cron-config}") // Run every 60 seconds
    public void collectInfluxDBMetrics() throws Exception {
        LOGGER.info("> start");
        LOGGER.info("Current time {}", Instant.now());

        // seriesMetricCollection collects all of the stats for given InfluxDB instances using "show stats" api
        Map<String, StatsResults.SeriesMetric[]> seriesMetricCollection =
                influxDBHelper.getSeriesMetricCollection(instancesStats.keySet());

        List<String> lineProtocoledCollection = new ArrayList<>();
        for(String instance : seriesMetricCollection.keySet()) {
            StatsResults.SeriesMetric[] seriesMetrics = seriesMetricCollection.get(instance);
            List<InfluxDBMetricsCollection.InfluxDBMetrics> metricsList = getTotalSeriesCount(seriesMetrics);
            instancesStats.put(instance, metricsList);

            metricsList.forEach(item -> {
                String lineProtocoledString = item.toLineProtocol(instance);
                lineProtocoledCollection.add(lineProtocoledString);
            });
        }

        String metricsToPublish = String.join("\n", lineProtocoledCollection);

        InfluxDB influxDB = influxDBFactory.getInfluxDB(localMetricsUrl);

        influxDB.write(localMetricsDatabase, localMetricsRetPolicy,
                InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS, metricsToPublish);

        LOGGER.info("< end");
    }

    private List<InfluxDBMetricsCollection.InfluxDBMetrics> getTotalSeriesCount(
            StatsResults.SeriesMetric[] seriesMetricCollection) throws Exception {

        List<InfluxDBMetricsCollection.InfluxDBMetrics> metricsList = new ArrayList<>();

        for(int i = 0; i < seriesMetricCollection.length; i++) {
            InfluxDBMetricsCollection.InfluxDBMetrics metric = new InfluxDBMetricsCollection.InfluxDBMetrics();

            metric.setName(seriesMetricCollection[i].getName());
            metric.setTags(seriesMetricCollection[i].getTags());

            String[] columns = seriesMetricCollection[i].getColumns();
            Long[][] values = seriesMetricCollection[i].getValues();

            if(values.length != 1) throw new Exception("I expect only one array of long values");

            Map<String, Long> fields = metric.getFields();

            for(int j = 0; j < columns.length; j++) {
                fields.put(columns[j], values[0][j]);
            }

            metricsList.add(metric);
        }

        return metricsList;
    }

    /**
     * This method returns InfluxDBInstance with minimum series count.
     * @return
     * @throws Exception
     */
    public synchronized String getMinSeriesCountInstance() throws Exception {
        return influxDBHelper.getMinInstance(getInfluxDBInstanceUrls());
    }
}
