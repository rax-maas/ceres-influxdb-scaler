package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatefulSetStatus;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import com.rackspacecloud.metrics.influxdbscaler.models.stats.InfluxDBInstanceStatsSummary;
import com.rackspacecloud.metrics.influxdbscaler.providers.InfluxDBInstancesUpdater;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import com.rackspacecloud.metrics.influxdbscaler.repositories.DatabasesSeriesCountRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.MaxMinInstancesRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.RoutingInformationRepository;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private InfluxDB influxDB;
    private String localMetricsDatabase;
    private String localMetricsRetPolicy;

    /**
     * instancesStats collects all of the stats for given InfluxDB instance using "show stats" api
     */
    private Map<String, List<InfluxDBMetricsCollection.InfluxDBMetrics>> instancesStats;

    List<InfluxDBInstance> influxDBInstances;

    private InfluxDBHelper influxDBHelper;
    private StatefulSetProvider statefulSetProvider;
//    private RoutingInformationRepository routingInformationRepository;
//    private MaxMinInstancesRepository maxMinInstancesRepository;
//    private DatabasesSeriesCountRepository databasesSeriesCountRepository;
    private InfluxDBInstancesUpdater updater;

    public MetricsCollector(
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            InfluxDBInstancesUpdater updater,
            String localMetricsUrl,
            String localMetricsDatabase,
            String localMetricsRetPolicy) {

        this.influxDBHelper = influxDBHelper;
        this.statefulSetProvider = statefulSetProvider;
        this.updater = updater;
        this.localMetricsDatabase = localMetricsDatabase;
        this.localMetricsRetPolicy = localMetricsRetPolicy;

        influxDBInstances = this.updater.update(statefulSetProvider);

        instancesStats = new HashMap<>();
        this.influxDB = InfluxDBFactory.connect(localMetricsUrl);
        this.influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
        this.influxDB.enableBatch(BatchOptions.DEFAULTS);

        influxDBInstances.forEach(item -> {
            String url = item.getUrl();
            instancesStats.put(url, new ArrayList<>());
        });
    }

    /**
     * This is the entry method that collects all of the metrics for all of the InfluxDB influxDBInstances in the system.
     * @throws Exception
     */
    @Scheduled(cron = "${cron-config}") // Run every 60 seconds
    public void collectInfluxDBMetrics() throws Exception {
        LOGGER.info("> start");
        LOGGER.info("Current time {}", Instant.now());

        Map<String, StatsResults.SeriesMetric[]> seriesMetricCollection =
                influxDBHelper.getSeriesMetricCollection(instancesStats.keySet());

        List<String> lineProtocoledCollection = new ArrayList<>();
        for(String instance : seriesMetricCollection.keySet()) {
            StatsResults.SeriesMetric[] seriesMetrics = seriesMetricCollection.get(instance);
            List<InfluxDBMetricsCollection.InfluxDBMetrics> metricsList = getTotalSeriesCount(seriesMetrics);
            instancesStats.put(instance, metricsList);

//            if(metric.getName().equalsIgnoreCase("database")) {
//                long seriesCount = metric.getFields().get("numSeries");
//                String databaseName = metric.getTags().get("database");
//                databaseSeriesCountMap.put(databaseName, seriesCount);
//                totalSeriesCount += seriesCount;
//            }

            metricsList.forEach(item -> {
                String lineProtocoledString = item.toLineProtocol(instance);
                lineProtocoledCollection.add(lineProtocoledString);
//                LOGGER.info(lineProtocoledString);
            });
        }

        String metricsToPublish = String.join("\n", lineProtocoledCollection);

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

    public synchronized String getMinSeriesCountInstance() throws Exception {
        List<String> urls = new ArrayList<>();
        influxDBInstances.forEach( item -> urls.add(item.getUrl()));

        return influxDBHelper.getMinInstance(urls);
    }
}
