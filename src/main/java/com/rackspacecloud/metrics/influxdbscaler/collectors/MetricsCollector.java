package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import com.rackspacecloud.metrics.influxdbscaler.providers.InfluxDBInstancesUpdater;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class collects all of the metrics from the InfluxDB instances
 */
@Slf4j
public class MetricsCollector {

    private MeterRegistry registry;
    private Timer influxdbStatsCollectionTimer;
    private Timer minSeriesCountInstanceTimer;
    private String localMetricsDatabase;
    private String localMetricsRetPolicy;
    private InfluxDB influxDBCeresWriter;

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
            MeterRegistry registry,
            String localMetricsUrl,
            String localMetricsDatabase,
            String localMetricsRetPolicy) {

        this.influxDBHelper = influxDBHelper;
        this.updater = updater;
        this.localMetricsDatabase = localMetricsDatabase;
        this.localMetricsRetPolicy = localMetricsRetPolicy;
        this.registry = registry;
        this.influxdbStatsCollectionTimer = this.registry.timer("influxdb.stats.collection.time");
        this.minSeriesCountInstanceTimer = this.registry.timer("influxdb.min.series.count.instance");

        influxDBInstances = this.updater.update(statefulSetProvider);

        setInfluxDBCeresWriter(influxDBFactory.getInfluxDB(localMetricsUrl));

        instancesStats = new HashMap<>();

        influxDBInstances.forEach(item -> {
            String url = item.getUrl();
            instancesStats.put(url, new ArrayList<>());
        });
    }

    public void setInfluxDBCeresWriter(InfluxDB influxDB) {
        this.influxDBCeresWriter = influxDB;
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
        log.debug("> start");
        log.debug("Current time {}", Instant.now());

        long statsCollectionStart = System.currentTimeMillis();
        // seriesMetricCollection collects all of the stats for given InfluxDB instances using "show stats" api
        Map<String, StatsResults.SeriesMetric[]> seriesMetricCollection =
                influxDBHelper.getSeriesMetricCollection(instancesStats.keySet());

        influxdbStatsCollectionTimer.record(System.currentTimeMillis() - statsCollectionStart, TimeUnit.MILLISECONDS);

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

        influxDBCeresWriter.write(localMetricsDatabase, localMetricsRetPolicy,
                InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS, metricsToPublish);

        log.debug("< end");
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
        long minSearchStart = System.currentTimeMillis();

        String minInstance = influxDBHelper.getMinInstance(getInfluxDBInstanceUrls());

        minSeriesCountInstanceTimer.record(System.currentTimeMillis() - minSearchStart, TimeUnit.MILLISECONDS);

        return minInstance;
    }
}
