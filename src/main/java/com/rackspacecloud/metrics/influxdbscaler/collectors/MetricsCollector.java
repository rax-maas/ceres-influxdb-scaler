package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatefulSetStatus;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.DatabasesSeriesCount;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.MaxAndMinSeriesInstances;
import com.rackspacecloud.metrics.influxdbscaler.models.stats.InfluxDBInstanceStatsSummary;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import com.rackspacecloud.metrics.influxdbscaler.repositories.DatabasesSeriesCountRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.MaxMinInstancesRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.RoutingInformationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.*;

public class MetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private InfluxDBInstanceStatsSummary influxDBInstanceStatsSummary;

    /**
     * instancesStats collects all of the stats for given InfluxDB instance using "show stats" api
     */
    private Map<String, List<InfluxDBMetricsCollection.InfluxDBMetrics>> instancesStats;

    private boolean scalingInProgress = false;
    private String namespace;
    private String statefulSetName;
    private String headlessServiceName;
    private int totalSeriesCountIterations;

    private static final long SERIES_GROWTH_THRESHOLD = 1;

    private InfluxDBHelper influxDBHelper;
    private StatefulSetProvider statefulSetProvider;
    private RoutingInformationRepository routingInformationRepository;
    private MaxMinInstancesRepository maxMinInstancesRepository;
    private DatabasesSeriesCountRepository databasesSeriesCountRepository;

    public MetricsCollector(
            String namespace,
            String statefulSetName,
            String headlessServiceName,
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            RoutingInformationRepository routingInformationRepository,
            MaxMinInstancesRepository maxMinInstancesRepository,
            DatabasesSeriesCountRepository databasesSeriesCountRepository,
            InfluxDBInstanceStatsSummary influxDBInstanceStatsSummary,
            int totalSeriesCountIterations,
            boolean isLocal) {
        this.namespace = namespace;
        this.statefulSetName = statefulSetName;
        this.headlessServiceName = headlessServiceName;
        this.influxDBHelper = influxDBHelper;
        this.statefulSetProvider = statefulSetProvider;
        this.routingInformationRepository = routingInformationRepository;
        this.maxMinInstancesRepository = maxMinInstancesRepository;
        this.databasesSeriesCountRepository = databasesSeriesCountRepository;
        this.influxDBInstanceStatsSummary = influxDBInstanceStatsSummary;
        this.totalSeriesCountIterations = totalSeriesCountIterations;

        setInitialInfluxDBInstances(isLocal);

        initialize();
    }

    /**
     * Get all of the InfluxDB URLs from the InfluxDB StatefulSet and add it to Redis database
     */
    private void setInitialInfluxDBInstances(boolean isLocal) {
        List<InfluxDBInstance> instances = new ArrayList<>();

        if(isLocal) {
            populateInstances(instances);
        }
        else {
            populateWithLatestInstances(instances);
        }

        routingInformationRepository.saveAll(instances);
    }

    public void populateWithLatestInstances(List<InfluxDBInstance> instances) {
        // URL example for an instance of InfluxDB in statefulset: http://data-influxdb-0.influxdbsvc:8086
        // Get all of the URLs from StatefulSet
        StatefulSetStatus status = statefulSetProvider.getStatefulSetStatus(namespace, statefulSetName);

        for(int i = 0; i < status.getReadyReplicas(); i++) {
            String influxDBInstanceName = String.format("%s-%d", statefulSetName, i);

            String url = String.format("http://%s:8086", influxDBInstanceName);
            if(headlessServiceName != null && !headlessServiceName.isEmpty()) {
                url = String.format("http://%s.%s:8086", influxDBInstanceName, headlessServiceName);
            }
            instances.add(new InfluxDBInstance(influxDBInstanceName, url));

            LOGGER.info("Initializing with InfluxDB URL: [{}]", url);
        }
    }

    /**
     * This is for dev/test env
     * @param instances
     */
    private void populateInstances(List<InfluxDBInstance> instances) {
        InfluxDBInstance instance = new InfluxDBInstance();
        instance.setName("influxdb-0");
        instance.setUrl("http://localhost:8086");

        InfluxDBInstance instance1 = new InfluxDBInstance();
        instance1.setName("influxdb-1");
        instance1.setUrl("http://localhost:8087");

        instances.add(instance);
        instances.add(instance1);
    }

    private void initialize() {
        instancesStats = new HashMap<>();

        routingInformationRepository.findAll().forEach( item -> {
            String url = item.getUrl();
            instancesStats.put(url, new ArrayList<>());
            InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount =
                    new InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount(
                            new long[totalSeriesCountIterations], new HashMap<>()
                    );
            influxDBInstanceStatsSummary.getInstancesStatsMap().put(url, instanceDatabasesSeriesCount);
        });
    }

    /**
     * This is the entry method that collects all of the metrics for all of the InfluxDB instances in the system.
     * @throws Exception
     */
    @Scheduled(cron = "*/10 * * * * *") // Run every 10 seconds
    public void collectInfluxDBMetrics() throws Exception {
        LOGGER.info("> start");
        LOGGER.info("Current time {}", Instant.now());

        influxDBHelper.populateStats(instancesStats, influxDBInstanceStatsSummary);

        String maxUrl = influxDBInstanceStatsSummary.getInstanceUrlWithMaxSeriesCount();
        String minUrl = influxDBInstanceStatsSummary.getInstanceUrlWithMinSeriesCount();

        saveMaxAndMinSeriesInfluxDBInstances(maxUrl, minUrl);
        saveDatabasesSeriesCount();

        String maxInstance = influxDBInstanceStatsSummary.getInstanceUrlWithMaxSeriesCount();
        InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount =
                influxDBInstanceStatsSummary.getInstancesStatsMap().get(maxInstance);

        long maxSeriesCountGrowth = instanceDatabasesSeriesCount.getSeriesCountPercentageGrowth();

        if(!scalingInProgress && maxSeriesCountGrowth > SERIES_GROWTH_THRESHOLD) {
            LOGGER.warn("ALERT: Max series count growth is at [{}] past threshold of [{}]",
                    maxSeriesCountGrowth, SERIES_GROWTH_THRESHOLD);

            addLatestStatefulSetNodes();
        }

        LOGGER.info("< end");
    }

    public String getMinSeriesCountInstance() throws Exception {
        influxDBHelper.populateStats(instancesStats, influxDBInstanceStatsSummary);

        return influxDBInstanceStatsSummary.getInstanceUrlWithMinSeriesCount();
    }

    private void saveDatabasesSeriesCount() {
        Map<String, InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount> instancesStats =
                influxDBInstanceStatsSummary.getInstancesStatsMap();

        List<DatabasesSeriesCount> databasesSeriesCountRecords = new ArrayList<>();

        for(String url : instancesStats.keySet()) {
            InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount =
                    instancesStats.get(url);
            DatabasesSeriesCount databasesSeriesCount = new DatabasesSeriesCount(url,
                    instanceDatabasesSeriesCount.getSeriesCountPercentageGrowth(),
                    instanceDatabasesSeriesCount.getDatabaseSeriesCountMap());

            long latestTotalSeriesCount = instanceDatabasesSeriesCount.getLatestTotalSeriesCount();
            LOGGER.info("Total series count is [{}] with percentage growth [{}] in instance [{}]",
                    latestTotalSeriesCount, instanceDatabasesSeriesCount.getSeriesCountPercentageGrowth(), url);

            databasesSeriesCountRecords.add(databasesSeriesCount);
        }

        databasesSeriesCountRepository.saveAll(databasesSeriesCountRecords);
    }

    private void saveMaxAndMinSeriesInfluxDBInstances(String maxUrl, String minUrl) throws Exception {
        List<MaxAndMinSeriesInstances> maxAndMinSeriesInstances = new ArrayList<>();

        if(influxDBInstanceStatsSummary == null)
            throw new Exception("influxDBInstanceStatsSummary is null");

        if(influxDBInstanceStatsSummary.getInstancesStatsMap() == null)
            throw new Exception("influxDBInstanceStatsSummary.getInstancesStatsMap() is null");

        if(influxDBInstanceStatsSummary.getInstancesStatsMap().get(maxUrl) == null)
            throw new Exception("influxDBInstanceStatsSummary.getInstancesStatsMap().get(maxUrl) is null");

        if(influxDBInstanceStatsSummary.getInstancesStatsMap().get(minUrl) == null)
            throw new Exception("influxDBInstanceStatsSummary.getInstancesStatsMap().get(minUrl) is null");

        maxAndMinSeriesInstances.add(new MaxAndMinSeriesInstances("MAX",
                maxUrl,
                influxDBInstanceStatsSummary.getInstancesStatsMap().get(maxUrl).getSeriesCountPercentageGrowth()));

        maxAndMinSeriesInstances.add(new MaxAndMinSeriesInstances("MIN",
                minUrl,
                influxDBInstanceStatsSummary.getInstancesStatsMap().get(minUrl).getSeriesCountPercentageGrowth()));

        maxMinInstancesRepository.saveAll(maxAndMinSeriesInstances);
    }

    private void addLatestStatefulSetNodes() throws Exception {
        int statusCheckCount = 0;
        StatefulSetStatus status = null;
        while(statusCheckCount < 60) {
            status = statefulSetProvider.getStatefulSetStatus(namespace, statefulSetName);
            if(status.getReadyReplicas() == status.getReplicas()) break;

            Thread.sleep(5000);
        }

        if(status == null || status.getReadyReplicas() != status.getReplicas())
            throw new Exception("statefulset nodes are not ready yet or there is no status on that yet");

        setInitialInfluxDBInstances(false);
        initialize();

        scalingInProgress = false;
    }
}
