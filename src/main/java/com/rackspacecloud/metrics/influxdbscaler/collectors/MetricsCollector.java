package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.PatchStatefulSetInput;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    private static final long SERIES_THRESHOLD = 500;

    private InfluxDBHelper influxDBHelper;
    private StatefulSetProvider statefulSetProvider;
    private RoutingInformationRepository routingInformationRepository;
    private MaxMinInstancesRepository maxMinInstancesRepository;
    private DatabasesSeriesCountRepository databasesSeriesCountRepository;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public MetricsCollector(
            String namespace,
            String statefulSetName,
            String headlessServiceName,
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            RoutingInformationRepository routingInformationRepository,
            MaxMinInstancesRepository maxMinInstancesRepository,
            DatabasesSeriesCountRepository databasesSeriesCountRepository,
            InfluxDBInstanceStatsSummary influxDBInstanceStatsSummary) {
        this.namespace = namespace;
        this.statefulSetName = statefulSetName;
        this.headlessServiceName = headlessServiceName;
        this.influxDBHelper = influxDBHelper;
        this.statefulSetProvider = statefulSetProvider;
        this.routingInformationRepository = routingInformationRepository;
        this.maxMinInstancesRepository = maxMinInstancesRepository;
        this.databasesSeriesCountRepository = databasesSeriesCountRepository;
        this.influxDBInstanceStatsSummary = influxDBInstanceStatsSummary;

        setInitialInfluxDBInstances();

        initialize();
    }

    /**
     * Get all of the InfluxDB URLs from the InfluxDB StatefulSet and add it to Redis database
     */
    private void setInitialInfluxDBInstances(){
//        // TODO: JUST FOR DEV/TEST PURPOSE
//        InfluxDBInstance instance = new InfluxDBInstance();
//        instance.setName("influxdb-0");
//        instance.setUrl("http://localhost:8086");
//
//        InfluxDBInstance instance1 = new InfluxDBInstance();
//        instance1.setName("influxdb-1");
//        instance1.setUrl("http://localhost:8087");

//        http://data-influxdb-0.influxdbsvc:8086

        List<InfluxDBInstance> instances = new ArrayList<>();

        // Get all of the URLs from StatefulSet
        StatefulSetStatus status = statefulSetProvider.getStatefulSetStatus(namespace, statefulSetName);

        for(int i = 0; i < status.getReadyReplicas(); i++) {
            String influxDBInstanceName = String.format("%s-%d", statefulSetName, i);
            String url = String.format("http://%s.%s:8086", influxDBInstanceName, headlessServiceName);
            instances.add(new InfluxDBInstance(influxDBInstanceName, url));

            LOGGER.info("Initializing with InfluxDB URL: [{}]", url);
        }

        routingInformationRepository.saveAll(instances);
    }

    private void initialize() {
//        influxDBInstanceStatsSummary = new InfluxDBInstanceStatsSummary();
        instancesStats = new HashMap<>();

        routingInformationRepository.findAll().forEach( item -> {
            String url = item.getUrl();
            instancesStats.put(url, new ArrayList<>());
            InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount =
                    new InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount(-1L, new HashMap<>());
            influxDBInstanceStatsSummary.getInstancesStatsMap().put(url, instanceDatabasesSeriesCount);
        });
    }

    /**
     * This is the entry method that collects all of the metrics for all of the InfluxDB instances in the system.
     * @throws Exception
     */
    @Scheduled(cron = "*/3 * * * * *") // Run every 3 seconds
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

        if(!scalingInProgress && instanceDatabasesSeriesCount.getTotalSeriesCount() > SERIES_THRESHOLD) {
            scalingInProgress = true;
//            scaleAsync();
            List<String> databasesToMoveOut = splitInstanceLoad(instanceDatabasesSeriesCount);

            List<String[]> tenantIdAndMeasurementPairs =
                    influxDBHelper.getTenantIdAndMeasurementList(maxInstance, databasesToMoveOut);

            scalingInProgress = false;
        }

        LOGGER.info("< end");
    }

    private void saveDatabasesSeriesCount() {
        Map<String, InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount> instancesStats =
                influxDBInstanceStatsSummary.getInstancesStatsMap();

        List<DatabasesSeriesCount> databasesSeriesCountRecords = new ArrayList<>();

        for(String url : instancesStats.keySet()) {
            InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount =
                    instancesStats.get(url);
            DatabasesSeriesCount databasesSeriesCount = new DatabasesSeriesCount(url,
                    instanceDatabasesSeriesCount.getTotalSeriesCount(),
                    instanceDatabasesSeriesCount.getDatabaseSeriesCountMap());

            LOGGER.info("Total series count in [{}] is [{}]", url, instanceDatabasesSeriesCount.getTotalSeriesCount());

            databasesSeriesCountRecords.add(databasesSeriesCount);
        }

        databasesSeriesCountRepository.saveAll(databasesSeriesCountRecords);
    }

    private void saveMaxAndMinSeriesInfluxDBInstances(String maxUrl, String minUrl) {
        List<MaxAndMinSeriesInstances> maxAndMinSeriesInstances = new ArrayList<>();
        maxAndMinSeriesInstances.add(new MaxAndMinSeriesInstances("MAX",
                maxUrl, influxDBInstanceStatsSummary.getInstancesStatsMap().get(maxUrl).getTotalSeriesCount()));

        maxAndMinSeriesInstances.add(new MaxAndMinSeriesInstances("MIN",
                minUrl, influxDBInstanceStatsSummary.getInstancesStatsMap().get(minUrl).getTotalSeriesCount()));

        maxMinInstancesRepository.saveAll(maxAndMinSeriesInstances);
    }

    private void getTenantIdAndMeasurementListToReroute(
            String instanceUrl, List<String> databaseToMoveOut) throws Exception {
        influxDBHelper.getTenantIdAndMeasurementList(instanceUrl, databaseToMoveOut);
    }


    private List<String> splitInstanceLoad(
            InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount) {
        long targetSeriesCount = instanceDatabasesSeriesCount.getTotalSeriesCount()/2;
        Map<String, Long> databaseSeriesCount = instanceDatabasesSeriesCount.getDatabaseSeriesCountMap();

        List<Map.Entry<String, Long>> entries = new ArrayList<>(databaseSeriesCount.entrySet());

        entries.removeIf(entry -> !entry.getKey().startsWith("db_"));

        System.out.println(entries);

        entries.sort(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return -1 * (o1.getValue().compareTo(o2.getValue()));
            }
        });

        System.out.println(entries);

        long tempTotal = 0L;
        List<String> databaseGroupToMoveOut = new ArrayList<>();
        for(int i = 0; i < entries.size(); i++) {
            Map.Entry<String, Long> entry = entries.get(i);
            tempTotal += entry.getValue();

            if(tempTotal > targetSeriesCount) break;

            databaseGroupToMoveOut.add(entry.getKey());
        }

        return databaseGroupToMoveOut;
    }

    private void scaleAsync() throws InterruptedException, ExecutionException {
        statefulSetProvider.setNamespace(namespace);
        statefulSetProvider.setName(statefulSetName);

        StatefulSetStatus status = statefulSetProvider.getStatefulSetStatus(namespace, statefulSetName);

        PatchStatefulSetInput patchStatefulSetInput = new PatchStatefulSetInput();
        patchStatefulSetInput.setOp("replace");
        patchStatefulSetInput.setPath("/spec/replicas");
        patchStatefulSetInput.setValue(2);

        statefulSetProvider.setBodyToPatch(new PatchStatefulSetInput[] {patchStatefulSetInput});

        Future<String> result = executorService.submit(statefulSetProvider);
        System.out.println(result.get());
    }
}
