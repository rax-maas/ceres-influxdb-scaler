package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.PatchStatefulSetInput;
import com.rackspacecloud.metrics.influxdbscaler.models.StatefulSetStatus;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.MaxAndMinSeriesInstances;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import com.rackspacecloud.metrics.influxdbscaler.repositories.MaxMinInstancesRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.RoutingInformationRepository;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MetricsCollector {
    @Data
    public static class MaxAndMinSeriesCountInstances {
        private InstanceSeriesCount max;
        private InstanceSeriesCount min;

        public MaxAndMinSeriesCountInstances(){
            this.max = new InstanceSeriesCount("", Long.MIN_VALUE);
            this.min = new InstanceSeriesCount("", Long.MAX_VALUE);
        }
    }

    @Data
    @RequiredArgsConstructor
    public static class InstanceSeriesCount {
        private String url;
        private long seriesCount;

        public InstanceSeriesCount(String url, long seriesCount) {
            this.url = url;
            this.seriesCount = seriesCount;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private ConcurrentHashMap<String, List<InfluxDBMetricsCollection.InfluxDBMetrics>> concurrentInstanceStats;
    private MaxAndMinSeriesCountInstances maxAndMinSeriesCountInstances;
    private ConcurrentHashMap<String, Long> instanceSeriesMap;

    private boolean scalingInProgress = false;

    @Value("${kubernetes.namespace}")
    private String namespace;

    @Value("${kubernetes.statefulset-name}")
    private String statefulSetName;
    private static final long SERIES_THRESHOLD = 42;

    private InfluxDBHelper influxDBHelper;
    private StatefulSetProvider statefulSetProvider;
    private RoutingInformationRepository routingInformationRepository;
    private MaxMinInstancesRepository maxMinInstancesRepository;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Autowired
    public MetricsCollector(InfluxDBHelper influxDBHelper,
                            StatefulSetProvider statefulSetProvider,
                            RoutingInformationRepository routingInformationRepository,
                            MaxMinInstancesRepository maxMinInstancesRepository) {
        this.influxDBHelper = influxDBHelper;
        this.statefulSetProvider = statefulSetProvider;
        this.routingInformationRepository = routingInformationRepository;
        this.maxMinInstancesRepository = maxMinInstancesRepository;

//        setInitialInfluxDBInstances();

        initialize();
    }

    // TODO: JUST FOR DEV/TEST PURPOSE
    private void setInitialInfluxDBInstances(){
        InfluxDBInstance instance = new InfluxDBInstance();
        instance.setName("influxdb-0");
        instance.setUrl("http://localhost:8086");

        InfluxDBInstance instance1 = new InfluxDBInstance();
        instance1.setName("influxdb-1");
        instance1.setUrl("http://localhost:8087");

        List<InfluxDBInstance> instances = new ArrayList<>();
        instances.add(instance);
        instances.add(instance1);

        routingInformationRepository.saveAll(instances);
    }

    private void initialize() {
//        // TODO: get URLs for the InfluxDB instances
//        List<String> influxDBInstanceURLs = new ArrayList<>();
//        influxDBInstanceURLs.add("http://localhost:8086");

        Iterable<InfluxDBInstance> instances = routingInformationRepository.findAll();

//        influxDBInstanceURLs.addAll(instances)

        this.concurrentInstanceStats = new ConcurrentHashMap<>();
        this.instanceSeriesMap = new ConcurrentHashMap<>();

        instances.forEach( item -> {
            concurrentInstanceStats.put(item.getUrl(), new ArrayList<>());
            instanceSeriesMap.put(item.getUrl(), 0L);
        });
    }

    private List<String> getInfluxDBInstances() {
        List<String> instances = new ArrayList<>();

        return instances;
    }

    @Scheduled(cron = "*/10 * * * * *") // Run every 10 seconds
    public void collectInfluxDBMetrics() throws Exception {
        LOGGER.info("> start");
        LOGGER.info("Current time {}", Instant.now());

        maxAndMinSeriesCountInstances = influxDBHelper.populateStats(concurrentInstanceStats, instanceSeriesMap);

        List<MaxAndMinSeriesInstances> maxAndMinSeriesInstances = new ArrayList<>();
        maxAndMinSeriesInstances.add(new MaxAndMinSeriesInstances("MAX",
                maxAndMinSeriesCountInstances.getMax().getUrl(),
                maxAndMinSeriesCountInstances.getMax().getSeriesCount()));

        maxAndMinSeriesInstances.add(new MaxAndMinSeriesInstances("MIN",
                maxAndMinSeriesCountInstances.getMin().getUrl(),
                maxAndMinSeriesCountInstances.getMin().getSeriesCount()));

        maxMinInstancesRepository.saveAll(maxAndMinSeriesInstances);

        /**
         * Once it breaches the threshold value, and scaling is not in process, then trigger
         * scaling method in a different thread.
         */
        if(maxAndMinSeriesCountInstances.getMax().getSeriesCount() >= SERIES_THRESHOLD && !scalingInProgress) {
            scalingInProgress = true;
            scaleAsync();
        }

        LOGGER.info("Total series count {}", instanceSeriesMap.get("http://localhost:8086"));

        LOGGER.info("< end");
    }

    private void scaleAsync() throws InterruptedException, ExecutionException {
        statefulSetProvider.setNamespace(namespace);
        statefulSetProvider.setName(statefulSetName);

        StatefulSetStatus status = statefulSetProvider.getStatefulSetStatus(namespace, statefulSetName);

        PatchStatefulSetInput patchStatefulSetInput = new PatchStatefulSetInput();
        patchStatefulSetInput.setOp("replace");
        patchStatefulSetInput.setPath("/spec/replicas");
        patchStatefulSetInput.setValue(status.getReplicas() + 2);

        statefulSetProvider.setBodyToPatch(new PatchStatefulSetInput[] {patchStatefulSetInput});

        Future<String> result = executorService.submit(statefulSetProvider);
        System.out.println(result.get());

        scalingInProgress = false;
    }
}
