package com.rackspacecloud.metrics.influxdbscaler.config;

import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBHelper;
import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import com.rackspacecloud.metrics.influxdbscaler.models.StatefulSetStatus;
import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import com.rackspacecloud.metrics.influxdbscaler.providers.InfluxDBInstancesUpdater;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableScheduling
public class ScalerConfig {
    @Value("${kubernetes.namespace}")
    private String namespace;

    @Value("${kubernetes.statefulset-name}")
    private String statefulSetName;

    @Value("${kubernetes.headless-service-name}")
    private String headlessServiceName;

    @Value("${total-series-count.iterations}")
    private int totalSeriesCountIterations;

    @Value("${local-metrics-url}")
    private String localMetricsUrl;

    @Value("${local-metrics-database}")
    private String localMetricsDatabase;

    @Value("${local-metrics-rp}")
    private String localMetricsRetPolicy;

    @Bean
    @Profile("development")
    public InfluxDBInstancesUpdater updater() {
        return statefulSetProvider -> {
            InfluxDBInstance instance1 = new InfluxDBInstance("influxdb-0", "http://localhost:8086");
            InfluxDBInstance instance2 = new InfluxDBInstance("influxdb-1", "http://localhost:8087");

            List<InfluxDBInstance> instances = new ArrayList<>();
            instances.add(instance1);
            instances.add(instance2);

            return instances;
        };
    }

    @Bean
    @Profile("production")
    public InfluxDBInstancesUpdater influxDBInstancesUpdater() {
        return statefulSetProvider -> {
            // URL example for an instance of InfluxDB in statefulset: http://data-influxdb-0.influxdbsvc:8086
            // Get all of the URLs from StatefulSet
            StatefulSetStatus status = statefulSetProvider.getStatefulSetStatus();

            List<InfluxDBInstance> instances = new ArrayList<>();

            for(int i = 0; i < status.getReadyReplicas(); i++) {
                String influxDBInstanceName = String.format("%s-%d", statefulSetName, i);

                String url = String.format("http://%s:8086", influxDBInstanceName);
                if(headlessServiceName != null && !headlessServiceName.isEmpty()) {
                    url = String.format("http://%s.%s:8086", influxDBInstanceName, headlessServiceName);
                }

                instances.add(new InfluxDBInstance(influxDBInstanceName, url));
            }

            return instances;
        };
    }

    @Bean
    @Autowired
    public MetricsCollector metricsCollector(
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            InfluxDBInstancesUpdater updater) {
        return new MetricsCollector(
                influxDBHelper,
                statefulSetProvider,
                updater,
                localMetricsUrl,
                localMetricsDatabase,
                localMetricsRetPolicy
        );
    }

    @Bean
    @Profile("development")
    public StatefulSetProvider statefulSetProvider() throws IOException {
        String homeDir = System.getProperty("user.home");
        String configFileName = Paths.get( homeDir, ".kube/config").toString();
        return new StatefulSetProvider(configFileName, namespace, statefulSetName);
    }

    @Bean
    @Profile("production")
    public StatefulSetProvider getStatefulSetProvider() throws IOException {
        return new StatefulSetProvider(null, namespace, statefulSetName);
    }
}
