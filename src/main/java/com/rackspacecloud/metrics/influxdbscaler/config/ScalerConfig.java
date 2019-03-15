package com.rackspacecloud.metrics.influxdbscaler.config;

import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBHelper;
import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import com.rackspacecloud.metrics.influxdbscaler.models.stats.InfluxDBInstanceStatsSummary;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import com.rackspacecloud.metrics.influxdbscaler.repositories.DatabasesSeriesCountRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.MaxMinInstancesRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.RoutingInformationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class ScalerConfig {
    @Value("${kubernetes.namespace}")
    private String namespace;

    @Value("${kubernetes.statefulset-name}")
    private String statefulSetName;

    @Value("${kubernetes.headless-service-name}")
    private String headlessServiceName;

    @Bean
    public InfluxDBInstanceStatsSummary influxDBInstanceStatsSummary() {
        return new InfluxDBInstanceStatsSummary();
    }

    @Bean
    @Autowired
    public MetricsCollector metricsCollector(
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            RoutingInformationRepository routingInformationRepository,
            MaxMinInstancesRepository maxMinInstancesRepository,
            DatabasesSeriesCountRepository databasesSeriesCountRepository,
            InfluxDBInstanceStatsSummary influxDBInstanceStatsSummary) {
        return new MetricsCollector(
                namespace,
                statefulSetName,
                headlessServiceName,
                influxDBHelper,
                statefulSetProvider,
                routingInformationRepository,
                maxMinInstancesRepository,
                databasesSeriesCountRepository,
                influxDBInstanceStatsSummary
        );
    }
}
