package com.rackspacecloud.metrics.influxdbscaler.config;

import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBHelper;
import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import com.rackspacecloud.metrics.influxdbscaler.repositories.MaxMinInstancesRepository;
import com.rackspacecloud.metrics.influxdbscaler.repositories.RoutingInformationRepository;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.naming.ConfigurationException;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableScheduling
@EnableRedisRepositories(value = "org.springframework.data.redis")
public class ScalerConfig {
    private static final String influxDbUrls = "http://localhost:8086";

    @Bean
    Map<String, InfluxDB> urlInfluxDBInstanceMap() throws ConfigurationException {
        // TODO: Get InfluxDBUrls from Redis
        String[] influxDbUrlsCollection = influxDbUrls.split(";");

        if(influxDbUrlsCollection.length == 0) throw new ConfigurationException("No database URLs found.");

        HashMap<String, InfluxDB> urlInstanceMap = new HashMap<>();

        for(int i = 0; i < influxDbUrlsCollection.length; i++) {
            String url = influxDbUrlsCollection[i];
            urlInstanceMap.put(url, InfluxDBFactory.connect(url));
        }
        return urlInstanceMap;
    }

    @Bean
    @Autowired
    public MetricsCollector metricsCollector(
            InfluxDBHelper influxDBHelper,
            StatefulSetProvider statefulSetProvider,
            RoutingInformationRepository routingInformationRepository,
            MaxMinInstancesRepository maxMinInstancesRepository) {
        return new MetricsCollector(
                influxDBHelper, statefulSetProvider, routingInformationRepository, maxMinInstancesRepository);
    }
}
