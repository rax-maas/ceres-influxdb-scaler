package com.rackspacecloud.metrics.influxdbscaler.models;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class InfluxDBMetricsCollection {
    Map<String, List<InfluxDBMetrics>> metricsMap;

    public InfluxDBMetricsCollection() {
        metricsMap = new HashMap<>();
    }

    @Data
    public static class InfluxDBMetrics {
        String name;
        Map<String, String> tags;
        Map<String, Long> fields;

        public InfluxDBMetrics() {
            tags = new HashMap<>();
            fields = new HashMap<>();
        }
    }
}
