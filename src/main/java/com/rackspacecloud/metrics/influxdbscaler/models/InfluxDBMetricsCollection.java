package com.rackspacecloud.metrics.influxdbscaler.models;

import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
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

        public String toLineProtocol(String instanceUrl) {
            if(StringUtils.isEmpty(name)) return null;

            List<String> tagStrings = new ArrayList<>();
            tags.forEach((key, value) -> tagStrings.add(String.format("%s=%s", key, value)));

            tagStrings.add(String.format("%s=%s", "instance", instanceUrl));

            List<String> fieldStrings = new ArrayList<>();
            fields.forEach((k, v) -> fieldStrings.add(String.format("%s=%s", k, v)));

            return String.format("%s,%s %s",
                    name,
                    String.join(",", tagStrings),
                    String.join(",", fieldStrings));
        }
    }
}
