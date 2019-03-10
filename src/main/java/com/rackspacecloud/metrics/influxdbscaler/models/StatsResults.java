package com.rackspacecloud.metrics.influxdbscaler.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatsResults {
    StatsResult[] results;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StatsResult {
        SeriesMetric[] series;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SeriesMetric {
        String name;

        Map<String, String> tags;
        String[] columns;
        Long[][] values;

        public SeriesMetric() {
            tags = new HashMap<>();
        }
    }
}
