package com.rackspacecloud.metrics.influxdbscaler.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabaseSeriesStatsResults {
    StatsResult[] results;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StatsResult {
        Series[] series;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Series {
        String[] columns;
        String[][] values;
    }
}
