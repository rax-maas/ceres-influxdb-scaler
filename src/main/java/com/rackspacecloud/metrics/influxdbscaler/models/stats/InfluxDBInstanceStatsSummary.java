package com.rackspacecloud.metrics.influxdbscaler.models.stats;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * InfluxDBInstanceStatsSummary class contains summary data for all of the stats data collected for
 * all of the InfluxDB instances running in the metrics system.
 */
@Data
public class InfluxDBInstanceStatsSummary {
    /**
     * instanceUrlWithMinSeriesCount contains the URL for the InfluxDB instance that has minimum
     * number of series count among all of the influxDB instances in the system.
     */
    String instanceUrlWithMinSeriesCount;

    /**
     * instanceUrlWithMaxSeriesCount contains the URL for the InfluxDB instance that has maximum
     * number of series count among all of the influxDB instances in the system.
     */
    String instanceUrlWithMaxSeriesCount;

    /**
     * instancesStatsMap contains key/value pair where keys are the InfluxDB instance URLs and
     * values are their corresponding stats on all of the databases and their series count.
     * Data hierarchy example:
     * key could be "http://localhost:8086" (which is path for one InfluxDB instance)
     * value could be an object that contains following:
     *  - total series count in that given instance (in this case http://localhost:8086) - say 765 is the total
     *  - Another map of databaseName and their corresponding series count.
     */
    Map<String, InstanceDatabasesSeriesCount> instancesStatsMap;

    public InfluxDBInstanceStatsSummary() {
        this.instanceUrlWithMinSeriesCount = "";
        this.instanceUrlWithMaxSeriesCount = "";
        this.instancesStatsMap = new HashMap<>();
    }

    /**
     * InstanceDatabasesSeriesCount class contains total series count in one InfluxDB instance
     * and a map of databaseName and their corresponding series count.
     */
    @Data
    @RequiredArgsConstructor
    public static class InstanceDatabasesSeriesCount {
        /**
         * Total series count in a given InfluxDB instance
         */
        long totalSeriesCount;

        /**
         * Map of databaseName and their corresponding series count
         */
        Map<String, Long> databaseSeriesCountMap;

        public InstanceDatabasesSeriesCount(long totalSeriesCount, Map<String, Long> databaseSeriesCountMap) {
            this.totalSeriesCount = totalSeriesCount;
            this.databaseSeriesCountMap = databaseSeriesCountMap;
        }
    }
}
