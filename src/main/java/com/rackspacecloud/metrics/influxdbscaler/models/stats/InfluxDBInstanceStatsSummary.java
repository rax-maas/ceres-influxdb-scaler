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
         * Collection of total series count in a given InfluxDB instance for given iteration
         */
        long[] totalSeriesCountArray;

        long latestTotalSeriesCount;

        private int writeIndex;

        private long seriesCountPercentageGrowth;

        /**
         * Map of databaseName and their corresponding series count
         */
        Map<String, Long> databaseSeriesCountMap;

        public InstanceDatabasesSeriesCount(long[] totalSeriesCountArray, Map<String, Long> databaseSeriesCountMap) {
            this.writeIndex = 0;
            this.seriesCountPercentageGrowth = 0;
            this.totalSeriesCountArray = totalSeriesCountArray;
            this.databaseSeriesCountMap = databaseSeriesCountMap;
        }

        /**
         * Add total series count to the circular array collection
         * @param totalSeriesCount
         */
        public void addToTotalSeriesCountArray(long totalSeriesCount) {
            int latestIndex = writeIndex;
            totalSeriesCountArray[writeIndex++] = totalSeriesCount;
            latestTotalSeriesCount = totalSeriesCount;

            if(writeIndex == totalSeriesCountArray.length) writeIndex = 0;

            int oldestIndex = writeIndex; // Now since writeIndex has moved to next one

            long diff = totalSeriesCountArray[latestIndex] - totalSeriesCountArray[oldestIndex];

            if(diff > 0 && totalSeriesCountArray[oldestIndex] > 0) {
                seriesCountPercentageGrowth = (diff * 100) / totalSeriesCountArray[oldestIndex];
            }
        }
    }
}
