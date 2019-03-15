package com.rackspacecloud.metrics.influxdbscaler.models.routing;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

import java.util.Map;

@RedisHash("instance-databases-series-count")
@Data
public class DatabasesSeriesCount {
    @Id
    private String url;
    private long seriesCount;
    private Map<String, Long> databaseSeriesCountMap;

    public DatabasesSeriesCount(String url, long seriesCount, Map<String, Long> databaseSeriesCountMap) {
        this.url = url;
        this.seriesCount = seriesCount;
        this.databaseSeriesCountMap = databaseSeriesCountMap;
    }
}
