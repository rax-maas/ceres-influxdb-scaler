package com.rackspacecloud.metrics.influxdbscaler.models.routing;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash("max-min-series-instances")
@Data
public class MaxAndMinSeriesInstances {
    @Id
    private String type;
    private String url;
    private long seriesCount;

    public MaxAndMinSeriesInstances(String type, String url, long seriesCount) {
        this.type = type;
        this.url = url;
        this.seriesCount = seriesCount;
    }
}
