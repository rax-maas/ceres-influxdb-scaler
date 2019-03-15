package com.rackspacecloud.metrics.influxdbscaler.models.routing;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash("influxdb-instances")
@Data
@RequiredArgsConstructor
public class InfluxDBInstance {
    @Id
    private String name;
    private String url;

    public InfluxDBInstance(String name, String url) {
        this.name = name;
        this.url = url;
    }
}
