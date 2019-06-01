package com.rackspacecloud.metrics.influxdbscaler.models.routing;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

//@RedisHash("influxdb-instances")
@Data
@AllArgsConstructor

public class InfluxDBInstance {
    @Id
    private String name;
    private String url;
}
