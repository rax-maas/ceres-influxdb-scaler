package com.rackspacecloud.metrics.influxdbscaler.models.routing;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor

public class InfluxDBInstance {
    private String name;
    private String url;
}
