package com.rackspacecloud.metrics.influxdbscaler.providers;

import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;

import java.util.List;

@FunctionalInterface
public interface InfluxDBInstancesUpdater {
    List<InfluxDBInstance> update(StatefulSetProvider statefulSetProvider);
}
