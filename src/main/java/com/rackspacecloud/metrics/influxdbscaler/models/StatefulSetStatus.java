package com.rackspacecloud.metrics.influxdbscaler.models;

import lombok.Data;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class StatefulSetStatus {
    private int currentReplicas;
    private int readyReplicas;
    private int replicas;
    private int updateReplicas;
}
