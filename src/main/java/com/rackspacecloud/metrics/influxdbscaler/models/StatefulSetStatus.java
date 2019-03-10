package com.rackspacecloud.metrics.influxdbscaler.models;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class StatefulSetStatus {
    private int currentReplicas;
    private int readyReplicas;
    private int replicas;
    private int updateReplicas;

    public StatefulSetStatus(int currentReplicas, int readyReplicas, int replicas, int updateReplicas) {
        this.currentReplicas = currentReplicas;
        this.readyReplicas = readyReplicas;
        this.replicas = replicas;
        this.updateReplicas = updateReplicas;
    }
}
