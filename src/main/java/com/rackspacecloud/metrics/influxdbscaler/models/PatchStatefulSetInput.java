package com.rackspacecloud.metrics.influxdbscaler.models;

import lombok.Data;

@Data
public class PatchStatefulSetInput {
    private String op;
    private String path;
    private int value;
}
