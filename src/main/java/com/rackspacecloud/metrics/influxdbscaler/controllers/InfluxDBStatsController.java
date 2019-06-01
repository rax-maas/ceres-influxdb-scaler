package com.rackspacecloud.metrics.influxdbscaler.controllers;

import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InfluxDBStatsController {
    private MetricsCollector metricsCollector;

    @Autowired
    public InfluxDBStatsController(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    @GetMapping(path = "/min-series-count-url")
    public String getMinSeriesCountInstanceUrl() throws Exception {
        return metricsCollector.getMinSeriesCountInstance();
    }
}
