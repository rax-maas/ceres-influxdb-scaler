package com.rackspacecloud.metrics.influxdbscaler.controllers;

import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InfluxDBStatsController {
    private MetricsCollector metricsCollector;

    @Autowired
    public InfluxDBStatsController(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    @RequestMapping(
            path = "/min-series-count-url",
            method = RequestMethod.GET
    )
    public String getMinSeriesCountInstanceUrl() throws Exception {
        String minSeriesInstanceUrl = metricsCollector.getMinSeriesCountInstance();

        return minSeriesInstanceUrl;
    }
}
