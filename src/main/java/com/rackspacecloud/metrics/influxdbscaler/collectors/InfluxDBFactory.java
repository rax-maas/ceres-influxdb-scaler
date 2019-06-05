package com.rackspacecloud.metrics.influxdbscaler.collectors;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;

public class InfluxDBFactory {
    public InfluxDB getInfluxDB(String url) {
        InfluxDB influxDB = org.influxdb.InfluxDBFactory.connect(url);
        influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
        return influxDB;
    }
}
