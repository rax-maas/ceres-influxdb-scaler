package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.metrics.influxdbscaler.models.DatabaseSeriesStatsResults;
import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import com.rackspacecloud.metrics.influxdbscaler.models.stats.InfluxDBInstanceStatsSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InfluxDBHelper {
    private RestTemplate restTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBHelper.class);

    @Autowired
    public InfluxDBHelper(RestTemplate restTemplate){
        this.restTemplate = restTemplate;
    }

    public void populateStats(
            final Map<String, List<InfluxDBMetricsCollection.InfluxDBMetrics>> instancesStats,
            final InfluxDBInstanceStatsSummary influxDBInstanceStatsSummary) throws Exception {

        long maxSeriesCount = Long.MIN_VALUE; // Initialize max
        long minSeriesCount = Long.MAX_VALUE; // Initialize min

        for(String baseUrl : instancesStats.keySet()) {
            String queryString = "q=SHOW STATS";

            // Get all of the stats for given InfluxDB instance
            ResponseEntity<String> response = getResponseEntity(baseUrl, queryString);

            String body = response.getBody();
            ObjectMapper mapper = new ObjectMapper();

            try {
                StatsResults result = mapper.readValue(body, StatsResults.class);
                StatsResults.StatsResult[] statsResults = result.getResults();

                if (statsResults.length != 1) throw new Exception("Either no result or more than 1 result found.");

                StatsResults.SeriesMetric[] seriesMetricCollection = statsResults[0].getSeries();
                List<InfluxDBMetricsCollection.InfluxDBMetrics> instanceMetricsList = instancesStats.get(baseUrl);
                InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount =
                        influxDBInstanceStatsSummary.getInstancesStatsMap().get(baseUrl);

                populateSeriesCountForTheInstance(seriesMetricCollection,
                        instanceMetricsList, instanceDatabasesSeriesCount);

                if(influxDBInstanceStatsSummary.getInstanceUrlWithMaxSeriesCount().equals("") ||
                        instanceDatabasesSeriesCount.getTotalSeriesCount() >= maxSeriesCount) {
                    influxDBInstanceStatsSummary.setInstanceUrlWithMaxSeriesCount(baseUrl);
                    maxSeriesCount = instanceDatabasesSeriesCount.getTotalSeriesCount();
                }

                if(influxDBInstanceStatsSummary.getInstanceUrlWithMinSeriesCount().equals("") ||
                        instanceDatabasesSeriesCount.getTotalSeriesCount() < minSeriesCount) {
                    influxDBInstanceStatsSummary.setInstanceUrlWithMinSeriesCount(baseUrl);
                    minSeriesCount = instanceDatabasesSeriesCount.getTotalSeriesCount();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public List<String[]> getTenantIdAndMeasurementList(
            final String instanceUrl,
            final List<String> databaseList) throws Exception {
        for(String database : databaseList) {
            String queryString = String.format("q=SHOW SERIES ON %s", database);

            // Get all of the series
            ResponseEntity<String> response = getResponseEntity(instanceUrl, queryString);

            String body = response.getBody();
            ObjectMapper mapper = new ObjectMapper();

            try {
                DatabaseSeriesStatsResults result = mapper.readValue(body, DatabaseSeriesStatsResults.class);
                DatabaseSeriesStatsResults.StatsResult[] statsResults = result.getResults();

                if (statsResults.length != 1) throw new Exception("No result found.");

                DatabaseSeriesStatsResults.Series[] series = statsResults[0].getSeries();
                if (series.length != 1) throw new Exception("No series found.");

                String[][] seriesItems = series[0].getValues();

                List<String[]> tenantIdAndMeasurementPairs = new ArrayList<>();

                for(int i = 0; i < seriesItems.length; i++) {
                    if(seriesItems[i].length != 1) throw new Exception("Invalid series item");

                    String[] tenantIdAndMeasurement = getTenantIdAndMeasurement(seriesItems[i][0]);
                    tenantIdAndMeasurementPairs.add(tenantIdAndMeasurement);
                }

                return tenantIdAndMeasurementPairs;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    private String[] getTenantIdAndMeasurement(String seriesString) throws Exception {
        String[] strArray = seriesString.split(",");

        if(strArray.length < 2) throw new Exception("seriesString is not a line protocol string");

        String[] tenantIdAndMeasurement = new String[2];
        tenantIdAndMeasurement[1] = strArray[0];

        for(int i = 1; i < strArray.length; i++) {
            String[] tagAndValue = strArray[i].split("=");

            if(tagAndValue.length != 2) throw new Exception("tag-value entry is not a line protocol string");
            if(tagAndValue[0].equalsIgnoreCase("tenantId")) {
                tenantIdAndMeasurement[0] = tagAndValue[1];
                break;
            }
        }

        return tenantIdAndMeasurement;
    }

    private void populateSeriesCountForTheInstance(
            StatsResults.SeriesMetric[] seriesMetricCollection,
            List<InfluxDBMetricsCollection.InfluxDBMetrics> influxDBMetricsList,
            InfluxDBInstanceStatsSummary.InstanceDatabasesSeriesCount instanceDatabasesSeriesCount) throws Exception {

        Long totalSeriesCount = 0L;
        Map<String, Long> databaseSeriesCountMap = instanceDatabasesSeriesCount.getDatabaseSeriesCountMap();

        for(int i = 0; i < seriesMetricCollection.length; i++) {
            InfluxDBMetricsCollection.InfluxDBMetrics metric = new InfluxDBMetricsCollection.InfluxDBMetrics();

            metric.setName(seriesMetricCollection[i].getName());
            metric.setTags(seriesMetricCollection[i].getTags());

            String[] columns = seriesMetricCollection[i].getColumns();
            Long[][] values = seriesMetricCollection[i].getValues();

            if(values.length != 1) throw new Exception("I expect only one array of long values");

            Map<String, Long> fields = metric.getFields();

            for(int j = 0; j < columns.length; j++) {
                fields.put(columns[j], values[0][j]);
            }

            if(metric.getName().equalsIgnoreCase("database")) {
                long seriesCount = metric.getFields().get("numSeries");
                String databaseName = metric.getTags().get("database");
                databaseSeriesCountMap.put(databaseName, seriesCount);
                totalSeriesCount += seriesCount;
            }

            influxDBMetricsList.add(metric);
        }

        instanceDatabasesSeriesCount.setTotalSeriesCount(totalSeriesCount);
    }

    private ResponseEntity<String> getResponseEntity(String baseUrl, String queryString) {
        ResponseEntity<String> response = null;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        String url = String.format("%s/query?%s", baseUrl, queryString);
        try {
            response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
        }
        catch(Exception ex){
            LOGGER.error("restTemplate.exchange threw exception with message: {}", ex.getMessage());
        }
        return response;
    }
}
