package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InfluxDBHelper {
    private RestTemplate restTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBHelper.class);

    @Autowired
    public InfluxDBHelper(RestTemplate restTemplate){
        this.restTemplate = restTemplate;
    }

    public MetricsCollector.MaxAndMinSeriesCountInstances populateStats(
            final ConcurrentHashMap<String, List<InfluxDBMetricsCollection.InfluxDBMetrics>> concurrentHashMap,
            final ConcurrentHashMap<String, Long> instanceSeriesMap) throws Exception {

        MetricsCollector.MaxAndMinSeriesCountInstances maxAndMinSeriesCountInstances =
                new MetricsCollector.MaxAndMinSeriesCountInstances();

        MetricsCollector.InstanceSeriesCount maxSeriesCountInstance = maxAndMinSeriesCountInstances.getMax();
        MetricsCollector.InstanceSeriesCount minSeriesCountInstance = maxAndMinSeriesCountInstances.getMin();

        for(String baseUrl : concurrentHashMap.keySet()) {
            String queryString = "q=SHOW STATS";
            ResponseEntity<String> response = getResponseEntity(baseUrl, queryString);

            String body = response.getBody();

            ObjectMapper mapper = new ObjectMapper();

            try {
                StatsResults result = mapper.readValue(body, StatsResults.class);
                StatsResults.StatsResult[] statsResults = result.getResults();

                if (statsResults.length != 1) throw new Exception("No stats found.");

                StatsResults.SeriesMetric[] seriesMetricCollection = statsResults[0].getSeries();
                List<InfluxDBMetricsCollection.InfluxDBMetrics> influxDBMetricsList = concurrentHashMap.get(baseUrl);

                Long totalSeriesCount = getSeriesCountForTheInstance(seriesMetricCollection, influxDBMetricsList);
                instanceSeriesMap.put(baseUrl, totalSeriesCount);

                if(maxSeriesCountInstance.getUrl().equals("") ||
                        totalSeriesCount >= maxSeriesCountInstance.getSeriesCount()) {
                    maxSeriesCountInstance.setUrl(baseUrl);
                    maxSeriesCountInstance.setSeriesCount(totalSeriesCount);
                }

                if(minSeriesCountInstance.getUrl().equals("") ||
                        totalSeriesCount < minSeriesCountInstance.getSeriesCount()) {
                    minSeriesCountInstance.setUrl(baseUrl);
                    minSeriesCountInstance.setSeriesCount(totalSeriesCount);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return maxAndMinSeriesCountInstances;
    }

    private Long getSeriesCountForTheInstance(
            StatsResults.SeriesMetric[] seriesMetricCollection,
            List<InfluxDBMetricsCollection.InfluxDBMetrics> influxDBMetricsList) throws Exception {

        Long totalSeriesCount = 0L;

        for(int i = 0; i < seriesMetricCollection.length; i++) {
            InfluxDBMetricsCollection.InfluxDBMetrics metric =
                    new InfluxDBMetricsCollection.InfluxDBMetrics();

            metric.setName(seriesMetricCollection[i].getName());
            metric.setTags(seriesMetricCollection[i].getTags());

            String[] columns = seriesMetricCollection[i].getColumns();
            Long[][] values = seriesMetricCollection[i].getValues();

            if(values.length != 1) throw new Exception("I don't expect more than one array of long values");

            Map<String, Long> fields = metric.getFields();

            for(int j = 0; j < columns.length; j++) {
                fields.put(columns[j], values[0][j]);
            }

            if(metric.getName().equalsIgnoreCase("database")) {
                totalSeriesCount += metric.getFields().get("numSeries");
            }

            influxDBMetricsList.add(metric);
        }
        return totalSeriesCount;
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
