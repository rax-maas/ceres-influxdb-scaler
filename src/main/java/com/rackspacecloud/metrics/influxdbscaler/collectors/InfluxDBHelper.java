package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfluxDBHelper {
    private final static String SHOW_STATS = "q=SHOW STATS";
    private RestTemplate restTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBHelper.class);

    @Autowired
    public InfluxDBHelper(RestTemplate restTemplate){
        this.restTemplate = restTemplate;
    }

    /**
     * Get InfluxDB instance with min series count from the given list of InfluxDB influxDBInstances
     * @param influxDBInstances
     * @return
     * @throws Exception
     */
    public String getMinInstance(List<String> influxDBInstances) throws Exception {
        long minSeriesCount = Long.MAX_VALUE; // Initialize min

        String minInstance = "";

        for(String url : influxDBInstances) {
            // Get all of the stats for given InfluxDB instance
            ResponseEntity<String> response = getResponseEntity(url, SHOW_STATS);

            String body = response.getBody();
            ObjectMapper mapper = new ObjectMapper();

            try {
                StatsResults result = mapper.readValue(body, StatsResults.class);
                StatsResults.StatsResult[] statsResults = result.getResults();

                if (statsResults.length != 1) throw new Exception("Either no result or more than 1 result found.");

                StatsResults.SeriesMetric[] seriesMetricCollection = statsResults[0].getSeries();

                long totalSeriesCount = getTotalSeriesCount(seriesMetricCollection);

                if(totalSeriesCount < minSeriesCount) {
                    minInstance = url;
                    minSeriesCount = totalSeriesCount;
                }
            } catch (IOException e) {
                LOGGER.error("While reading stats result for URL [{}], got error: [{}]", url, e.getMessage());
            }
        }

        return minInstance;
    }

    public Map<String, StatsResults.SeriesMetric[]> getSeriesMetricCollection(
            final Set<String> instances) throws Exception {

        Map<String, StatsResults.SeriesMetric[]> seriesMetricCollectionMap = new HashMap<>();

        for(String baseUrl : instances) {
            // Get all of the stats for given InfluxDB instance
            ResponseEntity<String> response = getResponseEntity(baseUrl, SHOW_STATS);

            String body = response.getBody();
            ObjectMapper mapper = new ObjectMapper();

            try {
                StatsResults result = mapper.readValue(body, StatsResults.class);
                StatsResults.StatsResult[] statsResults = result.getResults();

                if (statsResults.length != 1) throw new Exception("Either no result or more than 1 result found.");

                StatsResults.SeriesMetric[] seriesMetricCollection = statsResults[0].getSeries();
                seriesMetricCollectionMap.put(baseUrl, seriesMetricCollection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return seriesMetricCollectionMap;
    }

    /**
     * Get total series count from the stats result
     * @param seriesMetricCollection
     * @return
     * @throws Exception
     */
    private long getTotalSeriesCount(StatsResults.SeriesMetric[] seriesMetricCollection) throws Exception {
        Long totalSeriesCount = 0L;

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
                totalSeriesCount += seriesCount;
            }
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
