package com.rackspacecloud.metrics.influxdbscaler.collectors;

import com.rackspacecloud.metrics.influxdbscaler.models.InfluxDBMetricsCollection;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;

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
    public String getMinInstance(Collection<String> influxDBInstances) throws Exception {
        long minSeriesCount = Long.MAX_VALUE; // Initialize min

        String minInstance = "";

        for(String url : influxDBInstances) {
            // Get all of the stats for given InfluxDB instance
            StatsResults result = getResponseEntity(url, SHOW_STATS);

            try {
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
            final Collection<String> instances) throws Exception {

        Map<String, StatsResults.SeriesMetric[]> seriesMetricCollectionMap = new HashMap<>();

        for(String baseUrl : instances) {
            // Get all of the stats for given InfluxDB instance
            StatsResults result = getResponseEntity(baseUrl, SHOW_STATS);

            try {
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

    private StatsResults getResponseEntity(String baseUrl, String queryString) {
        String url = String.format("%s/query?%s", baseUrl, queryString);
        return restTemplate.getForObject(url, StatsResults.class);
    }
}
