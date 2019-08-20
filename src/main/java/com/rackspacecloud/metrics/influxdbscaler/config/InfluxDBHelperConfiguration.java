package com.rackspacecloud.metrics.influxdbscaler.config;

import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBHelper;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@EnableConfigurationProperties(RestTemplateConfigurationProperties.class)
public class InfluxDBHelperConfiguration {

    @Value("${statefuleset-stats-caller-threads-count}")
    private int statefulsetStatsCallerThreadsCount;

    @Autowired
    RestTemplateConfigurationProperties config;

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        PoolingHttpClientConnectionManager poolingConnectionManager = new PoolingHttpClientConnectionManager();
        poolingConnectionManager.setMaxTotal(config.getPoolingHttpClientConnectionManager().getMaxTotal());
        return poolingConnectionManager;
    }

    @Bean
    public RequestConfig requestConfig() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(config.getRequestConfig().getConnectionRequestTimeout())
                .setConnectTimeout(config.getRequestConfig().getConnectTimeout())
                .setSocketTimeout(config.getRequestConfig().getSocketTimeout())
                .build();
        return requestConfig;
    }

    @Bean
    public CloseableHttpClient httpClient(
            PoolingHttpClientConnectionManager poolingHttpClientConnectionManager,
            RequestConfig requestConfig) {

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        CloseableHttpClient builder = HttpClientBuilder
                .create()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
        return builder;
    }

    @Bean
    public RestTemplate restTemplate(HttpClient httpClient) {
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }

    @Bean
    @Autowired
    public InfluxDBHelper influxDBHelper(RestTemplate restTemplate, ExecutorService executorService) {
        return new InfluxDBHelper(restTemplate, executorService);
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(statefulsetStatsCallerThreadsCount);
    }
}
