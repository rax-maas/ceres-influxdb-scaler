package com.rackspacecloud.metrics.influxdbscaler;

import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import com.rackspacecloud.metrics.influxdbscaler.controllers.InfluxDBStatsController;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.doReturn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(value = InfluxDBStatsController.class)
public class InfluxDBStatsControllerTest {
    MockMvc mockMvc;

    @MockBean
    MetricsCollector metricsCollectorMock;

    @InjectMocks
    InfluxDBStatsController controller;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void test_getMinSeriesCountInstanceUrl() throws Exception {
        String[] instanceUrls = new String[] {"http://localhost:8086", "http://localhost:8087"};

        doReturn(instanceUrls[1])
                .when(metricsCollectorMock).getMinSeriesCountInstance();

        this.mockMvc.perform(get("/min-series-count-url"))
                .andExpect(status().isOk())
                .andExpect(content().string("http://localhost:8087"));
    }
}
