package com.rackspacecloud.metrics.influxdbscaler;

import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBFactory;
import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBHelper;
import com.rackspacecloud.metrics.influxdbscaler.collectors.MetricsCollector;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import com.rackspacecloud.metrics.influxdbscaler.providers.InfluxDBInstancesUpdater;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import org.influxdb.InfluxDB;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MetricsCollectorTests {
	private final static String SHOW_STATS = "q=SHOW STATS";

	@MockBean
	InfluxDBHelper influxDBHelperMock;

	@MockBean
	StatefulSetProvider statefulSetProviderMock;

    @MockBean
    InfluxDBFactory influxDBFactoryMock;

    @Autowired
    ApplicationContext context;

	@Before
	public void setUp() {
	}

    @Test
    public void verify_collectInfluxDBMetrics_writesToInfluxDB() throws Exception {
        String[] instanceUrls = new String[] {"http://localhost:8086", "http://localhost:8087"};

        Map<String, StatsResults.SeriesMetric[]> seriesMetricMap = new HashMap<>();

        seriesMetricMap.put(instanceUrls[0], getSeriesMetricsMap(74L, 77L));
        seriesMetricMap.put(instanceUrls[1], getSeriesMetricsMap(54L, 57L));

        InfluxDB influxDBMock = mock(InfluxDB.class);
        when(influxDBFactoryMock.getInfluxDB("http://localhost:8086")).thenReturn(influxDBMock);

        MetricsCollector metricsCollector = context.getBean(MetricsCollector.class);

        doReturn(seriesMetricMap)
                .when(influxDBHelperMock).getSeriesMetricCollection(metricsCollector.getInfluxDBInstanceUrls());

        metricsCollector.collectInfluxDBMetrics();

        verify(influxDBMock, Mockito.times(1))
                .write("ceres", "autogen", InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS,
                        "database,database=db_0,instance=http://localhost:8086 numSeries=74,numMeasurements=10\n" +
                                "database,database=db_1,instance=http://localhost:8086 numSeries=77,numMeasurements=10\n" +
                                "database,database=db_0,instance=http://localhost:8087 numSeries=54,numMeasurements=10\n" +
                                "database,database=db_1,instance=http://localhost:8087 numSeries=57,numMeasurements=10");
    }

	@Test
	public void verify_getMinSeriesCountInstance_returnsMinInstance() throws Exception {
		String[] instanceUrls = new String[] {"http://localhost:8086", "http://localhost:8087"};

        MetricsCollector metricsCollector = context.getBean(MetricsCollector.class);

        doReturn(instanceUrls[1])
                .when(influxDBHelperMock).getMinInstance(metricsCollector.getInfluxDBInstanceUrls());

		String actualMinInstance = metricsCollector.getMinSeriesCountInstance();

        Assert.assertEquals(instanceUrls[1], actualMinInstance);
	}

	private StatsResults.SeriesMetric[] getSeriesMetricsMap(long numSeries1, long numSeries2) {
		StatsResults.SeriesMetric seriesMetric1 = new StatsResults.SeriesMetric();
		seriesMetric1.setName("database");
		seriesMetric1.getTags().put("database", "db_0");
		seriesMetric1.setColumns(new String[] {"numMeasurements", "numSeries"});
		Long[][] values1 = {
				{10L, numSeries1}
		};
		seriesMetric1.setValues(values1);

		StatsResults.SeriesMetric seriesMetric2 = new StatsResults.SeriesMetric();
		seriesMetric2.setName("database");
		seriesMetric2.getTags().put("database", "db_1");
		seriesMetric2.setColumns(new String[] {"numMeasurements", "numSeries"});
		Long[][] values2 = {
				{10L, numSeries2}
		};
		seriesMetric2.setValues(values2);

		return new StatsResults.SeriesMetric[] {seriesMetric1, seriesMetric2};
	}
}
