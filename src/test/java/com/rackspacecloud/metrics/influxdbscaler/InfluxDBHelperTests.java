package com.rackspacecloud.metrics.influxdbscaler;

import static org.mockito.Mockito.when;

import com.rackspacecloud.metrics.influxdbscaler.collectors.InfluxDBHelper;
import com.rackspacecloud.metrics.influxdbscaler.models.StatsResults;
import com.rackspacecloud.metrics.influxdbscaler.providers.StatefulSetProvider;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("development")
public class InfluxDBHelperTests {
	private final static String SHOW_STATS = "q=SHOW STATS";

	@Mock
	RestTemplate restTemplateMock;

	@Mock
	ExecutorService executorServiceMock;

	@MockBean
	StatefulSetProvider statefulSetProviderMock;

	@Before
	public void setUp() {
		executorServiceMock = Executors.newFixedThreadPool(5);
	}

	@Test
	public void givenInfluxdbInstances_getMinInstance_successfully() throws Exception {
		String[] instanceUrls = getInstanceUrls();

		InfluxDBHelper influxDBHelper = new InfluxDBHelper(restTemplateMock, executorServiceMock);

		String url1 = String.format("%s/query?%s", instanceUrls[0], SHOW_STATS);
		when(restTemplateMock.getForObject(url1, StatsResults.class))
				.thenReturn(getDummyStatsResults(74L, 77L));

		String url2 = String.format("%s/query?%s", instanceUrls[1], SHOW_STATS);
		when(restTemplateMock.getForObject(url2, StatsResults.class))
				.thenReturn(getDummyStatsResults(64L, 67L));


		String minInstance = influxDBHelper.getMinInstance(Arrays.asList(instanceUrls));

		Assert.assertEquals("Failed to find min instance url.", instanceUrls[1], minInstance);
	}

	private String[] getInstanceUrls() {
		String[] instanceUrls = new String[] {"influxdb_instance_url1", "influxdb_instance_url2"};
		String url1 = String.format("%s/query?%s", instanceUrls[0], SHOW_STATS);
		when(restTemplateMock.getForObject(url1, StatsResults.class))
				.thenReturn(getDummyStatsResults(74L, 77L));

		String url2 = String.format("%s/query?%s", instanceUrls[1], SHOW_STATS);
		when(restTemplateMock.getForObject(url2, StatsResults.class))
				.thenReturn(getDummyStatsResults(64L, 67L));
		return instanceUrls;
	}

	@Test
	public void givenInfluxdbInstances_getSeriesMetricCollection_successfully() throws Exception {
		String[] instanceUrls = getInstanceUrls();

		InfluxDBHelper influxDBHelper = new InfluxDBHelper(restTemplateMock, executorServiceMock);

		Map<String, StatsResults.SeriesMetric[]> metricCollectionMap =
				influxDBHelper.getSeriesMetricCollection(new HashSet<>(Arrays.asList(instanceUrls)));

		Assert.assertEquals("Couldn't get series metrics collection failed.",
				2, metricCollectionMap.keySet().size());


		Assert.assertEquals(74, metricCollectionMap.get(instanceUrls[0])[0].getValues()[0][1].longValue());
		Assert.assertEquals(77, metricCollectionMap.get(instanceUrls[0])[1].getValues()[0][1].longValue());

		Assert.assertEquals(64, metricCollectionMap.get(instanceUrls[1])[0].getValues()[0][1].longValue());
		Assert.assertEquals(67, metricCollectionMap.get(instanceUrls[1])[1].getValues()[0][1].longValue());
	}

	private StatsResults getDummyStatsResults(long numSeries1, long numSeries2) {
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

		StatsResults.StatsResult statsResult = new StatsResults.StatsResult();
		statsResult.setSeries(new StatsResults.SeriesMetric[] {seriesMetric1, seriesMetric2});

		StatsResults statsResults = new StatsResults();
		statsResults.setResults(new StatsResults.StatsResult[]{statsResult});

		return statsResults;
	}
}
