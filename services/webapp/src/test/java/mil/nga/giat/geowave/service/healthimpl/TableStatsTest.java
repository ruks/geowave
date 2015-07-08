package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TableStatsTest
{

	@Before
	public void ingest() {
		String[] args = {
			"127.0.0.1:2181",
			"geowave",
			"root",
			"password",
			"ruks"
		};
		SimpleIngest.main(args);
	}

	@Test
	public void testTableCount()
			throws Exception {
		TableStats stat = new TableStats();
		Assert.assertEquals(
				5,
				stat.getTableStat().size());
	}

	@Test
	public void testTablets()
			throws Exception {

		TableStats stat = new TableStats();
		TableBean tableStat = stat.getTableStat().get(
				0);
		Assert.assertNotNull(tableStat);
		Assert.assertNotNull(tableStat.getTablets());
		Assert.assertNotNull(tableStat.getEntries());
	}

}
