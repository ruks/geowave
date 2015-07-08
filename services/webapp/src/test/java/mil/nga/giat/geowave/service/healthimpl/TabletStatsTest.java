package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TabletStatsTest
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
	public void testTabletCount()
			throws Exception {
		TabletStats stat = new TabletStats();
		Assert.assertTrue(stat.getTabletStats().size() > 0);
	}

	@Test
	public void testTablets()
			throws Exception {

		TabletStats stat = new TabletStats();
		TabletBean tabletStat = stat.getTabletStats().get(
				0);
		Assert.assertNotNull(tabletStat);
	}

}
