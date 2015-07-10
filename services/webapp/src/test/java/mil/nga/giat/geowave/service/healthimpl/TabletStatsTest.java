package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TabletStatsTest
{

	@BeforeClass
	public void ingest() {

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
