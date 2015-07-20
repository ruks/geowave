package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TabletStatsTest {

	@BeforeClass
	public void ingest() {

	}

	@Test
	public void testTabletCount() throws Exception {
		TabletStat stat = new TabletStat();
		Assert.assertTrue(stat.getTabletStats("2",
				"rukshan-ThinkPad-T540p:50964").size() > 0);
	}

	@Test
	public void testTablets() throws Exception {

		TabletStat stat = new TabletStat();
		TabletBean tabletStat = stat.getTabletStats("2", "rukshan-ThinkPad-T540p:50964").get(0);
		Assert.assertNotNull(tabletStat);
	}

}
