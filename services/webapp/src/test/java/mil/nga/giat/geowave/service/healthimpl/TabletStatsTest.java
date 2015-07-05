package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.datastore.accumulo.app.GeoWaveDemoApp;
import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.junit.Assert;
import org.junit.Test;

public class TabletStatsTest {

	@Test
	public void testTabletCount() throws Exception {
		TabletStats.main(null);
		Assert.assertEquals(2, TabletStats.getTabletStats().size());
	}

	@Test
	public void testTables() throws Exception {
		GeoWaveDemoApp.main(null);
		
		TabletStats.main(null);
		TabletBean stat=TabletStats.getTabletStats().get(0);
		Assert.assertEquals(2, stat.getTablets());
		Assert.assertEquals(14, stat.getEntries());
		Assert.assertEquals(1, stat.getIngest());
		Assert.assertEquals(3, stat.getQuery());
	}
	
	@Test
	public void testTablesAfterIngest() throws Exception {
		TabletStats.main(null);
		TabletBean stat=TabletStats.getTabletStats().get(0);
		Assert.assertEquals(2, stat.getTablets());
		Assert.assertEquals(14, stat.getEntries());
		Assert.assertEquals(1, stat.getIngest());
		Assert.assertEquals(3, stat.getQuery());
	}
}
