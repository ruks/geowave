package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.datastore.accumulo.app.GeoWaveDemoApp;
import mil.nga.giat.geowave.service.jaxbbean.TableBean;
import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.junit.Assert;
import org.junit.Test;

public class TabletStatsTest {

	@Test
	public void testTabletCount() throws Exception {
		TabletStats stat = new TabletStats();
		Assert.assertEquals(2, stat.getTabletStats().size());
	}

	@Test
	public void testTables() throws Exception {
		GeoWaveDemoApp.main(null);

		TableStats stat = new TableStats();
		TableBean tableStat = stat.getTableStat().get(0);
		Assert.assertEquals(2, tableStat.getTablets());
		Assert.assertEquals(14, tableStat.getEntries());
	}

	@Test
	public void testTablesAfterIngest() throws Exception {

		TableStats stat = new TableStats();
		TableBean tableStat = stat.getTableStat().get(0);
		Assert.assertEquals(2, tableStat.getTablets());
		Assert.assertEquals(14, tableStat.getEntries());
	}
}
