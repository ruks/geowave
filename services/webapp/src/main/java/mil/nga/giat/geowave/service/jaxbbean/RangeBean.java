package mil.nga.giat.geowave.service.jaxbbean;

import org.apache.accumulo.core.data.Range;

public class RangeBean
{
	private Range range;
	private String tablet;
	private String tabletUUID;
	
	public RangeBean(
			Range range,
			String tablet,String tabletUUID ) {
		super();
		this.range = range;
		this.tablet = tablet;
		this.tabletUUID=tabletUUID;
	}

	public String getTabletUUID() {
		return tabletUUID;
	}

	public void setTabletUUID(String tabletUUID) {
		this.tabletUUID = tabletUUID;
	}

	public Range getRange() {
		return range;
	}

	public void setRange(
			Range range ) {
		this.range = range;
	}

	public String getTablet() {
		return tablet;
	}

	public void setTablet(
			String tablet ) {
		this.tablet = tablet;
	}
}
