package mil.nga.giat.geowave.service.jaxbbean;

import java.util.List;

public class GeoJson
{

	public GeoJson(
			String tablet,
			List<Points> points ) {
		super();
		this.tablet = tablet;
		this.points = points;
	}

	public String getTablet() {
		return tablet;
	}

	public List<Points> getPoints() {
		return points;
	}

	public void setPoints(
			List<Points> points ) {
		this.points = points;
	}

	public void setTablet(
			String tablet ) {
		this.tablet = tablet;
	}

	String tablet;
	List<Points> points;

}
