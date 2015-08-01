package mil.nga.giat.geowave.service.jaxbbean;

public class Points
{
	public Points(
			double x,
			double y ) {
		super();
		this.x = x;
		this.y = y;
	}

	double x;
	double y;

	public double getX() {
		return x;
	}

	public void setX(
			double x ) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(
			double y ) {
		this.y = y;
	}
}
