package mil.nga.giat.geowave.service.healthimpl;

public class DataRate
{
	public DataRate(
			int id,
			double rate ) {
		super();
		this.id = id;
		this.rate = rate;
	}

	public int getId() {
		return id;
	}

	public void setId(
			int id ) {
		this.id = id;
	}

	public double getRate() {
		return rate;
	}

	public void setRate(
			double rate ) {
		this.rate = rate;
	}

	private int id;
	private double rate;
}
