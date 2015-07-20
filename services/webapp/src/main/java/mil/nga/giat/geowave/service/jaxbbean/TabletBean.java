package mil.nga.giat.geowave.service.jaxbbean;

public class TabletBean {

	private String table;
	private String tablet;
	private long entries;
	private double ingest;
	private double query;
	private double miAvg;
	private double mistd;
	private double miAvges;
	private double maAvg;
	private double mastd;
	private double maAvges;

	public TabletBean(String table, String tablet, long entries, double ingest,
			double query, double miAvg, double mistd, double miAvges,
			double maAvg, double mastd, double maAvges) {
		super();
		this.table = table;
		this.tablet = tablet;
		this.entries = entries;
		this.ingest = ingest;
		this.query = query;
		this.miAvg = miAvg;
		this.mistd = mistd;
		this.miAvges = miAvges;
		this.maAvg = maAvg;
		this.mastd = mastd;
		this.maAvges = maAvges;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getTablet() {
		return tablet;
	}

	public void setTablet(String tablet) {
		this.tablet = tablet;
	}

	public long getEntries() {
		return entries;
	}

	public void setEntries(long entries) {
		this.entries = entries;
	}

	public double getIngest() {
		return ingest;
	}

	public void setIngest(double ingest) {
		this.ingest = ingest;
	}

	public double getQuery() {
		return query;
	}

	public void setQuery(double query) {
		this.query = query;
	}

	public double getMiAvg() {
		return miAvg;
	}

	public void setMiAvg(double miAvg) {
		this.miAvg = miAvg;
	}

	public double getMistd() {
		return mistd;
	}

	public void setMistd(double mistd) {
		this.mistd = mistd;
	}

	public double getMiAvges() {
		return miAvges;
	}

	public void setMiAvges(double miAvges) {
		this.miAvges = miAvges;
	}

	public double getMaAvg() {
		return maAvg;
	}

	public void setMaAvg(double maAvg) {
		this.maAvg = maAvg;
	}

	public double getMastd() {
		return mastd;
	}

	public void setMastd(double mastd) {
		this.mastd = mastd;
	}

	public double getMaAvges() {
		return maAvges;
	}

	public void setMaAvges(double maAvges) {
		this.maAvges = maAvges;
	}

}
