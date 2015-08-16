package mil.nga.giat.geowave.service.jaxbbean;

public class TabletBean
{

	private String table;
	private String tablet;
	private String entries;
	private String ingest;
	private String query;
	private String miAvg;
	private String mistd;
	private String miAvges;
	private String maAvg;
	private String mastd;
	private String maAvges;
	private String tabletUUID;

	public String getTabletUUID() {
		return tabletUUID;
	}

	public void setTabletUUID(
			String tabletUUID ) {
		this.tabletUUID = tabletUUID;
	}

	public TabletBean(
			String table,
			String tablet,
			String entries,
			String ingest,
			String query,
			String miAvg,
			String mistd,
			String miAvges,
			String maAvg,
			String mastd,
			String maAvges,
			String tabletUUID ) {
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
		this.tabletUUID = tabletUUID;
	}

	public String getTable() {
		return table;
	}

	public void setTable(
			String table ) {
		this.table = table;
	}

	public String getTablet() {
		return tablet;
	}

	public void setTablet(
			String tablet ) {
		this.tablet = tablet;
	}

	public String getEntries() {
		return entries;
	}

	public void setEntries(
			String entries ) {
		this.entries = entries;
	}

	public String getIngest() {
		return ingest;
	}

	public void setIngest(
			String ingest ) {
		this.ingest = ingest;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(
			String query ) {
		this.query = query;
	}

	public String getMiAvg() {
		return miAvg;
	}

	public void setMiAvg(
			String miAvg ) {
		this.miAvg = miAvg;
	}

	public String getMistd() {
		return mistd;
	}

	public void setMistd(
			String mistd ) {
		this.mistd = mistd;
	}

	public String getMiAvges() {
		return miAvges;
	}

	public void setMiAvges(
			String miAvges ) {
		this.miAvges = miAvges;
	}

	public String getMaAvg() {
		return maAvg;
	}

	public void setMaAvg(
			String maAvg ) {
		this.maAvg = maAvg;
	}

	public String getMastd() {
		return mastd;
	}

	public void setMastd(
			String mastd ) {
		this.mastd = mastd;
	}

	public String getMaAvges() {
		return maAvges;
	}

	public void setMaAvges(
			String maAvges ) {
		this.maAvges = maAvges;
	}

}
