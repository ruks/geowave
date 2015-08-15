package mil.nga.giat.geowave.service.jaxbbean;


public class TableBean {
	private String tableName;
	private String state;
	private int tablets;
	private int offlineTablets;
	private String entries;
	private String entriesInMemory;
	private String ingest;
	private String entriesRead;
	private String entriesReturned;
	private String holdTime;
	private String majorunningScans;
	private String minorCompactions;
	private String majorCompactions;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public int getTablets() {
		return tablets;
	}

	public void setTablets(int tablets) {
		this.tablets = tablets;
	}

	public int getOfflineTablets() {
		return offlineTablets;
	}

	public void setOfflineTablets(int offlineTablets) {
		this.offlineTablets = offlineTablets;
	}

	public String getHoldTime() {
		return holdTime;
	}

	public void setHoldTime(String holdTime) {
		this.holdTime = holdTime;
	}

	

	public TableBean(String tableName, String state, int tablets,
			int offlineTablets, String entries, String entriesInMemory,
			String ingest, String entriesRead, String entriesReturned,
			String holdTime, String majorunningScans,
			String minorCompactions, String majorCompactions) {
		super();
		this.tableName = tableName;
		this.state = state;
		this.tablets = tablets;
		this.offlineTablets = offlineTablets;
		this.entries = entries;
		this.entriesInMemory = entriesInMemory;
		this.ingest = ingest;
		this.entriesRead = entriesRead;
		this.entriesReturned = entriesReturned;
		this.holdTime = holdTime;
		this.majorunningScans = majorunningScans;
		this.minorCompactions = minorCompactions;
		this.majorCompactions = majorCompactions;
	}

	public String getMajorunningScans() {
		return majorunningScans;
	}

	public void setMajorunningScans(String majorunningScans) {
		this.majorunningScans = majorunningScans;
	}

	public String getMinorCompactions() {
		return minorCompactions;
	}

	public void setMinorCompactions(String minorCompactions) {
		this.minorCompactions = minorCompactions;
	}

	public String getMajorCompactions() {
		return majorCompactions;
	}

	public void setMajorCompactions(String majorCompactions) {
		this.majorCompactions = majorCompactions;
	}

	public String getEntries() {
		return entries;
	}

	public void setEntries(String entries) {
		this.entries = entries;
	}

	public String getEntriesInMemory() {
		return entriesInMemory;
	}

	public void setEntriesInMemory(String entriesInMemory) {
		this.entriesInMemory = entriesInMemory;
	}

	public String getIngest() {
		return ingest;
	}

	public void setIngest(String ingest) {
		this.ingest = ingest;
	}

	public String getEntriesRead() {
		return entriesRead;
	}

	public void setEntriesRead(String entriesRead) {
		this.entriesRead = entriesRead;
	}

	public String getEntriesReturned() {
		return entriesReturned;
	}

	public void setEntriesReturned(String entriesReturned) {
		this.entriesReturned = entriesReturned;
	}

}
