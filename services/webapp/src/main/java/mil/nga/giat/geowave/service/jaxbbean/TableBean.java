package mil.nga.giat.geowave.service.jaxbbean;

import org.apache.accumulo.core.master.thrift.Compacting;

public class TableBean
{
	private String tableName;
	private String state;
	private int tablets;
	private int offlineTablets;
	private long entries;
	private long entriesInMemory;
	private double ingest;
	private double entriesRead;
	private double entriesReturned;
	private long holdTime;
	private Compacting majorunningScans;
	private Compacting minorCompactions;
	private Compacting majorCompactions;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(
			String tableName ) {
		this.tableName = tableName;
	}

	public String getState() {
		return state;
	}

	public void setState(
			String state ) {
		this.state = state;
	}

	public int getTablets() {
		return tablets;
	}

	public void setTablets(
			int tablets ) {
		this.tablets = tablets;
	}

	public int getOfflineTablets() {
		return offlineTablets;
	}

	public void setOfflineTablets(
			int offlineTablets ) {
		this.offlineTablets = offlineTablets;
	}

	public long getEntries() {
		return entries;
	}

	public void setEntries(
			long entries ) {
		this.entries = entries;
	}

	public long getEntriesInMemory() {
		return entriesInMemory;
	}

	public void setEntriesInMemory(
			long entriesInMemory ) {
		this.entriesInMemory = entriesInMemory;
	}

	public double getIngest() {
		return ingest;
	}

	public void setIngest(
			double ingest ) {
		this.ingest = ingest;
	}

	public double getEntriesRead() {
		return entriesRead;
	}

	public void setEntriesRead(
			double entriesRead ) {
		this.entriesRead = entriesRead;
	}

	public double getEntriesReturned() {
		return entriesReturned;
	}

	public void setEntriesReturned(
			double entriesReturned ) {
		this.entriesReturned = entriesReturned;
	}

	public long getHoldTime() {
		return holdTime;
	}

	public void setHoldTime(
			long holdTime ) {
		this.holdTime = holdTime;
	}

	public Compacting getMajorunningScans() {
		return majorunningScans;
	}

	public void setMajorunningScans(
			Compacting majorunningScans ) {
		this.majorunningScans = majorunningScans;
	}

	public Compacting getMinorCompactions() {
		return minorCompactions;
	}

	public void setMinorCompactions(
			Compacting minorCompactions ) {
		this.minorCompactions = minorCompactions;
	}

	public Compacting getMajorCompactions() {
		return majorCompactions;
	}

	public void setMajorCompactions(
			Compacting majorCompactions ) {
		this.majorCompactions = majorCompactions;
	}

	public TableBean(
			String tableName,
			String state,
			int tablets,
			int offlineTablets,
			long entries,
			long entriesInMemory,
			double ingest,
			double entriesRead,
			double entriesReturned,
			long holdTime,
			Compacting majorunningScans,
			Compacting minorCompactions,
			Compacting majorCompactions ) {
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

}
