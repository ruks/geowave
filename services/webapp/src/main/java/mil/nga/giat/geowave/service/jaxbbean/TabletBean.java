package mil.nga.giat.geowave.service.jaxbbean;

import org.apache.accumulo.core.master.thrift.Compacting;

public class TabletBean
{
	private String name;
	private int tablets;
	private long lastContact;
	private int entries = 0;
	private int ingest = 0;
	private int query = 0;
	private long holdTime;
	private Compacting scans;
	private Compacting minor;
	private Compacting major;
	private double datacHits;
	private double indexcHits;
	private double osLoad;

	public TabletBean(
			String name,
			int tablets,
			long lastContact,
			int entries,
			int ingest,
			int query,
			long holdTime,
			Compacting scans,
			Compacting minor,
			Compacting major,
			double datacHits,
			double indexcHits,
			double osLoad ) {
		super();
		this.name = name;
		this.tablets = tablets;
		this.lastContact = lastContact;
		this.entries = entries;
		this.ingest = ingest;
		this.query = query;
		this.holdTime = holdTime;
		this.scans = scans;
		this.minor = minor;
		this.major = major;
		this.datacHits = datacHits;
		this.indexcHits = indexcHits;
		this.osLoad = osLoad;
	}

	public long getHoldTime() {
		return holdTime;
	}

	public void setHoldTime(
			long holdTime ) {
		this.holdTime = holdTime;
	}

	public double getOsLoad() {
		return osLoad;
	}

	public void setOsLoad(
			double osLoad ) {
		this.osLoad = osLoad;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public int getTablets() {
		return tablets;
	}

	public void setTablets(
			int tablets ) {
		this.tablets = tablets;
	}

	public long getLastContact() {
		return lastContact;
	}

	public void setLastContact(
			long lastContact ) {
		this.lastContact = lastContact;
	}

	public double getDatacHits() {
		return datacHits;
	}

	public void setDatacHits(
			double datacHits ) {
		this.datacHits = datacHits;
	}

	public double getIndexcHits() {
		return indexcHits;
	}

	public void setIndexcHits(
			double indexcHits ) {
		this.indexcHits = indexcHits;
	}

	public void setQuery(
			int query ) {
		this.query = query;
	}

	public void setScans(
			Compacting scans ) {
		this.scans = scans;
	}

	public void setMinor(
			Compacting minor ) {
		this.minor = minor;
	}

	public int getEntries() {
		return entries;
	}

	public void setEntries(
			int entries ) {
		this.entries = entries;
	}

	public int getIngest() {
		return ingest;
	}

	public void setIngest(
			int ingest ) {
		this.ingest = ingest;
	}

	public Compacting getMajor() {
		return major;
	}

	public void setMajor(
			Compacting major ) {
		this.major = major;
	}

	public int getQuery() {
		return query;
	}

	public Compacting getScans() {
		return scans;
	}

	public Compacting getMinor() {
		return minor;
	}
}
