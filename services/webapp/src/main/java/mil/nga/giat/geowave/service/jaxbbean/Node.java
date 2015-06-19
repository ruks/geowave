package mil.nga.giat.geowave.service.jaxbbean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Node
{
	public int id;
	public double rate;
	public String name;
	public double ingestRate;
	public double scanRate;
	public double indexCacheHits;
	public double dataCacheHits;

	public Node(
			int id,
			double rate,
			String name,
			double ingestRate,
			double scanRate,
			double indexCacheHits,
			double dataCacheHits ) {
		super();
		this.id = id;
		this.rate = rate;
		this.name = name;
		this.ingestRate = ingestRate;
		this.scanRate = scanRate;
		this.indexCacheHits = indexCacheHits;
		this.dataCacheHits = dataCacheHits;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public double getIngestRate() {
		return ingestRate;
	}

	public void setIngestRate(
			double ingestRate ) {
		this.ingestRate = ingestRate;
	}

	public double getScanRate() {
		return scanRate;
	}

	public void setScanRate(
			double scanRate ) {
		this.scanRate = scanRate;
	}

	public double getIndexCacheHits() {
		return indexCacheHits;
	}

	public void setIndexCacheHits(
			double indexCacheHits ) {
		this.indexCacheHits = indexCacheHits;
	}

	public double getDataCacheHits() {
		return dataCacheHits;
	}

	public void setDataCacheHits(
			double dataCacheHits ) {
		this.dataCacheHits = dataCacheHits;
	}

	public Node() {} // JAXB needs this

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

}
