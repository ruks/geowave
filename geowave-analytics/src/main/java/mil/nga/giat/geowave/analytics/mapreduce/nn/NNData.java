package mil.nga.giat.geowave.analytics.mapreduce.nn;

import org.apache.commons.codec.binary.Hex;

import mil.nga.giat.geowave.index.ByteArrayId;

public class NNData<T> implements
		Comparable<NNData<T>>
{
	private T element;
	private ByteArrayId id;
	private double distance;

	public NNData() {}

	public NNData(
			T element,
			ByteArrayId id,
			double distance ) {
		super();
		this.element = element;
		this.id = id;
		this.distance = distance;
	}

	public NNData(
			final NNData<T> element,
			final double distance ) {
		super();
		this.element = element.getElement();
		this.id = element.getId();
		this.distance = distance;
	}

	protected ByteArrayId getId() {
		return id;
	}

	protected void setId(
			ByteArrayId id ) {
		this.id = id;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(
			final double distance ) {
		this.distance = distance;
	}

	public T getElement() {
		return element;
	}

	protected void setElement(
			final T neighbor ) {
		this.element = neighbor;
	}

	@Override
	public int hashCode() {
		return ((element == null) ? 0 : element.hashCode());
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		@SuppressWarnings("unchecked")
		NNData<T> other = (NNData<T>) obj;
		if (element == null) {
			if (other.element != null) return false;
		}
		else if (!element.equals(other.element)) return false;
		return true;
	}

	@Override
	public int compareTo(
			NNData<T> otherNNData ) {
		final int dist = Double.compare(
				distance,
				otherNNData.distance);
		// do not care about the ordering based on the neighbor data.
		// just need to force some ordering if they are not the same.
		return dist == 0 ? hashCode() - otherNNData.hashCode() : dist;
	}

	@Override
	public String toString() {
		return Hex.encodeHexString(id.getBytes()) + ":" + element.toString() + "(" + this.distance + ")";
	}
}
