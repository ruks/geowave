package mil.nga.giat.geowave.service.impl;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import mil.nga.giat.geowave.service.healthimpl.Data;
import mil.nga.giat.geowave.service.healthimpl.Monitor;

import org.apache.accumulo.core.util.Pair;

/**
 * Root resource (exposed at "monitor" path)
 */
@Path("/monitor")
public class AccumuloMonitor
{

	/**
	 * Method handling HTTP GET requests. The returned object will be sent to
	 * the client as "text/plain" media type.
	 * 
	 * @return String that will be returned as a text/plain response.
	 * @throws IOException
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public boolean isRunning() {
		return Data.isStarted();
	}

	@GET
	@Path("/getLoadOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getLoadOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getLoadOverTime();
		return list;
	}

	@GET
	@Path("/getIngestRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getIngestRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getIngestRateOverTime();
		return list;
	}

	@GET
	@Path("/getIngestByteRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getIngestByteRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getIngestByteRateOverTime();
		return list;
	}

	@GET
	@Path("/getMinorCompactionsOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getMinorCompactionsOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getMinorCompactionsOverTime();
		return list;
	}

	@GET
	@Path("/getMajorCompactionsOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getMajorCompactionsOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getMajorCompactionsOverTime();
		return list;
	}

	@GET
	@Path("/getLookupsOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getLookupsOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getLookupsOverTime();
		return list;
	}

	@GET
	@Path("/getQueryRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getQueryRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getQueryRateOverTime();
		return list;
	}

	@GET
	@Path("/getScanRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getScanRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getScanRateOverTime();
		return list;
	}

	@GET
	@Path("/getQueryByteRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getQueryByteRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getQueryByteRateOverTime();
		return list;
	}

	@GET
	@Path("/getIndexCacheHitRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getIndexCacheHitRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getIndexCacheHitRateOverTime();
		return list;
	}

	@GET
	@Path("/getDataCacheHitRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getDataCacheHitRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getDataCacheHitRateOverTime();
		return list;
	}

}
