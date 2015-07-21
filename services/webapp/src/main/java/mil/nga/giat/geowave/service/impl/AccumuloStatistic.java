package mil.nga.giat.geowave.service.impl;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.service.healthimpl.TableStats;
import mil.nga.giat.geowave.service.healthimpl.TabletStat;
import mil.nga.giat.geowave.service.jaxbbean.TableBean;
import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

/**
 * Root resource (exposed at "stat" path)
 */
@Path("stat")
public class AccumuloStatistic
{
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getIt() {
		return "Got it!";
	}

	@GET
	@Path("/table")
	@Produces("application/json")
	public Response tables() {

		TableStats stat;
		try {
			stat = new TableStats();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		List<TableBean> list = stat.getTableStat();

		return Response.ok().entity(
				list).header(
				"Access-Control-Allow-Origin",
				"*").header(
				"Access-Control-Allow-Methods",
				"GET, POST, DELETE, PUT").allow(
				"OPTIONS").build();
	}

	@GET
	@Path("/tablet")
	@Produces("application/json")
	public Response tablets() {

		TabletStat stat;
		try {
			stat = new TabletStat();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		List<TabletBean> list = stat.getTabletStats(
				"2",
				"rukshan-ThinkPad-T540p:55358");

		return Response.ok().entity(
				list).header(
				"Access-Control-Allow-Origin",
				"*").header(
				"Access-Control-Allow-Methods",
				"GET, POST, DELETE, PUT").allow(
				"OPTIONS").build();
	}
}
