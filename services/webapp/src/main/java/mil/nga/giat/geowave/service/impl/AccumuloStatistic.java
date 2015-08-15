package mil.nga.giat.geowave.service.impl;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.service.healthimpl.BackgroundWorker;
import mil.nga.giat.geowave.service.healthimpl.TableStats;
import mil.nga.giat.geowave.service.healthimpl.TabletStat;
import mil.nga.giat.geowave.service.jaxbbean.GeoJson;
import mil.nga.giat.geowave.service.jaxbbean.TableBean;
import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

/**
 * Root resource (exposed at "stat" path)
 */
@Path("stat")
public class AccumuloStatistic {
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getIt() {
		return "Got it!";
	}

	@GET
	@Path("/geo/{table}")
	@Produces("application/json")
	public Response geojson(@PathParam("table") String table) {

		BackgroundWorker thread = BackgroundWorker.getInstance();

		List<GeoJson> nodes = thread.getTableExtent(table);

		return Response
				.ok()
				.entity(nodes)
				.header("Access-Contrl-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods",
						"GET, POST, DELETE, PUT").allow("OPTIONS").build();
	}

	@GET
	@Path("/table")
	@Produces("application/json")
	public Response tables() {

		TableStats stat;
		try {
			String instanceName = "geowave";
			String zooServers = "127.0.0.1";
			String user = "root";
			String pass = "password";

			stat = new TableStats(instanceName, zooServers, user, pass);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		List<TableBean> list = stat.getTableStat();

		return Response
				.ok()
				.entity(list)
				.header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods",
						"GET, POST, DELETE, PUT").allow("OPTIONS").build();
	}

	@GET
	@Path("/tablet/{table}")
	@Produces("application/json")
	public Response tablets(@PathParam("table") String table) {

		TabletStat stat;
		try {
			String instanceName = "geowave";
			String zooServers = "127.0.0.1";
			String user = "root";
			String pass = "password";
			stat = new TabletStat(instanceName, zooServers, user, pass);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		List<TabletBean> list = stat.getTabletStats(table);

		return Response
				.ok()
				.entity(list)
				.header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods",
						"GET, POST, DELETE, PUT").allow("OPTIONS").build();
	}

	@GET
	@Path("/tablet/{table}/{tabletId}")
	@Produces("application/json")
	public Response tablet(@PathParam("table") String table,
			@PathParam("tabletId") String tabletId) {

		TabletStat stat;
		try {
			String instanceName = "geowave";
			String zooServers = "127.0.0.1";
			String user = "root";
			String pass = "password";
			stat = new TabletStat(instanceName, zooServers, user, pass);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		List<TabletBean> list = stat.getTabletStats(table);
		TabletBean result = null;
		for (TabletBean bean : list) {
			if (bean.getTabletUUID().equals(tabletId)) {
				result = bean;
			}
		}
		return Response
				.ok()
				.entity(result)
				.header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods",
						"GET, POST, DELETE, PUT").allow("OPTIONS").build();
	}

}
