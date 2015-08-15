package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import mil.nga.giat.geowave.service.jaxbbean.GeoJson;
import mil.nga.giat.geowave.service.jaxbbean.Points;
import mil.nga.giat.geowave.service.jaxbbean.RangeBean;
import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class BackgroundWorker extends
		Thread implements
		ServletContextListener
{
	private static BackgroundWorker thread = null;
	int i;
	private List<GeoJson> nodes;
	private Map<String, List<GeoJson>> geoMap = new TreeMap<String, List<GeoJson>>();

	public synchronized static BackgroundWorker getInstance() {
		if (thread == null) {
			thread = new BackgroundWorker();
			thread.start();
		}
		return thread;

	}

	public List<GeoJson> getTableExtent(
			String table ) {
		return geoMap.get(table);
	}

	public List<GeoJson> getNodes() {
		return nodes;
	}

	public void setNodes(
			List<GeoJson> nodes ) {
		this.nodes = nodes;
	}

	public synchronized void contextInitialized(
			ServletContextEvent sce ) {
		if ((thread == null) || (!thread.isAlive())) {
			thread = new BackgroundWorker();
			thread.start();
		}
	}

	public void contextDestroyed(
			ServletContextEvent sce ) {
		try {
			// thread.doShutdown();
			thread.interrupt();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// while (true) {
		try {
			 TablesconvexHull();
			System.out.println("run " + (i++));
			Thread.sleep(5 * 60 * 000);
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// }
	}

	private void convexHull(
			String table ) {
		nodes = new ArrayList<GeoJson>();
		GeospatialExtent ex = new GeospatialExtent(
				"geowave",
				"127.0.0.1",
				"password",
				"root",
				"ruks");

		// String table = "ruks_SPATIAL_VECTOR_IDX";
		List<RangeBean> splits = ex.getSplits(table);
		System.out.println("splits " + splits.size());
		for (RangeBean bean : splits) {
			Coordinate[] points = ex.extent(
					table,
					bean.getRange());
			Geometry g = ex.getConvexHull(points);
			System.out.println(g);
			Coordinate[] c = g.getCoordinates();
			List<Points> no = new ArrayList<Points>();
			for (Coordinate co : c) {
				no.add(new Points(
						co.x,
						co.y));
			}
			nodes.add(new GeoJson(
					bean.getTabletUUID(),
					no));
		}
		geoMap.put(
				table,
				nodes);
	}

	private void TablesconvexHull() {
		TableStats stat;
		try {
			String instanceName = "geowave";
			String zooServers = "127.0.0.1";
			String user = "root";
			String pass = "password";

			stat = new TableStats(
					instanceName,
					zooServers,
					user,
					pass);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		List<TableBean> list = stat.getTableStat();
		for (TableBean tableBean : list) {
			try {
				convexHull(tableBean.getTableName());
			}
			catch (Exception e) {
				e.printStackTrace();
				System.out.println("table " + tableBean.getTableName() + " Not support");
			}
		}
	}
}
