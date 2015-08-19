package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

	private static final int MAXIMUM_CONCURRENT = 1;

	private ScheduledThreadPoolExecutor executor = null;

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
			// thread.start();
		}
		executor = new ScheduledThreadPoolExecutor(
				MAXIMUM_CONCURRENT);
		Runnable object = thread;
		executor.scheduleAtFixedRate(
				object,
				5,
				360,
				TimeUnit.SECONDS);
	}

	public void contextDestroyed(
			ServletContextEvent sce ) {
		executor.shutdown();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//
		try {
			TablesconvexHull();
		}
		catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
		System.out.println("run " + (i++));
	}

	private void convexHull(
			String table )
			throws Exception {
		nodes = new ArrayList<GeoJson>();

		String instanceName = GeowavePropertyReader.readProperty(GeowaveConstant.instanceName);
		String zooServers = GeowavePropertyReader.readProperty(GeowaveConstant.zooServers);
		String user = GeowavePropertyReader.readProperty(GeowaveConstant.user);
		String pass = GeowavePropertyReader.readProperty(GeowaveConstant.pass);

		GeospatialExtent ex = new GeospatialExtent(
				instanceName,
				zooServers,
				user,
				pass);

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

		String instanceName = GeowavePropertyReader.readProperty(GeowaveConstant.instanceName);
		String zooServers = GeowavePropertyReader.readProperty(GeowaveConstant.zooServers);
		String user = GeowavePropertyReader.readProperty(GeowaveConstant.user);
		String pass = GeowavePropertyReader.readProperty(GeowaveConstant.pass);

		stat = new TableStats(
				instanceName,
				zooServers,
				user,
				pass);

		List<TableBean> list = stat.getTableStat();
		for (TableBean tableBean : list) {
			try {
				convexHull(tableBean.getTableName());
			}
			catch (Exception e) {
				// e.printStackTrace();
				System.out.println("table " + tableBean.getTableName() + " Not support");
			}
		}
	}
}
