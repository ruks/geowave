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

/**
 * Class is use to start Demon thread when web application first loaded. It find
 * the Geo-spatial coordinates per tablet for all the Table and store in a Map.
 * 
 * @author rukshan
 * 
 */
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

	/**
	 * Singleton implementation of the class
	 * 
	 * @return BackgroundWorker
	 */
	public synchronized static BackgroundWorker getInstance() {
		if (thread == null) {
			thread = new BackgroundWorker();
			thread.start();
		}
		return thread;

	}

	/**
	 * Return all the coordinates of the tablet of table provided
	 * 
	 * @param table
	 *            name of the table
	 * @return List<GeoJson>: Coordinates
	 */
	public List<GeoJson> getTableExtent(
			String table ) {
		return geoMap.get(table);
	}

	/**
	 * Return all the tablet coordinates across tables
	 * 
	 * @return
	 */
	public List<GeoJson> getNodes() {
		return nodes;
	}

	public void setNodes(
			List<GeoJson> nodes ) {
		this.nodes = nodes;
	}

	/**
	 * Fire when application loaded into container. Start the demon thread.
	 */
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

	/**
	 * Fire when Application destroy or unload. Stop the demon thread.
	 */
	public void contextDestroyed(
			ServletContextEvent sce ) {
		executor.shutdown();
	}

	/**
	 * Thread job implementation
	 */
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

	/**
	 * Calculate convexHull of a tablet of provided table
	 * 
	 * @param table
	 *            Table name
	 * @throws Exception
	 */
	private void convexHull(
			String table )
			throws Exception {
		nodes = new ArrayList<GeoJson>();

		// get the configurations
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

		// get ranges of each tablet
		List<RangeBean> splits = ex.getSplits(table);

		System.out.println("splits " + splits.size());

		// finding convexHull of all the tablet
		for (RangeBean bean : splits) {
			Coordinate[] points = ex.extent(
					table,
					bean.getRange()); // Find the geo-spatial extent of each
										// tablet
			Geometry g = ex.getConvexHull(points); // calculate convexHull of
													// the extent
			System.out.println(g);
			Coordinate[] c = g.getCoordinates(); // get the convexHull
													// coordinates
			List<Points> no = new ArrayList<Points>();

			// adding all the convexHull points to list
			for (Coordinate co : c) {
				no.add(new Points(
						co.x,
						co.y));
			}

			// adding tablet's convexHull to list
			nodes.add(new GeoJson(
					bean.getTabletUUID(),
					no));
		}

		// Mapping table name to list of tablet convexHull
		geoMap.put(
				table,
				nodes);
	}

	/**
	 * start to find convexHull for all the table's tablet
	 */
	private void TablesconvexHull() {
		TableStats stat;

		String instanceName = GeowavePropertyReader.readProperty(GeowaveConstant.instanceName);
		String zooServers = GeowavePropertyReader.readProperty(GeowaveConstant.zooServers);
		String user = GeowavePropertyReader.readProperty(GeowaveConstant.user);
		String pass = GeowavePropertyReader.readProperty(GeowaveConstant.pass);

		// init to get table names
		stat = new TableStats(
				instanceName,
				zooServers,
				user,
				pass);

		List<TableBean> list = stat.getTableStat(); // getting table name

		for (TableBean tableBean : list) {
			try {
				// find convecHull for each table
				convexHull(tableBean.getTableName());
			}
			catch (Exception e) {
				// e.printStackTrace();
				System.out.println("table " + tableBean.getTableName() + " Not support");
			}
		}
	}
}
