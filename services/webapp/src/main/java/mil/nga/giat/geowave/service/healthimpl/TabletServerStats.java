package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.service.jaxbbean.TabletServerBean;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

/**
 * Class to fetch tabler server based stat data
 * 
 * @author rukshan
 * 
 */
public class TabletServerStats
{
	private List<TabletServerBean> tabletStats;
	private MasterMonitorInfo masterMonitorInfo = null;

	/**
	 * constructor to create object
	 * 
	 * @param instanceName
	 * @param zooServers
	 * @param user
	 * @param pass
	 * @throws Exception
	 */
	public TabletServerStats(
			String instanceName,
			String zooServers,
			String user,
			String pass )
			throws Exception {

		// getting instance
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);

		MasterClientService.Iface client = null;

		try {
			// getting context
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(
							inst));
			client = MasterClient.getConnectionWithRetry(context);

			// getting master info object
			masterMonitorInfo = client.getMasterStats(
					Tracer.traceInfo(),
					context.rpcCreds());
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		}
		finally {

			if (client != null) MasterClient.close(client);
		}

	}

	/**
	 * return list of tabletserver related stat data
	 * 
	 * @return list of tablet data stat
	 */
	public List<TabletServerBean> getTabletStats() {
		// get the tablet server stat
		List<TabletServerStatus> tabs = masterMonitorInfo.getTServerInfo();
		tabletStats = new ArrayList<TabletServerBean>();

		// iterate over each tablet server stat
		for (int i = 0; i < tabs.size(); i++) {
			TabletServerStatus sta = tabs.get(i);

			String name = sta.getName(); // set the name
			int tablets = sta.getTableMapSize(); // set the tablets
			long lastContact = sta.getLastContact(); // set the last access time
			long holdTime = 0; // set the holdtime

			Map<String, TableInfo> map = sta.getTableMap(); // get the table map
			Set<String> key = map.keySet(); // get all the table names

			int entries = 0, ingest = 0, query = 0;
			Compacting scans = new Compacting(
					0,
					0);
			Compacting minor = new Compacting(
					0,
					0);
			Compacting major = new Compacting(
					0,
					0);

			Object[] arr = key.toArray();
			for (int j = 0; j < arr.length; j++) {
				TableInfo info = map.get(arr[j]); // get the table stat
				entries += info.getRecs(); // adding each table'ss entries
											// together
				ingest += info.getIngestRate(); // adding each table's ingest
												// rate together
				query += info.getQueryRate(); // adding each table's query rate
												// together

				if (info.getScans() != null) {
					scans.setRunning(info.getScans().getRunning()); // set the
																	// scan
																	// running
					scans.setQueued(info.getScans().getQueued()); // set the
																	// scan
																	// queuing

					minor.setRunning(info.getMinors().getRunning()); // set the
																		// Minor
																		// running
					minor.setQueued(info.getMinors().getQueued()); // set the
																	// Minor
																	// queuing

					major.setRunning(info.getMajors().getRunning()); // set the
																		// Major
																		// running
					major.setQueued(info.getMajors().getQueued()); // set the
																	// Major
																	// queuing
				}

			}

			double datacHits = sta.getDataCacheHits() / (sta.getDataCacheRequest() + 0.0); // set
																							// the
																							// data
																							// hits
																							// as
																							// percentage
			double indexcHits = sta.getIndexCacheHits() / (sta.getIndexCacheRequest() + 0.0); // set
																								// the
																								// index
																								// hits
																								// as
																								// percentage

			double osLoad = sta.getOsLoad(); // set the os load

			// add the all the tablet server stat to object
			tabletStats.add(new TabletServerBean(
					name,
					tablets,
					lastContact,
					entries,
					ingest,
					query,
					holdTime,
					scans,
					minor,
					major,
					datacHits,
					indexcHits,
					osLoad));

		}

		return tabletStats;

	}

	/**
	 * Test method
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(
			String[] args )
			throws Exception {
		String instanceName = GeowavePropertyReader.readProperty(GeowaveConstant.instanceName);
		String zooServers = GeowavePropertyReader.readProperty(GeowaveConstant.zooServers);
		String user = GeowavePropertyReader.readProperty(GeowaveConstant.user);
		String pass = GeowavePropertyReader.readProperty(GeowaveConstant.pass);
		TabletServerStats stats = new TabletServerStats(
				instanceName,
				zooServers,
				user,
				pass);
		List<TabletServerBean> sta = stats.getTabletStats();
		System.out.println(sta.size());
	}
}
