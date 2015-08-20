package mil.nga.giat.geowave.service.healthimpl;

import static org.apache.accumulo.core.util.NumUtil.bigNumberForQuantity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.celltypes.CompactionsType;
import org.apache.accumulo.monitor.util.celltypes.DurationType;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.util.TableInfoUtil;

/**
 * Class to find the statistic data of a table
 * 
 * @author rukshan
 * 
 */
public class TableStats
{
	private MasterMonitorInfo masterMonitorInfo = null;
	private List<TableBean> tableStats;
	private Connector conn;

	/**
	 * TableStats constructor
	 * 
	 * @param instanceName
	 * @param zooServers
	 * @param user
	 * @param pass
	 */
	public TableStats(
			String instanceName,
			String zooServers,
			String user,
			String pass ) {

		// create zookeeper instance
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		AuthenticationToken authToken = new PasswordToken(
				pass);
		try {
			// getting the connection from the instance
			this.conn = inst.getConnector(
					user,
					authToken);
		}
		catch (AccumuloException | AccumuloSecurityException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1.getMessage());
		}

		MasterClientService.Iface client = null;
		try {
			// getting contx
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(
							inst));
			client = MasterClient.getConnectionWithRetry(context);
			// getting masterMonitorInfo
			masterMonitorInfo = client.getMasterStats(
					Tracer.traceInfo(),
					context.rpcCreds());
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			// return;
		}
		finally {

			if (client != null) MasterClient.close(client);
		}

	}

	public List<TableBean> getTableStat() {
		Map<String, TableInfo> map = masterMonitorInfo.tableMap;
		tableStats = new ArrayList<TableBean>();

		String tableName;
		String state;
		int tablets;
		int offlineTablets;
		String entries;
		String entriesInMemory;
		String ingest;
		String entriesRead;
		String entriesReturned;
		String holdTime;
		String majorunningScans;
		String minorCompactions;
		String majorCompactions;

		// getting table operation
		TableOperations t = conn.tableOperations();
		// getting reverser table to id map
		Map<String, String> idm = reverseMap(t.tableIdMap());

		// getting table stats
		Map<String, Double> compactingByTable = TableInfoUtil.summarizeTableStats(Monitor.getMmi());
		// TableManager tableManager = TableManager.getInstance();

		// iterate through all the tableinfo
		for (Entry<String, TableInfo> entry : map.entrySet()) {
			TableInfo info = entry.getValue();

			tableName = idm.get(entry.getKey());
			// state = new TableStateType().format(tableManager
			// .getTableState(entry.getKey()));
			state = "ONLINE";
			tablets = info.getTablets(); // set the no of tablets
			offlineTablets = info.getTablets() - info.getOnlineTablets();// set
																			// the
																			// offline
																			// tablets
			entries = bigNumberForQuantity(info.getRecs()); // format and set no
															// of entries
			entriesInMemory = bigNumberForQuantity(info.getRecsInMemory()); // format
																			// and
																			// set
																			// no
																			// of
																			// entries
																			// in
																			// memory
			ingest = bigNumberForQuantity(info.getIngestRate()); // set the
																	// ingest
																	// rate and
																	// format
			entriesRead = bigNumberForQuantity(info.getScanRate()); // set the
																	// scane
																	// rate and
																	// format
			entriesReturned = bigNumberForQuantity(info.getQueryRate()); // set
																			// and
																			// format
																			// the
																			// return
																			// entries
			Double hTime = compactingByTable.get(entry.getKey());
			if (hTime == null) hTime = new Double(
					0.);
			holdTime = new DurationType(
					0l,
					0l).format(hTime.longValue()); // set and format hold time
			majorunningScans = new CompactionsType(
					"scans").format(info); // set the scans
			minorCompactions = new CompactionsType(
					"minor").format(info); // set the minor
			majorCompactions = new CompactionsType(
					"major").format(info); // set the majors

			// adding all the data to object
			tableStats.add(new TableBean(
					tableName,
					state,
					tablets,
					offlineTablets,
					entries,
					entriesInMemory,
					ingest,
					entriesRead,
					entriesReturned,
					holdTime,
					majorunningScans,
					minorCompactions,
					majorCompactions));
		}

		return tableStats;
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

		TableStats stats = new TableStats(
				instanceName,
				zooServers,
				user,
				pass);
		List<TableBean> sta = stats.getTableStat();
		for (int i = 0; i < sta.size(); i++) {
			System.out.println(sta.get(
					i).getTableName());
			String l = sta.get(
					i).getState();
			System.out.println(l);
		}

	}

	/**
	 * reverse map
	 * 
	 * @param map
	 * @return reversed map
	 */
	public Map<String, String> reverseMap(
			Map<String, String> map ) {
		Map<String, String> idm = new TreeMap<String, String>(); // create new
																	// map
		for (Entry<String, String> entry : map.entrySet()) {
			String v = entry.getValue();
			idm.put(
					v,
					entry.getKey()); // interchange key and value, and add to
										// map
		}
		return idm;
	}
}
