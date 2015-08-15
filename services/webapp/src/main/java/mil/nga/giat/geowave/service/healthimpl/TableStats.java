package mil.nga.giat.geowave.service.healthimpl;

import static org.apache.accumulo.core.util.NumUtil.bigNumberForQuantity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import mil.nga.giat.geowave.service.jaxbbean.TableBean;

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
import org.apache.accumulo.monitor.util.celltypes.TableStateType;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;

public class TableStats {
	private MasterMonitorInfo masterMonitorInfo = null;
	private List<TableBean> tableStats;
	private Connector conn;

	public TableStats(String instanceName, String zooServers, String user,
			String pass) throws Exception {

		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		AuthenticationToken authToken = new PasswordToken(pass);
		this.conn = inst.getConnector(user, authToken);

		MasterClientService.Iface client = null;
		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(inst));
			client = MasterClient.getConnectionWithRetry(context);
			masterMonitorInfo = client.getMasterStats(Tracer.traceInfo(),
					context.rpcCreds());
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		} finally {

			if (client != null)
				MasterClient.close(client);
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

		TableOperations t = conn.tableOperations();
		Map<String, String> idm = reverseMap(t.tableIdMap());
		Map<String, Double> compactingByTable = TableInfoUtil
				.summarizeTableStats(Monitor.getMmi());
//		TableManager tableManager = TableManager.getInstance();

		for (Entry<String, TableInfo> entry : map.entrySet()) {
			TableInfo info = entry.getValue();

			tableName = idm.get(entry.getKey());
//			state = new TableStateType().format(tableManager
//					.getTableState(entry.getKey()));
			state="ONLINE";
			tablets = info.getTablets();
			offlineTablets = info.getTablets() - info.getOnlineTablets();
			entries = bigNumberForQuantity(info.getRecs());
			entriesInMemory = bigNumberForQuantity(info.getRecsInMemory());
			ingest = bigNumberForQuantity(info.getIngestRate());
			entriesRead = bigNumberForQuantity(info.getScanRate());
			entriesReturned = bigNumberForQuantity(info.getQueryRate());
			Double hTime = compactingByTable.get(entry.getKey());
			if (hTime == null)
				hTime = new Double(0.);
			holdTime = new DurationType(0l, 0l).format(hTime.longValue());
			majorunningScans = new CompactionsType("scans").format(info);
			minorCompactions = new CompactionsType("minor").format(info);
			majorCompactions = new CompactionsType("major").format(info);

			tableStats.add(new TableBean(tableName, state, tablets,
					offlineTablets, entries, entriesInMemory, ingest,
					entriesRead, entriesReturned, holdTime, majorunningScans,
					minorCompactions, majorCompactions));
		}

		return tableStats;
	}

	public static void main(String[] args) throws Exception {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		String user = "root";
		String pass = "password";

		TableStats stats = new TableStats(instanceName, zooServers, user, pass);
		List<TableBean> sta = stats.getTableStat();
		for (int i = 0; i < sta.size(); i++) {
			System.out.println(sta.get(i).getTableName());
			String l = sta.get(i).getState();
			System.out.println(l);
		}

	}

	public Map<String, String> reverseMap(Map<String, String> map) {
		Map<String, String> idm = new TreeMap<String, String>();
		for (Entry<String, String> entry : map.entrySet()) {
			String v = entry.getValue();
			idm.put(v, entry.getKey());
		}
		return idm;
	}
}
