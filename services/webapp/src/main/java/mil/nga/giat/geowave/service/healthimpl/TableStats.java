package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

public class TableStats
{
	private MasterMonitorInfo masterMonitorInfo = null;
	private List<TableBean> tableStats;

	public TableStats()
			throws Exception {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		@SuppressWarnings("deprecation")
		Connector conn = inst.getConnector(
				"root",
				"password");
		System.out.println(conn.getInstance().getInstanceName());

		MasterClientService.Iface client = null;
		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(
							inst));
			client = MasterClient.getConnectionWithRetry(context);
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

	public List<TableBean> getTableStat() {
		Map<String, TableInfo> map = masterMonitorInfo.getTableMap();
		tableStats = new ArrayList<TableBean>();

		System.out.println(map.keySet());
		Set<String> tabs = map.keySet();

		Object[] arr = tabs.toArray();

		String tableName;
		String state;
		int tablets;
		int offlineTablets;
		long entries;
		long entriesInMemory;
		double ingest;
		double entriesRead;
		double entriesReturned;
		long holdTime;
		Compacting majorunningScans;
		Compacting minorCompactions;
		Compacting majorCompactions;

		for (int i = 0; i < arr.length; i++) {
			TableInfo info = map.get(arr[i]);

			tableName = info.toString();
			state = "ONLINE";
			tablets = info.getTablets();
			offlineTablets = info.getTablets() - info.getOnlineTablets();
			entries = info.getRecs();
			entriesInMemory = info.getRecsInMemory();
			ingest = info.getIngestRate();
			entriesRead = info.getScanRate();
			entriesReturned = info.getQueryRate();
			holdTime = 0;
			majorunningScans = info.getScans();
			minorCompactions = info.getMinors();
			majorCompactions = info.getMajors();

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

	public static void main(
			String[] args )
			throws Exception {
		TableStats stats = new TableStats();
		List<TableBean> sta = stats.getTableStat();
		for (int i = 0; i < sta.size(); i++) {
			System.out.println(sta.get(
					i).getTableName());
		}
	}
}
