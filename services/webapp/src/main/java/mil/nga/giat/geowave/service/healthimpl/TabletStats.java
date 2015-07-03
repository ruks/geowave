package mil.nga.giat.geowave.service.healthimpl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

public class TabletStats {
	public static void startStat() throws Exception {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		@SuppressWarnings("deprecation")
		Connector conn = inst.getConnector("root", "password");
		System.out.println(conn.getInstance().getInstanceName());

		MasterClientService.Iface client = null;
		MasterMonitorInfo stats = null;
		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(inst));
			client = MasterClient.getConnectionWithRetry(context);
			stats = client.getMasterStats(Tracer.traceInfo(),
					context.rpcCreds());
			// System.out.println(stats.getTServerInfo());
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		} finally {

			if (client != null)
				MasterClient.close(client);
		}

		tabletStat(stats);
	}

	private static void tabletStat(MasterMonitorInfo stats) {
		List<TabletServerStatus> tabs = stats.getTServerInfo();
		System.out.println();
		// System.out.println(tabs);

		for (int i = 0; i < tabs.size(); i++) {
			TabletServerStatus sta = tabs.get(i);
			System.out.println("name " + sta.getName());
			System.out.println("time " + sta.getLastContact());

			Map<String, TableInfo> map = sta.getTableMap();
			Set<String> key = map.keySet();
			Object[] arr = key.toArray();
			for (int j = 0; j < arr.length; j++) {
				TableInfo info=map.get(arr[j]);
				System.out.println(info.getIngestRate());
				System.out.println(info.getRecs());
				System.out.println(info.getQueryRate());
				System.out.println(info.getScans());
				System.out.println(info.getMinors());
				System.out.println(info.getMajors());
			}
			
			System.out.println("time " + sta.getIndexCacheHits());
			System.out.println("time " + sta.getDataCacheHits());
			System.out.println("time " + sta.getOsLoad());
			System.out.println();
		}

	}

	public static void main(String[] args) throws Exception {

		startStat();
	}
}
