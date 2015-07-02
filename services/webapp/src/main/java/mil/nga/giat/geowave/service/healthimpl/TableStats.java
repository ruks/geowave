package mil.nga.giat.geowave.service.healthimpl;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

public class TableStats {
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
			System.out.println(stats.getTServerInfo());
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
		Map<String, TableInfo> map = stats.getTableMap();
		System.out.println(map.keySet());
		System.out.println(map.size());
		Set<String> tabs = map.keySet();

		Object[] arr = tabs.toArray();
		System.out.println(arr.length);

		for (int i = 0; i < arr.length; i++) {
			TableInfo info = map.get(arr[i]);
			System.out.println("Tablets "+info.getTablets());
			System.out.println("Offline Tables "+(info.getTablets()-info.getOnlineTablets()));
			System.out.println("Entries "+info.getRecs());
			System.out.println("Entries in mem "+info.getRecsInMemory());
			System.out.println("ingest "+info.getIngestRate());
			System.out.println("Entried Read "+info.getScanRate());
			System.out.println("Entried return "+info.getQueryRate());
			System.out.println("Hold time "+"-");
			System.out.println("Running Scans "+info.getScans().getRunning()+" "+info.getScans().getQueued());
			System.out.println("Minor Compaction "+info.getMinors().getRunning()+" "+info.getMinors().getQueued());
			System.out.println("Major Compaction "+info.getMajors().getRunning()+" "+info.getMajors().getQueued());
			
			System.out.println();
		}

	}

	public static void main(String[] args) throws Exception {

		startStat();
	}
}
