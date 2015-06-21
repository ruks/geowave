package mil.nga.giat.geowave.service.healthimpl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.LogManager;

import mil.nga.giat.geowave.service.jaxbbean.Node;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.util.TableInfoUtil;

public class Main extends Thread {

	// private static Instance instance;
	// private static ServerConfigurationFactory config;
	// private static AccumuloServerContext context;

	public static List<Node> startStat() throws Exception {

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
			return null;
		} finally {

			if (client != null)
				MasterClient.close(client);
		}

		// run(stats);
		return tabletStat(stats);
	}

	public static void run(MasterMonitorInfo stats) throws Exception {

		out(0, "State: " + stats.state.name());
		out(0, "Goal State: " + stats.goalState.name());
		if (stats.serversShuttingDown != null
				&& stats.serversShuttingDown.size() > 0) {
			out(0, "Servers to shutdown");
			for (String server : stats.serversShuttingDown) {
				out(1, "%s", server);
			}
		}
		out(0, "Unassigned tablets: %d", stats.unassignedTablets);
		if (stats.badTServers != null && stats.badTServers.size() > 0) {
			out(0, "Bad servers");

			for (Entry<String, Byte> entry : stats.badTServers.entrySet()) {
				out(1, "%s: %d", entry.getKey(), (int) entry.getValue());
			}
		}
		out(0, "Dead tablet servers count: %s", stats.deadTabletServers.size());
		for (DeadServer dead : stats.deadTabletServers) {
			out(1, "Dead tablet server: %s", dead.server);
			out(2, "Last report: %s",
					new SimpleDateFormat().format(new Date(dead.lastStatus)));
			out(2, "Cause: %s", dead.status);
		}
		if (stats.tableMap != null && stats.tableMap.size() > 0) {
			out(0, "Tables");
			for (Entry<String, TableInfo> entry : stats.tableMap.entrySet()) {
				TableInfo v = entry.getValue();
				out(1, "%s", entry.getKey());
				out(2, "Records: %d", v.recs);
				out(2, "Records in Memory: %d", v.recsInMemory);
				out(2, "Tablets: %d", v.tablets);
				out(2, "Online Tablets: %d", v.onlineTablets);
				out(2, "Ingest Rate: %.2f", v.ingestRate);
				out(2, "Query Rate: %.2f", v.queryRate);
			}
		}
		if (stats.tServerInfo != null && stats.tServerInfo.size() > 0) {
			out(0, "Tablet Servers");
			long now = System.currentTimeMillis();
			for (TabletServerStatus server : stats.tServerInfo) {
				TableInfo summary = TableInfoUtil.summarizeTableStats(server);
				out(1, "Name: %s", server.name);
				out(2, "Ingest: %.2f", summary.ingestRate);
				out(2, "Last Contact: %s", server.lastContact);
				out(2, "OS Load Average: %.2f", server.osLoad);
				out(2, "Queries: %.2f", summary.queryRate);
				out(2, "Time Difference: %.1f",
						((now - server.lastContact) / 1000.));
				out(2, "Total Records: %d", summary.recs);
				out(2, "Lookups: %d", server.lookups);
				if (server.holdTime > 0)
					out(2, "Hold Time: %d", server.holdTime);
				if (server.tableMap != null && server.tableMap.size() > 0) {
					out(2, "Tables");
					for (Entry<String, TableInfo> status : server.tableMap
							.entrySet()) {
						TableInfo info = status.getValue();
						out(3, "Table: %s", status.getKey());
						out(4, "Tablets: %d", info.onlineTablets);
						out(4, "Records: %d", info.recs);
						out(4, "Records in Memory: %d", info.recsInMemory);
						out(4, "Ingest: %.2f", info.ingestRate);
						out(4, "Queries: %.2f", info.queryRate);
						out(4, "Major Compacting: %d", info.majors == null ? 0
								: info.majors.running);
						out(4, "Queued for Major Compaction: %d",
								info.majors == null ? 0 : info.majors.queued);
						out(4, "Minor Compacting: %d", info.minors == null ? 0
								: info.minors.running);
						out(4, "Queued for Minor Compaction: %d",
								info.minors == null ? 0 : info.minors.queued);
					}
				}
				out(2, "Recoveries: %d", server.logSorts.size());
				for (RecoveryStatus sort : server.logSorts) {
					out(3, "File: %s", sort.name);
					out(3, "Progress: %.2f%%", sort.progress * 100);
					out(3, "Time running: %s", sort.runtime / 1000.);
				}
			}
		}
	}

	private static void out(int indent, String string, Object... args) {
		for (int i = 0; i < indent; i++) {
			System.out.print(" ");
		}
		System.out.println(String.format(string, args));
	}

	private static List<Node> tabletStat(MasterMonitorInfo stats) {
		List<Node> nodes = new ArrayList<Node>();
		for (TabletServerStatus server : stats.tServerInfo) {
			TableInfo summary = TableInfoUtil.summarizeTableStats(server);
			System.out.println(summary.ingestRate);
			System.out.println(summary.scanRate);
			System.out.println(summary.ingestByteRate);
			System.out.println(server.getIndexCacheHits());
			System.out.println(server.dataCacheHits);
			nodes.add(new Node(1, 0.0, server.getName(), summary.ingestRate,
					summary.scanRate, summary.ingestByteRate, server
							.getIndexCacheHits()));
			System.out.println(server);
		}
		return nodes;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//while (true) {
				startStat();
				Thread.sleep(1000);
			//}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		Main m = new Main();
		m.start();
	}
}
