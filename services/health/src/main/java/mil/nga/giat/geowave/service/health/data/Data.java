package mil.nga.giat.geowave.service.health.data;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.util.TableInfoUtil;

public class Data {

	private static Instance instance;
	private static ServerConfigurationFactory config;
	private static AccumuloServerContext context;

	public static void main1(String[] args) throws Exception {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn = inst.getConnector("root", "password");

		Instance instt = HdfsZooInstance.getInstance();

		// GetMasterStats m=new GetMasterStats();
		// m.main(null);

		MasterClientService.Iface client = null;
		MasterMonitorInfo stats = null;
		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(instt));
			client = MasterClient.getConnectionWithRetry(context);
			stats = client.getMasterStats(Tracer.traceInfo(),
					context.rpcCreds());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {

			if (client != null)
				MasterClient.close(client);
		}
	}

	public static void main2(String[] args) throws Exception {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn = inst.getConnector("root", "password");
		System.out.println(conn.getInstance().getInstanceName());

		System.out.println(Monitor.getTotalTabletCount());

		Instance instt = HdfsZooInstance.getInstance();

		// GetMasterStats m=new GetMasterStats();
		// m.main(null);

		MasterClientService.Iface client = null;
		MasterMonitorInfo stats = null;
		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(inst));
			client = MasterClient.getConnectionWithRetry(context);
			stats = client.getMasterStats(Tracer.traceInfo(),
					context.rpcCreds());
			System.out.println("ss");
			System.out.println(client);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {

			if (client != null)
				MasterClient.close(client);
		}

		// run(stats);
		List<TabletServerStatus> a = stats.getTServerInfo();

		System.out.println(a.get(0));
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

	public static void main3(String[] args) throws Exception {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn = inst.getConnector("root", "password");

		SecurityUtil.serverLogin(SiteConfiguration.getInstance());

		ServerOpts opts = new ServerOpts();
		final String app = "monitor";
		opts.parseArgs(app, args);
		String hostname = opts.getAddress();

		Accumulo.setupLogging(app);
		VolumeManager fs = VolumeManagerImpl.get();
		instance = HdfsZooInstance.getInstance();

		config = new ServerConfigurationFactory(inst);
		context = new AccumuloServerContext(config);

		Accumulo.init(fs, config, app);
		Monitor monitor = new Monitor();

		Field[] f = monitor.getClass().getDeclaredFields();
		Field con = null;
		for (int i = 0; i < f.length; i++) {
			if (f[i].getName().equals("context")) {
				System.out.println(f[i].getName());
				con = f[i];
			}
		}

		con.setAccessible(true);
		con.set(monitor, context);

		monitor.fetchData();
		// monitor.run(hostname);
		System.out.println(monitor.getDataCacheHitRateOverTime().size());
	}

	public static void main(String[] args) throws Exception {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn = inst.getConnector("root", "password");
		
		
		SecurityUtil.serverLogin(SiteConfiguration.getInstance());

		ServerOpts opts = new ServerOpts();
		final String app = "monitor";
		opts.parseArgs(app, args);
		String hostname = opts.getAddress();

		Accumulo.setupLogging(app);
		VolumeManager fs = VolumeManagerImpl.get();
		
//		instance = HdfsZooInstance.getInstance();
		instance = conn.getInstance();

		config = new ServerConfigurationFactory(inst);
		context = new AccumuloServerContext(config);
		Accumulo.init(fs, config, app);
		Monitor monitor = new Monitor();
		DistributedTrace.enable(hostname, app, config.getConfiguration());
		
		Field[] f = monitor.getClass().getDeclaredFields();
		Field in = null;
		Field con = null;
		Field cont = null;
		for (int i = 0; i < f.length; i++) {
			if (f[i].getName().equals("instance")) {
				in = f[i];
			}
			if (f[i].getName().equals("config")) {
				con = f[i];
			}
			if (f[i].getName().equals("context")) {
				cont = f[i];
			}
		}

		in.setAccessible(true);
		in.set(monitor, inst);
		con.setAccessible(true);
		con.set(monitor, config);
		cont.setAccessible(true);
		cont.set(monitor, context);
		
		try {
			monitor.run(hostname);
		} finally {
			DistributedTrace.disable();
		}
		
//		monitor.fetchData();
		System.out.println(monitor.getDataCacheHitRateOverTime().size());
		
		while (true) {
			System.out.println(monitor.getDataCacheHitRateOverTime().size());
			Thread.sleep(1000);
			List<Pair<Long, Double>> li=monitor.getLoadOverTime();
			for (int i = 0; i < li.size(); i++) {
				Pair<Long, Double> p=li.get(i);
				System.out.print(p.getFirst()+" "+p.getSecond()+", ");
			}
			System.out.println();
		}
	}
}