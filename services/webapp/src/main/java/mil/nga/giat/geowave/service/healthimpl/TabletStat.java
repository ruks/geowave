package mil.nga.giat.geowave.service.healthimpl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.service.jaxbbean.TabletBean;
import mil.nga.giat.geowave.service.jaxbbean.TabletServerBean;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.thrift.TException;

import com.google.common.net.HostAndPort;

public class TabletStat
{

	AccumuloServerContext context;
	HostAndPort address;
	// MasterClientService.Iface c;
	ClientContext ctx;

	String instanceName;
	String zooServers;
	String user;
	String pass;

	public TabletStat(
			String instanceName,
			String zooServers,
			String user,
			String pass ) {

		this.instanceName = instanceName;
		this.zooServers = zooServers;
		this.user = user;
		this.pass = pass;

		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		Instance accInstance = inst;
		ClientConfiguration clientConf = ClientConfiguration.loadDefault();
		ctx = new ClientContext(
				accInstance,
				new Credentials(
						user,
						new PasswordToken(
								pass)),
				clientConf);

		ServerConfigurationFactory config;

		config = new ServerConfigurationFactory(
				inst);
		context = new AccumuloServerContext(
				config);

	}

	private static double stddev(
			double elapsed,
			double num,
			double sumDev ) {
		if (num != 0) {
			double average = elapsed / num;
			return Math.sqrt((sumDev / num) - (average * average));
		}
		return 0;
	}

	public List<TabletBean> getTabletStats(
			String tid ) {

		String table;
		String tablet;
		long entries;
		double ingest;
		double query;
		double miAvg = 0;
		double mistd = 0;
		double miAvges = 0;
		double maAvg = 0;
		double mastd = 0;
		double maAvges = 0;

		List<TabletBean> stat = new ArrayList<TabletBean>();

		try {

			List<TabletStats> tsStats = new ArrayList<TabletStats>();

			TabletServerStats stats;
			try {
				stats = new TabletServerStats(
						this.instanceName,
						this.zooServers,
						this.user,
						this.pass);
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}

			List<TabletServerBean> sta = stats.getTabletStats();
			TabletClientService.Client client;

			for (TabletServerBean ts : sta) {

				address = HostAndPort.fromString(ts.getName());

				client = ThriftUtil.getClient(
						new TabletClientService.Client.Factory(),
						address,
						ctx);

				List<TabletStats> tss = client.getTabletStats(
						Tracer.traceInfo(),
						context.rpcCreds(),
						tid);
				tsStats.addAll(tss);
			}

			for (TabletStats info : tsStats) {

				if (info.extent == null) {
					continue;
				}

				KeyExtent extent = new KeyExtent(
						info.extent);
				String tableId = extent.getTableId().toString();

				MessageDigest digester = MessageDigest.getInstance("MD5");
				if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
					digester.update(
							extent.getEndRow().getBytes(),
							0,
							extent.getEndRow().getLength());
				}
				String obscuredExtent = Base64.encodeBase64String(digester.digest());

				table = tableId;
				tablet = obscuredExtent;

				entries = info.numEntries;
				ingest = info.ingestRate;
				query = info.queryRate;

				miAvg = info.minors.num != 0 ? info.minors.elapsed / info.minors.num : 0;
				mistd = stddev(
						info.minors.elapsed,
						info.minors.num,
						info.minors.sumDev);
				miAvges = info.minors.elapsed != 0 ? info.minors.count / info.minors.elapsed : 0;
				maAvg = info.majors.num != 0 ? info.majors.elapsed / info.majors.num : 0;
				mastd = stddev(
						info.majors.elapsed,
						info.majors.num,
						info.majors.sumDev);
				maAvges = info.majors.elapsed != 0 ? info.majors.count / info.majors.elapsed : 0;
				stat.add(new TabletBean(
						table,
						tablet,
						entries,
						ingest,
						query,
						miAvg,
						mistd,
						miAvges,
						maAvg,
						mastd,
						maAvges));
			}

		}
		catch (NoSuchAlgorithmException | TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		return stat;
	}

	public static void main(
			String[] args ) {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		String user = "root";
		String pass = "password";
		TabletStat t = new TabletStat(
				instanceName,
				zooServers,
				user,
				pass);
		System.out.println(t.getTabletStats(
				"!0").size());
	}
}
