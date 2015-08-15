package mil.nga.giat.geowave.service.healthimpl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.service.jaxbbean.TabletBean;
import mil.nga.giat.geowave.service.jaxbbean.TabletServerBean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.thrift.TException;
import org.apache.accumulo.core.util.Duration;
import com.google.common.net.HostAndPort;

public class TabletStat {

	AccumuloServerContext context;
	HostAndPort address;
	// MasterClientService.Iface c;
	ClientContext ctx;

	String instanceName;
	String zooServers;
	String user;
	String pass;
	TableOperations ops;

	public TabletStat(String instanceName, String zooServers, String user,
			String pass) {

		this.instanceName = instanceName;
		this.zooServers = zooServers;
		this.user = user;
		this.pass = pass;

		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Instance accInstance = inst;

		AuthenticationToken authToken = new PasswordToken(pass);

		Connector connector;
		try {
			connector = accInstance.getConnector(user, authToken);
			ops = connector.tableOperations();
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// return;
		}

		ClientConfiguration clientConf = ClientConfiguration.loadDefault();
		ctx = new ClientContext(accInstance, new Credentials(user,
				new PasswordToken(pass)), clientConf);

		ServerConfigurationFactory config;

		config = new ServerConfigurationFactory(inst);
		context = new AccumuloServerContext(config);

	}

	private static double stddev(double elapsed, double num, double sumDev) {
		if (num != 0) {
			double average = elapsed / num;
			return Math.sqrt((sumDev / num) - (average * average));
		}
		return 0;
	}

	public List<TabletBean> getTabletStats(String tableName) {

		String table = "";
		String tablet;
		String entries;
		String ingest;
		String query;
		String miAvg;
		String mistd;
		String miAvges;
		String maAvg;
		String mastd;
		String maAvges;
		String tabletUUID;

		List<TabletBean> stat = new ArrayList<TabletBean>();
		String tid = ops.tableIdMap().get(tableName);

		try {

			List<TabletStats> tsStats = new ArrayList<TabletStats>();

			TabletServerStats stats;
			try {
				stats = new TabletServerStats(this.instanceName,
						this.zooServers, this.user, this.pass);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}

			List<TabletServerBean> sta = stats.getTabletStats();
			TabletClientService.Client client;

			for (TabletServerBean ts : sta) {

				address = HostAndPort.fromString(ts.getName());

				client = ThriftUtil.getClient(
						new TabletClientService.Client.Factory(), address, ctx);

				List<TabletStats> tss = client.getTabletStats(
						Tracer.traceInfo(), context.rpcCreds(), tid);
				tsStats.addAll(tss);
			}

			Map<String, String> tcon = ops.tableIdMap();
			for (Entry<String, String> entry : tcon.entrySet()) {
				String row = entry.getValue();
				if (tid.equals(row)) {
					table = entry.getKey();
					break;
				}
			}

			for (TabletStats info : tsStats) {

				if (info.extent == null) {
					continue;
				}

				KeyExtent extent = new KeyExtent(info.extent);
				tabletUUID = extent.getUUID().toString();

				MessageDigest digester = MessageDigest.getInstance("MD5");
				if (extent.getEndRow() != null
						&& extent.getEndRow().getLength() > 0) {
					digester.update(extent.getEndRow().getBytes(), 0, extent
							.getEndRow().getLength());
				}
				String obscuredExtent = Base64.encodeBase64String(digester
						.digest());

				table = tableName;
				tablet = obscuredExtent;

				entries= new NumberType<Long>().format(info.numEntries);
				ingest = new NumberType<Long>().format(info.ingestRate);
				query = new NumberType<Long>().format(info.queryRate);
				
				miAvg =new SecondType().format(info.minors.num != 0 ? info.minors.elapsed / info.minors.num : null);
				mistd=new SecondType().format(stddev(info.minors.elapsed, info.minors.num, info.minors.sumDev));
				miAvges=new NumberType<Double>().format(info.minors.elapsed != 0 ? info.minors.count / info.minors.elapsed : null);
				maAvg=new SecondType().format(info.majors.num != 0 ? info.majors.elapsed / info.majors.num : null);
				mastd=new SecondType().format(stddev(info.majors.elapsed, info.majors.num, info.majors.sumDev));
				maAvges=new NumberType<Double>().format(info.majors.elapsed != 0 ? info.majors.count / info.majors.elapsed : null);
			      
				stat.add(new TabletBean(table, tablet, entries, ingest, query,
						miAvg, mistd, miAvges, maAvg, mastd, maAvges,tabletUUID));
			}

		} catch (NoSuchAlgorithmException | TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		return stat;
	}

	static class SecondType extends NumberType<Double> {

	    private static final long serialVersionUID = 1L;

	    @Override
	    public String format(Object obj) {
	      if (obj == null)
	        return "&mdash;";
	      return Duration.format((long) (1000.0 * (Double) obj));
	    }
	}

	    
	public static void main(String[] args) {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		String user = "root";
		String pass = "password";
		TabletStat t = new TabletStat(instanceName, zooServers, user, pass);
		System.out.println(t.getTabletStats("ruks_SPATIAL_VECTOR_IDX").get(0).getTabletUUID());
//		System.out.println(t.getTabletStats("!0").get(0).getTable());
	}
}
