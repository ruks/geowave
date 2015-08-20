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

/**
 * Class to fetch tablet related stat data
 * 
 * @author rukshan
 * 
 */
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
	TableOperations ops;

	/**
	 * Constructor which create instance
	 * 
	 * @param instanceName
	 * @param zooServers
	 * @param user
	 * @param pass
	 */
	public TabletStat(
			String instanceName,
			String zooServers,
			String user,
			String pass ) {

		this.instanceName = instanceName;
		this.zooServers = zooServers;
		this.user = user;
		this.pass = pass;

		// getting the instance
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		Instance accInstance = inst;

		AuthenticationToken authToken = new PasswordToken(
				pass);

		Connector connector;
		try {
			// get the connector
			connector = accInstance.getConnector(
					user,
					authToken);
			// get the tableOperation
			ops = connector.tableOperations();
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// return;
		}

		ClientConfiguration clientConf = ClientConfiguration.loadDefault();
		// get the client context
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

	/**
	 * method to find standard deviation
	 * 
	 * @param elapsed
	 * @param num
	 * @param sumDev
	 * @return
	 */
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

	/**
	 * find the tablet stat data
	 * 
	 * @param tableName
	 * @return
	 */
	public List<TabletBean> getTabletStats(
			String tableName ) {

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

		// get the table id of the table name
		String tid = ops.tableIdMap().get(
				tableName);

		try {
			// getting the tablet server infomation
			List<TabletStats> tsStats = new ArrayList<TabletStats>();

			TabletServerStats stats;
			try {
				// create tabletServerStats instance
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

			// getting tablet server stats
			List<TabletServerBean> sta = stats.getTabletStats();
			TabletClientService.Client client;

			// iterate over all the tablet server info
			for (TabletServerBean ts : sta) {

				// find the address of the tablet server
				address = HostAndPort.fromString(ts.getName());

				// create client to get tablet stat
				client = ThriftUtil.getClient(
						new TabletClientService.Client.Factory(),
						address,
						ctx);

				// getting tablet stats of the tablet in tablet server
				List<TabletStats> tss = client.getTabletStats(
						Tracer.traceInfo(),
						context.rpcCreds(),
						tid);
				tsStats.addAll(tss); // adding all the tablet stat to list
			}

			// getting table map
			Map<String, String> tcon = ops.tableIdMap();

			// get the table name form the table id
			for (Entry<String, String> entry : tcon.entrySet()) {
				String row = entry.getValue();
				if (tid.equals(row)) {
					table = entry.getKey();
					break;
				}
			}

			// iterate over tablet info
			for (TabletStats info : tsStats) {

				if (info.extent == null) {
					continue;
				}

				// get the tablet key extent
				KeyExtent extent = new KeyExtent(
						info.extent);
				tabletUUID = extent.getUUID().toString(); // get the tablet uuid

				// find the tablet name
				MessageDigest digester = MessageDigest.getInstance("MD5");
				if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
					digester.update(
							extent.getEndRow().getBytes(),
							0,
							extent.getEndRow().getLength());
				}
				String obscuredExtent = Base64.encodeBase64String(digester.digest());

				table = tableName; // set the table name
				tablet = obscuredExtent; // set the tablet name

				entries = new NumberType<Long>().format(info.numEntries); // set
																			// and
																			// format
																			// entries
																			// of
																			// the
																			// tablet
				ingest = new NumberType<Long>().format(info.ingestRate); // set
																			// and
																			// format
																			// ingest
																			// rate
																			// of
																			// the
																			// tablet
				query = new NumberType<Long>().format(info.queryRate); // set
																		// and
																		// format
																		// query
																		// rate
																		// of
																		// the
																		// tablet

				miAvg = new SecondType().format(info.minors.num != 0 ? info.minors.elapsed / info.minors.num : null);// find
																														// and
																														// set
																														// the
																														// minor
																														// average
				mistd = new SecondType().format(stddev(
						info.minors.elapsed,
						info.minors.num,
						info.minors.sumDev)); // find and set the minor std
				miAvges = new NumberType<Double>().format(info.minors.elapsed != 0 ? info.minors.count / info.minors.elapsed : null); // find
																																		// and
																																		// set
																																		// the
																																		// minor
																																		// average
																																		// e/s
				maAvg = new SecondType().format(info.majors.num != 0 ? info.majors.elapsed / info.majors.num : null); // find
																														// and
																														// set
																														// the
																														// major
																														// average
				mastd = new SecondType().format(stddev(
						info.majors.elapsed,
						info.majors.num,
						info.majors.sumDev)); // find and set the major std
				maAvges = new NumberType<Double>().format(info.majors.elapsed != 0 ? info.majors.count / info.majors.elapsed : null); // find
																																		// and
																																		// set
																																		// the
																																		// major
																																		// average
																																		// e/s

				// add the table info to the object
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
						maAvges,
						tabletUUID));
			}

		}
		catch (NoSuchAlgorithmException | TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		return stat;
	}

	/**
	 * class to format numeric typess
	 * 
	 * @author rukshan
	 * 
	 */
	static class SecondType extends
			NumberType<Double>
	{

		private static final long serialVersionUID = 1L;

		@Override
		public String format(
				Object obj ) {
			if (obj == null) return "&mdash;";
			return Duration.format((long) (1000.0 * (Double) obj));
		}
	}

	/**
	 * Test method
	 * 
	 * @param args
	 */
	public static void main(
			String[] args ) {
		String instanceName = GeowavePropertyReader.readProperty(GeowaveConstant.instanceName);
		String zooServers = GeowavePropertyReader.readProperty(GeowaveConstant.zooServers);
		String user = GeowavePropertyReader.readProperty(GeowaveConstant.user);
		String pass = GeowavePropertyReader.readProperty(GeowaveConstant.pass);
		TabletStat t = new TabletStat(
				instanceName,
				zooServers,
				user,
				pass);
		System.out.println(t.getTabletStats(
				"namespace_SPATIAL_VECTOR_IDX").get(
				0).getTabletUUID());
		// System.out.println(t.getTabletStats("!0").get(0).getTable());
	}
}
