package mil.nga.giat.geowave.service.healthimpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class TabletStatsTest
{
	static String testTname = "sampleTable";
	static TableOperations operation;
	static Connector conn;

	static String instanceName = "geowaveTest";
	static String zooServers = "127.0.0.1";
	static String user = "root";
	static String pass = "password";
	static MiniAccumuloClusterImpl accumulo;

	@BeforeClass
	public static void ingest() {

		Logger.getRootLogger().setLevel(
				Level.WARN);
		try {

			Text rowID = new Text(
					"row1");
			Text colFam = new Text(
					"myColFam");
			Text colQual = new Text(
					"myColQual");
			ColumnVisibility colVis = new ColumnVisibility(
					"public");
			long timestamp = System.currentTimeMillis();

			Value value = new Value(
					"myValue".getBytes());

			Mutation mutation = new Mutation(
					rowID);
			mutation.put(
					colFam,
					colQual,
					colVis,
					timestamp,
					value);

			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxMemory(10000000L); // bytes available to batchwriter
											// for buffering mutations

			conn = getInstance();
			operation = conn.tableOperations();
			operation.create(testTname);

			SortedSet<Text> keys = new TreeSet<Text>();
			keys.add(new Text(
					"r"));

			operation.addSplits(
					testTname,
					keys);

			BatchWriter writer = conn.createBatchWriter(
					testTname,
					config);
			writer.addMutation(mutation);
			writer.close();

		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void stopAccumulo()
			throws Exception {
		accumulo.stop();
	}

	public static Connector getInstance()
			throws IOException,
			InterruptedException,
			AccumuloException,
			AccumuloSecurityException {

		File tempDirectory = Files.createTempDir();

		MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(
				tempDirectory,
				pass).setNumTservers(
				2).setInstanceName(
				instanceName).setZooKeeperPort(
				2181);

		accumulo = new MiniAccumuloClusterImpl(
				miniAccumuloConfig);

		accumulo.start();

		Instance instance = new ZooKeeperInstance(
				accumulo.getInstanceName(),
				accumulo.getZooKeepers());
		Connector conn = instance.getConnector(
				user,
				new PasswordToken(
						pass));
		return conn;
	}

	@Test
	public void testTabletAfteraddSplit()
			throws Exception {

		TabletStat stats = new TabletStat(
				instanceName,
				zooServers,
				user,
				pass);
		String tid = operation.tableIdMap().get(
				testTname);

		SortedSet<Text> splitkeys = new TreeSet<Text>();
		splitkeys.add(new Text(
				"r"));
		splitkeys.add(new Text(
				"c"));

		operation.addSplits(
				testTname,
				splitkeys);

		int tabs = 0;
		for (TabletBean tablet : stats.getTabletStats(tid)) {
			if (tablet.getTable().equals(
					tid)) {
				tabs++;
			}
		}
		Assert.assertEquals(
				tabs,
				3);

	}

	// @Test
	public void testTablet()
			throws Exception {

		TabletStat stats = new TabletStat(
				instanceName,
				zooServers,
				user,
				pass);
		String tid = operation.tableIdMap().get(
				testTname);

		int tabs = 0;
		for (TabletBean tablet : stats.getTabletStats(tid)) {
			if (tablet.getTable().equals(
					tid)) {
				tabs++;
			}
		}
		Assert.assertEquals(
				tabs,
				1);
	}

	public List<String> getMasterStat(
			String tid )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			InterruptedException {

		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		ArrayList<String> ts = new ArrayList<String>();
		MasterClientService.Iface client = null;
		if (tid == null) {
			return ts;
		}

		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(
							inst));
			client = MasterClient.getConnectionWithRetry(context);
			MasterMonitorInfo masterMonitorInfo = client.getMasterStats(
					Tracer.traceInfo(),
					context.rpcCreds());
			for (TabletServerStatus tss : masterMonitorInfo.getTServerInfo()) {
				if (tss.tableMap.get(tid) != null) {
					ts.add(tss.getName());
				}
			}
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			return ts;
		}
		finally {

			if (client != null) MasterClient.close(client);
		}

		return ts;
	}
}
