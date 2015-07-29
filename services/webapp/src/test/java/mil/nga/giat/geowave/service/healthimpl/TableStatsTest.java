package mil.nga.giat.geowave.service.healthimpl;

import java.io.File;
import java.io.IOException;

import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class TableStatsTest
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

	@Test
	public void testTable()
			throws Exception {

		TableStats stats = new TableStats(
				instanceName,
				zooServers,
				user,
				pass);
		for (TableBean table : stats.getTableStat()) {
			if (table.getTableName().equals(
					testTname)) {
				Assert.assertTrue(true);
				return;
			}
		}
		Assert.fail("table not found");
	}

	@Test
	public void testTableCount()
			throws Exception {
		TableStats stats = new TableStats(
				instanceName,
				zooServers,
				user,
				pass);
		Assert.assertTrue(stats.getTableStat().size() > 1);
	}

	@Test
	public void testTables()
			throws Exception {
		TableStats stats = new TableStats(
				instanceName,
				zooServers,
				user,
				pass);
		for (TableBean table : stats.getTableStat()) {
			if (table.getTableName().equals(
					testTname)) {
				// Assert.assertTrue();
				System.out.println(table.getEntries());
				return;
			}
		}
		Assert.fail("table not found");

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

}
