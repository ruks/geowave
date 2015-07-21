package mil.nga.giat.geowave.service.healthimpl;

import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableStatsTest
{
	static String testTname = "sampleTable";
	static TableOperations operation;

	@BeforeClass
	public static void ingest() {

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

			Connector conn = getInstance();
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
	public void testTableCount()
			throws Exception {
		TableStats stat = new TableStats();
		// Assert.assertEquals(2, stat.getTableStat().size());
		Assert.assertTrue(stat.getTableStat().size() >= 2);
	}

	@Test
	public void testTables()
			throws Exception {

		TableStats stat = new TableStats();
		TableBean tableStat = stat.getTableStat().get(
				0);
		Assert.assertNotNull(tableStat);
		Assert.assertNotNull(tableStat.getTablets());
		Assert.assertNotNull(tableStat.getEntries());
	}

	@Test
	public void testTableEntries()
			throws Exception {
		TableStats stat = new TableStats();
		Assert.assertTrue(operation.exists(testTname));
		for (TableBean t : stat.getTableStat()) {
			if (t.equals(testTname)) {
				Assert.assertEquals(
						1,
						t.getEntries());
				return;
			}
		}
	}

	@AfterClass
	public static void deleteTable()
			throws Exception {
		operation.delete(testTname);
	}

	public static Connector getInstance() {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		Connector conn;
		try {
			AuthenticationToken authToken = new PasswordToken(
					"password");
			conn = inst.getConnector(
					"root",
					authToken);
			return conn;
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
