package mil.nga.giat.geowave.service.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;

import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class Client {

	public static void main1(String[] args) {
		// TODO Auto-generated method stub
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		try {
			Connector conn = inst.getConnector("root", "password");
			System.out.println(conn.whoami());

			TableOperations t = conn.tableOperations();
			// t.create("ruks");

			TreeSet<Text> splits = new TreeSet<Text>();
			for (int i = 0; i < 256; i++) {
				byte[] bytes = { (byte) i };
				splits.add(new Text(bytes));
			}

			// t.addSplits("ruks", splits);

			Collection<Text> c = t.listSplits("ruks");
			ArrayList<Text> txt = new ArrayList<Text>(c);
			System.out.println(c.size());
			System.out.println(txt.get(0));

			Key key = new Key(txt.get(0));
			AccumuloRowId id = new AccumuloRowId(key);

		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// ListInstances li=new ListInstances();

		// AccumuloRowId id = new AccumuloRowId(key);
		// System.out.println(id.getInsertionId());
	}

	public static void addData() {

	}

	public static void createRuks(Connector conn) throws Exception {
		Text rowID = new Text("row1");
		Text colFam = new Text("myColFam");
		Text colQual = new Text("myColQual");
		ColumnVisibility colVis = new ColumnVisibility("public");
		long timestamp = System.currentTimeMillis();

		Value value = new Value("myValue".getBytes());

		Mutation mutation = new Mutation(rowID);
		mutation.put(colFam, colQual, colVis, timestamp, value);

		long memBuf = 1000000L; // bytes to store before sending a batch
		long timeout = 1000L; // milliseconds to wait before sending
		int numThreads = 10;

		BatchWriter writer = conn.createBatchWriter("table", memBuf, timeout,
				numThreads);

		writer.addMutation(mutation);

		writer.close();

	}

	public static void main(String[] args) throws Exception {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		Connector conn = inst.getConnector("root", "password");
		AccumuloUtils au = new AccumuloUtils();

		BasicAccumuloOperations ba = new BasicAccumuloOperations(conn);
		ba.createTable("ruks1");
		// Index i=new
		// au.setSplitsByNumSplits(conn, "ruks", index, 5);

	}
}
