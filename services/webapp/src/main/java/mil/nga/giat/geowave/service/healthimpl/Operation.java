package mil.nga.giat.geowave.service.healthimpl;

import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class Operation {
	@SuppressWarnings("deprecation")
	public static void main1(String[] args) {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn;
		try {
			conn = inst.getConnector("root", "password");
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		TableOperations op = conn.tableOperations();
		try {
			System.out.println(op.getProperties("accumulo.metadata"));
		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// createTable(op);
		// addSplit(op);
		// addData(conn);
		// if(true) return;
		// try {
		// Range r = new Range("~");
		// Authorizations auths = new Authorizations("root");
		// Scanner scan = conn.createScanner("accumulo.metadata", auths);
		// scan.setRange(r);
		//
		// System.out.println(scan.getBatchSize());
		// for (Entry<Key, Value> entry : scan) {
		// Text row = entry.getKey().getRow();
		// Value value = entry.getValue();
		// }
		// } catch (TableNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// Collection<Text> list = op.listSplits("t1");
		// System.out.println(list.size());
		//
		// ArrayList<Text> ll = new ArrayList<Text>(list);
		// System.out.println(ll.get(0));

		try {

//			Range r = new Range();
			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner("accumulo.metadata", auths);

			for (Entry<Key, Value> entry : scan) {
				Key k = entry.getKey();
				System.out.println(k);
				try {
					AccumuloRowId id = new AccumuloRowId(k);
					id.getInsertionId();
					System.out.println(k);
				} catch (Exception e) {
					// System.out.println(e.getMessage());
				}

			}
			System.out.println("finished");

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static void createTable(TableOperations op) {
		try {
			op.create("t1");

		} catch (AccumuloException | AccumuloSecurityException
				| TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void addSplit(TableOperations op) {
		try {
			SortedSet<Text> set = new TreeSet<Text>();
			set.add(new Text("r"));
			op.addSplits("t1", set);
		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void addData(Connector conn) {
		Text rowID1 = new Text("q");
		Text rowID2 = new Text("e");
		Text rowID3 = new Text("r");
		Text rowID4 = new Text("t");
		Text rowID5 = new Text("y");
		Text rowID6 = new Text("i");
		Text rowID7 = new Text("b");
		Text rowID8 = new Text("c");

		Text colFam = new Text("myColFam");
		Text colQual = new Text("myColQual");
		ColumnVisibility colVis = new ColumnVisibility("public");
		long timestamp = System.currentTimeMillis();

		Value value = new Value("myValue".getBytes());

		Mutation m1 = new Mutation(rowID1);
		m1.put(colFam, colQual, colVis, timestamp, value);

		Mutation m2 = new Mutation(rowID2);
		m2.put(colFam, colQual, colVis, timestamp, value);

		Mutation m3 = new Mutation(rowID3);
		m3.put(colFam, colQual, colVis, timestamp, value);
		Mutation m4 = new Mutation(rowID4);
		m4.put(colFam, colQual, colVis, timestamp, value);
		Mutation m5 = new Mutation(rowID5);
		m5.put(colFam, colQual, colVis, timestamp, value);
		Mutation m6 = new Mutation(rowID6);
		m6.put(colFam, colQual, colVis, timestamp, value);
		Mutation m7 = new Mutation(rowID7);
		m7.put(colFam, colQual, colVis, timestamp, value);
		Mutation m8 = new Mutation(rowID8);
		m8.put(colFam, colQual, colVis, timestamp, value);

		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L); // bytes available to batchwriter for
										// buffering mutations

		BatchWriter writer;
		try {
			writer = conn.createBatchWriter("t1", config);
			writer.addMutation(m1);
			writer.addMutation(m2);
			writer.close();
			System.out.println("added");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main2(String[] args) {
		Text rowID = new Text("row1123456789");
		Text colFam = new Text("myColFam");
		Text colQual = new Text("myColQual");
		ColumnVisibility colVis = new ColumnVisibility("public");
		long timestamp = System.currentTimeMillis();

		Value value = new Value("myValue".getBytes());

		Mutation mutation = new Mutation(rowID);
		mutation.put(colFam, colQual, colVis, timestamp, value);

		System.out.println(mutation);

		// Key k = new Key(rowID, colFam, colQual, colVis, timestamp);
		// AccumuloRowId id = new AccumuloRowId(k);

		// NumericDimensionDefinition[] dimensionDefinitions = indexStrategy
		// .getOrderedDimensionDefinitions();

		// TieredSFCIndexStrategy tt=new TieredSFCIndexStrategy(baseDefinitions,
		// orderedSfcs, orderedSfcIndexToTierId);
	}

	public static void main(String[] args) {
		main1(null);
		// byte[] b = new byte[16];
		// AccumuloRowId id = new AccumuloRowId(b);
		// byte[] iid = id.getInsertionId();

	}
}
