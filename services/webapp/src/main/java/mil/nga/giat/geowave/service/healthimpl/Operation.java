package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
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
		// createTable(op);

		System.out.println(op.list());
		try {
			Iterable<Map.Entry<String, String>> it = op
					.getProperties("accumulo.root");
			System.out.println(it);
		} catch (AccumuloException | TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			Collection<Text> list = op.listSplits("accumulo.metadata");
			System.out.println(list.size());

			ArrayList<Text> ll = new ArrayList<Text>(list);
			System.out.println(ll.get(0));

			AccumuloRowId id = new AccumuloRowId(new Key(ll.get(0)));
			byte[] b = id.getInsertionId();
			System.out.println(b);

		} catch (TableNotFoundException | AccumuloSecurityException
				| AccumuloException e) {
			// TODO Auto-generated catch block
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
			set.add(new Text(""));
			op.addSplits("t1", set);
		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		TieredSFCIndexStrategy st = new TieredSFCIndexStrategy() {
		};
		ByteArrayId id = new ByteArrayId("rukssdsdsdhan");
		long[] l = st.getCoordinatesPerDimension(id);
		for (int i = 0; i < l.length; i++) {
			System.out.println(l[i]);
		}
	}
}
