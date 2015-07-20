package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class GeospatialExtent {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn;
		AuthenticationToken authToken = new PasswordToken("password");
		conn = inst.getConnector("root", authToken);
		 addSplits(conn);
//		getSplits(conn, inst);
	}

	public static void addSplits(Connector conn) {
		Authorizations auths = new Authorizations();
		Scanner scan;
		try {
			scan = conn.createScanner("ruks_SPATIAL_VECTOR_IDX", auths);
		} catch (TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}

		System.out.println("size " + scan.getBatchSize());

		TableOperations op = conn.tableOperations();

		int cnt = 0;
		SortedSet<Text> keys = new TreeSet<Text>();
		for (Entry<Key, Value> entry : scan) {
			Key k = entry.getKey();
			if (cnt == 300) {
				keys.add(k.getRow());
			} else if (cnt == 600) {
				keys.add(k.getRow());
			} else if (cnt == 900) {
				keys.add(k.getRow());
			}
			cnt++;
		}

		try {
			op.addSplits("ruks_SPATIAL_VECTOR_IDX", keys);
		} catch (TableNotFoundException | AccumuloException
				| AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void getSplits(Connector conn, Instance inst) {
		TableOperations op = conn.tableOperations();
		System.out.println(op.tableIdMap());
		String table = "ruks_SPATIAL_VECTOR_IDX";
		String tid = op.tableIdMap().get("ruks_SPATIAL_VECTOR_IDX");

		try {
			List<Text> list = new ArrayList<Text>(op.listSplits(table));
			System.out.println(list.size());

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();

			Instance accInstance = inst;
			ClientContext ctx = new ClientContext(accInstance, new Credentials(
					"root", new PasswordToken("password")), clientConf);
			TabletLocator tl = TabletLocator.getLocator(ctx, new Text(tid));
			System.out.println();

			TabletLocation tt;
			String loc;
			Range r;
			String uuid;
			KeyExtent ke;
			for (int i = 0; i < list.size(); i++) {
				tt = tl.locateTablet(ctx, list.get(i), false, false);
				System.out.println(tt.tablet_location);
				ke = tt.tablet_extent;
				loc = tl.locateTablet(ctx, ke.getEndRow(), false, false).tablet_location;
				System.out.println(loc);
				r = new Range(list.get(i), ke.getEndRow());
				uuid = ke.getUUID().toString();
				System.out.println(uuid);
			}

			Text first, last;
			Key[] kk = read(list.get(list.size() - 1), conn, table);
			first = kk[0].getRow();
			last = kk[1].getRow();

			tt = tl.locateTablet(ctx, first, false, false);
			System.out.println(tt.tablet_location);
			ke = tt.tablet_extent;
			loc = tl.locateTablet(ctx, last, false, false).tablet_location;
			System.out.println(loc);
			r = new Range(first, last);
			System.out.println(ke.getUUID());

		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Key[] read(Text end, Connector conn, String table) {

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner(table, auths);
			scan.setRange(new Range(end, null));
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			int j = 0;
			Key first = null, last;
			Key k = null;
			for (Entry<Key, Value> entry : scan) {
				k = entry.getKey();
				if (j++ == 1) {
					first = k;
				}
			}
			last = k;
			return new Key[] { first, last };

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}
}
