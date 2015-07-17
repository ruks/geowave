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
		// addSplits(conn);
		getSplits(conn, inst);
		// read();
		// t.delete(testTname);
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

		try {
			List<Text> list = new ArrayList<Text>(
					op.listSplits("ruks_SPATIAL_VECTOR_IDX"));
			System.out.println(list.size());

			for (int i = 0; i < list.size(); i++) {
				Key ke = new Key(list.get(i));

			}

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();

			Instance accInstance = inst;
			ClientContext ctx = new ClientContext(accInstance, new Credentials(
					"root", new PasswordToken("password")), clientConf);
			TabletLocator tl = TabletLocator.getLocator(ctx, new Text("2"));
			System.out.println();

			TabletLocation tt1 = tl
					.locateTablet(ctx, list.get(0), false, false);
			System.out.println(tt1.tablet_location);
			KeyExtent ke1 = tt1.tablet_extent;
			String loc1 = tl.locateTablet(ctx, ke1.getEndRow(), false, false).tablet_location;
			System.out.println(loc1);
			Range r1 = new Range(list.get(0), ke1.getEndRow());
			// TabletId id1=new TabletIdImpl(ke1);
			// System.out.println(id1);

			TabletLocation tt2 = tl
					.locateTablet(ctx, list.get(1), false, false);
			System.out.println(tt2.tablet_location);
			KeyExtent ke2 = tt2.tablet_extent;
			String loc2 = tl.locateTablet(ctx, ke2.getEndRow(), false, false).tablet_location;
			System.out.println(loc2);
			Range r2 = new Range(list.get(1), ke2.getEndRow());

			TabletLocation tt3 = tl
					.locateTablet(ctx, list.get(2), false, false);
			System.out.println(tt3.tablet_location);
			KeyExtent ke3 = tt3.tablet_extent;
			String loc3 = tl.locateTablet(ctx, ke3.getEndRow(), false, false).tablet_location;
			System.out.println(loc3);
			Range r3 = new Range(list.get(2), ke3.getEndRow());

			Text fRow = read(list.get(0), conn).getRow();
			TabletLocation tt0 = tl.locateTablet(ctx, fRow, false, false);
			System.out.println(tt0.tablet_location);
			KeyExtent ke0 = tt0.tablet_extent;
			String loc0 = tl.locateTablet(ctx, ke0.getEndRow(), false, false).tablet_location;
			System.out.println(loc0);
			Range r0 = new Range(fRow, ke0.getEndRow());

		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Key read(Text end, Connector conn) {

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner("ruks_SPATIAL_VECTOR_IDX", auths);
			scan.setRange(new Range(end, null));
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			int j = 0;
			Key i = null;
			for (Entry<Key, Value> entry : scan) {
				Key k = entry.getKey();
				if (j++ == 1) {
					return k;
				}
			}
			return i;

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}
}
