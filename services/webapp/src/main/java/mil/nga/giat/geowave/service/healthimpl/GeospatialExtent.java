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
import org.apache.accumulo.core.data.Value;
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
//		 addSplits(conn);
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

			TabletLocator root;
			Text tid = new Text("ruks_SPATIAL_VECTOR_IDX");
			// String instanceName = "geowave";
			// String zooServers = "127.0.0.1";
			// Instance inst = new ZooKeeperInstance(instanceName, zooServers);

			// Connector conn;
			try {
				conn = inst.getConnector("root", "password");
			} catch (AccumuloException | AccumuloSecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();
//			ClientConfiguration cc = new ClientConfiguration();
//			cc.setProperty(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST, "localhost:2181");
//			cc.setProperty(ClientConfiguration.ClientProperty.INSTANCE_NAME, "geowave");
//			cc.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD, "password");
			
			Instance accInstance = inst;
			ClientContext ctx = new ClientContext(accInstance, new Credentials(
					"root", new PasswordToken("password")), clientConf);
			TabletLocator tl = TabletLocator.getLocator(ctx, new Text("2"));
			System.out.println();
			
			TabletLocation tt1=tl.locateTablet(ctx, list.get(0), false, false);
			System.out.println(tt1.tablet_location);
			TabletLocation tt2=tl.locateTablet(ctx, list.get(1), false, false);
			System.out.println(tt2.tablet_location);
			TabletLocation tt3=tl.locateTablet(ctx, list.get(2), false, false);
			System.out.println(tt3.tablet_location);
			
			
			// AccumuloServerContext context = new AccumuloServerContext(
			// new ServerConfigurationFactory(conn.getInstance()));
			//
			// AuthenticationToken authToken = new PasswordToken("password");
			//
			// ClientContext c = new ClientContext(conn.getInstance(),
			// new Credentials("geowave", authToken),
			// inst.getConfiguration());

			// TabletLocator tl = TabletLocator.getLocator(c, new Text(
			// "ruks_SPATIAL_VECTOR_IDX"));
			// System.out.println(tl.locateTablet(c, list.get(0), false,
			// false));
			// root.getLocator(context, tableId);
			// TabletLocator.getLocator(context, tid).locateTablet(context,
			// list.get(0), false, false);

		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void read() {
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
			System.out.println(op.getProperties("ruks_SPATIAL_VECTOR_IDX"));
		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {

			// Range r = new Range();
			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner("ruks_SPATIAL_VECTOR_IDX", auths);

			System.out.println(scan.getBatchSize());
			for (Entry<Key, Value> entry : scan) {
				Key k = entry.getKey();
				Value v = entry.getValue();
				// System.out.println(k);
			}
			System.out.println("finished");

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
