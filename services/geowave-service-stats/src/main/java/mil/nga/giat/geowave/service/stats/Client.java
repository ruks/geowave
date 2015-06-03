package mil.nga.giat.geowave.service.stats;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class Client {

	public static void main1(String[] args) {
		// TODO Auto-generated method stub
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		try {
			// Connector conn = inst.getConnector("root", "password");
			// System.out.println(conn.whoami());

			Text t = new Text("rukshakn12");

			Text a = new Text("aa");
			Text b = new Text("bb");
			Text c = new Text("cc");
			Text d = new Text("dd");

			byte[] bb = new byte[0];

			Key k = new Key(a.getBytes(), 0, a.getLength(), c.getBytes(), 0,
					b.getLength(), c.getBytes(), 0, c.getLength(),
					d.getBytes(), 0, d.getLength(), Long.MAX_VALUE);

			// AccumuloRowIds ids = new AccumuloRowIds(k);

			Mutation m = new Mutation(new Text(String.format("row_%d", 1)));
			Text t1 = new Text(String.format("colqual_%d", 0));
			Value v = new Value((String.format("value_%d_%d", 0, 1)).getBytes());
			Text colf = new Text("colfam");

			m.put(colf, t1, v);

			AccumuloRowIds id = new AccumuloRowIds(m.getRow());

			// TieredSFCIndexStrategy tt=new
			// TieredSFCIndexStrategy(baseDefinitions, orderedSfcs,
			// orderedSfcIndexToTierId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] arsg) {
		// String instanceName = "geowave";
		// String zooServers = "127.0.0.1";
		// Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		//

//		TieredSFCIndexStrategy tt = new TieredSFCIndexStrategy(baseDefinitions, orderedSfcs, orderedSfcIndexToTierId);
//
//		ByteArrayId ids = new ByteArrayId("qsdfdsfwew");
//		tt.getCoordinatesPerDimension(ids);

	}
}
