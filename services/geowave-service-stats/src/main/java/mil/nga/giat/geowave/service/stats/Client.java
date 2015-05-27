package mil.nga.giat.geowave.service.stats;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;

public class Client {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		try {
			Connector conn = inst.getConnector("root", "password");
			System.out.println(conn.whoami());
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// ListInstances li=new ListInstances();

		Key key = new Key("ruks");
//		AccumuloRowId id = new AccumuloRowId(key);
	}
}
