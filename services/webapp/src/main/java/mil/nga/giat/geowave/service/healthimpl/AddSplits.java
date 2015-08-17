package mil.nga.giat.geowave.service.healthimpl;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class AddSplits
{
	static String ns = "namespace";

	public static void main(
			String[] args )
			throws Exception {
		// TODO Auto-generated method stub
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		Connector conn;
		AuthenticationToken authToken = new PasswordToken(
				"password");
		conn = inst.getConnector(
				"root",
				authToken);
		addSplits(conn);
	}

	public static void addSplits(
			Connector conn ) {
		Authorizations auths = new Authorizations();
		Scanner scan;
		try {
			scan = conn.createScanner(
					ns + "_SPATIAL_VECTOR_IDX",
					auths);
		}
		catch (TableNotFoundException e1) {
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
			if (cnt == 150) {
				keys.add(k.getRow());
			}
			else if (cnt == 350) {
				keys.add(k.getRow());
			}
			else if (cnt == 550) {
				keys.add(k.getRow());
			}
			else if (cnt == 750) {
				keys.add(k.getRow());
			}
			cnt++;
		}

		try {
			op.addSplits(
					ns + "_SPATIAL_VECTOR_IDX",
					keys);
		}
		catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
