package mil.nga.giat.geowave.service.healthimpl;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class Test
{

	public static void main(
			String[] args )
			throws Exception {
		// TODO Auto-generated method stub

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		String user = "root";
		String pass = "password";

		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		AuthenticationToken authToken = new PasswordToken(
				pass);
		Connector conn = inst.getConnector(
				user,
				authToken);
		TableOperations t = conn.tableOperations();

		Authorizations auths = new Authorizations();

		Scanner scan = conn.createScanner(
				IngestData.ns + "_SPATIAL_VECTOR_IDX",
				auths);

		// scan.setRange(new Range("harry", "john"));
		// scan.fetchColumnFamily(new Text("attributes"));
		IteratorSetting it = new IteratorSetting(
				1,
				org.apache.accumulo.core.iterators.user.WholeRowIterator.class);
		scan.addScanIterator(it);

		int i = 0;
		SortedSet<Text> keys = new TreeSet<Text>();
		for (Entry<Key, Value> entry : scan) {
			Text row = entry.getKey().getRow();
			System.out.println(row);
			i++;
			if (i == 3) {
				keys.add(row);
			}
		}
		System.out.println(i);

		t.addSplits(
				IngestData.ns + "_SPATIAL_VECTOR_IDX",
				keys);

		// GeospatialExtent ex = new GeospatialExtent("geowave", "127.0.0.1",
		// "password", "root", SimpleIngest.ns);
		// ex.getTabletPolygon(SimpleIngest.ns+"_SPATIAL_VECTOR_IDX");
	}
}
