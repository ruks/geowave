package mil.nga.giat.geowave.service.healthimpl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class GeospatialExtentTest {
	static String testTnameSuffix = "_SPATIAL_VECTOR_IDX";
	static TableOperations operation;
	static Connector conn;

	static String instanceName = "geowaveTest";
	static String zooServers = "127.0.0.1";
	static String user = "root";
	static String pass = "password";
	static String ns = "testns";
	static MiniAccumuloClusterImpl accumulo;

	@BeforeClass
	public static void ingest() {
		try {
			startAccumulo();
		} catch (IOException | InterruptedException | AccumuloException
				| AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IngestData ingest = new IngestData();
		ingest.ingestData(zooServers, instanceName, user, pass, ns);
		try {
			addSpits();
		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void addSpits() throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException {
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		AuthenticationToken authToken = new PasswordToken(pass);
		Connector conn = inst.getConnector(user, authToken);
		TableOperations t = conn.tableOperations();

		Authorizations auths = new Authorizations();

		String table = ns + testTnameSuffix;
		Scanner scan = conn.createScanner(table, auths);

		IteratorSetting it = new IteratorSetting(1,
				org.apache.accumulo.core.iterators.user.WholeRowIterator.class);
		scan.addScanIterator(it);

		int i = 0;
		SortedSet<Text> keys = new TreeSet<Text>();
		for (Entry<Key, Value> entry : scan) {
			Text row = entry.getKey().getRow();
			i++;
			if (i == 3) {
				keys.add(row);
			}
		}

		t.addSplits(table, keys);
	}

	@AfterClass
	public static void stopAccumulo() throws Exception {
		accumulo.stop();
	}

	@Test
	public void getTabletPolygonTest() {
		GeospatialExtent ex = new GeospatialExtent(instanceName,
				"127.0.0.1:2181", pass, user, ns);
		String testTname = ns + testTnameSuffix;
		List<Range> splits = ex.getSplits(testTname);

		for (Range range : splits) {
			Coordinate[] points = ex.extent(testTname, range);
			Assert.assertEquals(3, points.length);
			Geometry g = ex.getConvexHull(points);
			Assert.assertEquals(4, g.getNumPoints());

			Coordinate[] cos = g.getCoordinates();
			Assert.assertEquals(cos[0].x, cos[3].x, 0);
			Assert.assertEquals(cos[0].y, cos[3].y, 0);

			boolean exist;
			for (Coordinate poi1 : points) {
				exist = false;
				for (Coordinate cos1 : cos) {
					if (poi1.x == cos1.x && poi1.y == cos1.y) {
						exist = true;
						break;
					}
				}
				Assert.assertTrue(exist);
			}
		}

	}

	public void getPointsTabletTest(Coordinate[] cos, List<Range> splits) {

	}

	public static void startAccumulo() throws IOException,
			InterruptedException, AccumuloException, AccumuloSecurityException {

		File tempDirectory = Files.createTempDir();

		MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(
				tempDirectory, pass).setNumTservers(2)
				.setInstanceName(instanceName).setZooKeeperPort(2181);

		accumulo = new MiniAccumuloClusterImpl(miniAccumuloConfig);

		accumulo.start();

	}
}
