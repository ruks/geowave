package mil.nga.giat.geowave.service.healthimpl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class GeospatialExtentTest {
	static String testTname = "sampleTable";
	static TableOperations operation;
	static Connector conn;

	static String instanceName = "geowaveTest";
	static String zooServers = "127.0.0.1";
	static String user = "root";
	static String pass = "password";
	static MiniAccumuloClusterImpl accumulo;

	@BeforeClass
	public static void ingest() {

	}

	@AfterClass
	public static void stopAccumulo() throws Exception {
		// accumulo.stop();
	}

	@Test
	public void getTabletPolygonTest() {
		GeospatialExtent ex = new GeospatialExtent("geowave", "127.0.0.1",
				"password", "root", "test4");
		String table = "test4_SPATIAL_VECTOR_IDX";
		List<Range> splits = ex.getTabletPolygon(table);
		System.out.println("splits " + splits.size());

		for (Range range : splits) {
			Coordinate[] points = ex.extent(table, range, ex.connector);
			Assert.assertEquals(3, points.length);
			Geometry g = ex.getConvexHull(points);
			Assert.assertEquals(4, g.getNumPoints());
			System.out.println(g);

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

	public static Connector getInstance() throws IOException,
			InterruptedException, AccumuloException, AccumuloSecurityException {

		File tempDirectory = Files.createTempDir();

		MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(
				tempDirectory, pass).setNumTservers(2)
				.setInstanceName(instanceName).setZooKeeperPort(2181);

		accumulo = new MiniAccumuloClusterImpl(miniAccumuloConfig);

		accumulo.start();

		Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(),
				accumulo.getZooKeepers());
		Connector conn = instance.getConnector(user, new PasswordToken(pass));
		return conn;
	}
}
