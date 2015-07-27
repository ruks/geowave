package mil.nga.giat.geowave.service.healthimpl;

import java.io.File;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import com.google.common.io.Files;

public class Test
{

	public static void main(
			String[] args )
			throws Exception {
		// TODO Auto-generated method stub
		File tempDirectory = Files.createTempDir();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(
				tempDirectory,
				"password");

		accumulo.start();

		Instance instance = new ZooKeeperInstance(
				accumulo.getInstanceName(),
				accumulo.getZooKeepers());
		Connector conn = instance.getConnector(
				"root",
				new PasswordToken(
						"password"));

		TableOperations to = conn.tableOperations();
		to.create("rukshan");
		System.out.println(to.list());
		System.out.println(conn.whoami());

	}
}
