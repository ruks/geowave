package mil.nga.giat.geowave.service.healthimpl;

import java.io.File;
import java.util.List;

import mil.nga.giat.geowave.service.jaxbbean.TableBean;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

import com.google.common.io.Files;

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

		File tempDirectory = Files.createTempDir();
		MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(
				tempDirectory,
				pass).setNumTservers(
				2).setInstanceName(
				instanceName).setZooKeeperPort(
				2181);

		MiniAccumuloClusterImpl accumulo = new MiniAccumuloClusterImpl(
				miniAccumuloConfig);
		accumulo.start();

		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);

		MasterMonitorInfo masterMonitorInfo = null;

		MasterClientService.Iface client = null;
		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(
							inst));
			client = MasterClient.getConnectionWithRetry(context);
			masterMonitorInfo = client.getMasterStats(
					Tracer.traceInfo(),
					context.rpcCreds());
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		}
		finally {

			if (client != null) MasterClient.close(client);
		}

		System.out.println(masterMonitorInfo.tableMap);
	}

}
