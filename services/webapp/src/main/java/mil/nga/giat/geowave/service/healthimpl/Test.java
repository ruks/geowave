package mil.nga.giat.geowave.service.healthimpl;

import java.security.MessageDigest;
import java.util.List;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

import com.google.common.net.HostAndPort;

public class Test {
	public static void main(String[] args) {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Instance accInstance = inst;
		ClientConfiguration clientConf = ClientConfiguration.loadDefault();
		ClientContext ctx = new ClientContext(accInstance, new Credentials(
				"root", new PasswordToken("password")), clientConf);

		HostAndPort address = HostAndPort.fromString("rukshan-ThinkPad-T540p:59228");
//		ClientContext context = Monitor.getContext();
		
		ServerConfigurationFactory config;
		AccumuloServerContext context;
		config = new ServerConfigurationFactory(inst);
		context = new AccumuloServerContext(config);
		
		try {
			TabletClientService.Client client = ThriftUtil.getClient(
					new TabletClientService.Client.Factory(), address, ctx);
			// String tid = op.tableIdMap().get("ruks_SPATIAL_VECTOR_IDX");
			List<TabletStats> li = client.getTabletStats(Tracer.traceInfo(),
					context.rpcCreds(), "2");
			 System.out.println(li.size());
			 System.out.println(li);
			 
			 TabletStats t0=li.get(0);
			 KeyExtent extent = new KeyExtent(t0.extent);
		      String tableId = extent.getTableId().toString();
		      
		      MessageDigest digester = MessageDigest.getInstance("MD5");
		      if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
		        digester.update(extent.getEndRow().getBytes(), 0, extent.getEndRow().getLength());
		      }
		      String obscuredExtent = Base64.encodeBase64String(digester.digest());
			 System.out.println(tableId);
			 System.out.println(obscuredExtent);
			 System.out.println(extent.getUUID());
			 
			 TabletStats t1=li.get(1);
			 KeyExtent extent1 = new KeyExtent(t1.extent);
		      String tableId1 = extent1.getTableId().toString();
		      
		      MessageDigest digester1 = MessageDigest.getInstance("MD5");
		      if (extent1.getEndRow() != null && extent1.getEndRow().getLength() > 0) {
		        digester.update(extent1.getEndRow().getBytes(), 0, extent1.getEndRow().getLength());
		      }
		      String obscuredExtent1 = Base64.encodeBase64String(digester1.digest());
			 System.out.println(tableId1);
			 System.out.println(obscuredExtent1);
			 System.out.println(extent1.getUUID());
			 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
