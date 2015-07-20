package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.service.jaxbbean.TabletBean;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

public class TabletStats {
	private List<TabletBean> tabletStats;
	private MasterMonitorInfo masterMonitorInfo = null;

	public TabletStats() throws Exception {

		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		MasterClientService.Iface client = null;

		try {
			AccumuloServerContext context = new AccumuloServerContext(
					new ServerConfigurationFactory(inst));
			client = MasterClient.getConnectionWithRetry(context);
			masterMonitorInfo = client.getMasterStats(Tracer.traceInfo(),
					context.rpcCreds());
			List<String> li = client.getActiveTservers(Tracer.traceInfo(),
					context.rpcCreds());
			System.out.println(li);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		} finally {
			if (client != null)
				MasterClient.close(client);
		}

	}

	public List<TabletBean> getTabletStats() {
		List<TabletServerStatus> tabs = masterMonitorInfo.getTServerInfo();

		System.out.println(masterMonitorInfo.getTableMap());
		tabletStats = new ArrayList<TabletBean>();

		for (int i = 0; i < tabs.size(); i++) {
			TabletServerStatus sta = tabs.get(i);

			String name = sta.getName();
			int tablets = sta.getTableMapSize();
			long lastContact = sta.getLastContact();
			long holdTime = 0;

			Map<String, TableInfo> map = sta.getTableMap();

			Set<String> key = map.keySet();

			int entries = 0, ingest = 0, query = 0;
			Compacting scans = new Compacting(0, 0);
			Compacting minor = new Compacting(0, 0);
			Compacting major = new Compacting(0, 0);

			System.out.println(sta.getName());
			System.out.println(sta);

			Object[] arr = key.toArray();
			for (int j = 0; j < arr.length; j++) {

				// TableInfo info = map.get(arr[j]);
				// if (info.getTablets() > 1){
				// System.out.println(info.getRecs());
				// System.out.println(info);
				// }

			}

		}

		return tabletStats;

	}

	public static void main(String[] args) throws Exception {
		TabletStats stats = new TabletStats();
		List<TabletBean> sta = stats.getTabletStats();
		System.out.println(sta.size());
	}
}
