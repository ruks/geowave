package mil.nga.giat.geowave.service.healthimpl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SecurityUtil;

public class Data {

	// private static Instance instance;
	private static ServerConfigurationFactory config;
	private static AccumuloServerContext context;

	public static void start(String[] args) throws AccumuloException,
			AccumuloSecurityException, IOException, IllegalArgumentException,
			IllegalAccessException {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		SecurityUtil.serverLogin(SiteConfiguration.getInstance());

		final String app = "monitor";
		String hostname = "127.0.0.1";

		Accumulo.setupLogging(app);
		VolumeManager fs = VolumeManagerImpl.get();

		config = new ServerConfigurationFactory(inst);
		context = new AccumuloServerContext(config);
		Accumulo.init(fs, config, app);
		Monitor monitor = new Monitor();
		DistributedTrace.enable(hostname, app, config.getConfiguration());

		Field[] f = monitor.getClass().getDeclaredFields();
		Field in = null;
		Field con = null;
		Field cont = null;
		for (int i = 0; i < f.length; i++) {
			if (f[i].getName().equals("instance")) {
				in = f[i];
			}
			if (f[i].getName().equals("config")) {
				con = f[i];
			}
			if (f[i].getName().equals("context")) {
				cont = f[i];
			}
		}

		in.setAccessible(true);
		in.set(monitor, inst);
		con.setAccessible(true);
		con.set(monitor, config);
		cont.setAccessible(true);
		cont.set(monitor, context);

		try {
			monitor.run(hostname);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {

			DistributedTrace.disable();
		}

		trace();
	}

	private static void trace() {
		System.out.println(Monitor.getMmi().getTableMap());		
		
	}

	public static void main(String[] args) {
		try {
			start(null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}