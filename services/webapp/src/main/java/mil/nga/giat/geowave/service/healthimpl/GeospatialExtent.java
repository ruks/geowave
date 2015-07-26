package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeospatialExtent {

	private static final Logger logger = LogManager
			.getLogger(GeospatialExtent.class);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Connector conn;
		AuthenticationToken authToken = new PasswordToken("password");
		conn = inst.getConnector("root", authToken);
		// addSplits(conn);
		getSplits(conn, inst);
	}

	public static void addSplits(Connector conn) {
		Authorizations auths = new Authorizations();
		Scanner scan;
		try {
			scan = conn.createScanner("ruks_SPATIAL_VECTOR_IDX", auths);
		} catch (TableNotFoundException e1) {
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
			if (cnt == 300) {
				keys.add(k.getRow());
			} else if (cnt == 600) {
				keys.add(k.getRow());
			} else if (cnt == 900) {
				keys.add(k.getRow());
			}
			cnt++;
		}

		try {
			op.addSplits("ruks_SPATIAL_VECTOR_IDX", keys);
		} catch (TableNotFoundException | AccumuloException
				| AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void getSplits(Connector conn, Instance inst) {
		TableOperations op = conn.tableOperations();
		System.out.println(op.tableIdMap());
		String table = "ruks_SPATIAL_VECTOR_IDX";
		String tid = op.tableIdMap().get("ruks_SPATIAL_VECTOR_IDX");

		try {
			List<Text> list = new ArrayList<Text>(op.listSplits(table));
			System.out.println(list.size());

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();

			Instance accInstance = inst;
			ClientContext ctx = new ClientContext(accInstance, new Credentials(
					"root", new PasswordToken("password")), clientConf);
			TabletLocator tl = TabletLocator.getLocator(ctx, new Text(tid));
			System.out.println();

			TabletLocation tt;
			String loc;
			ArrayList<Range> ranges = new ArrayList<Range>();
			Range r;
			String uuid;
			KeyExtent ke;
			for (int i = 0; i < list.size(); i++) {
				tt = tl.locateTablet(ctx, list.get(i), false, false);
				System.out.println(tt.tablet_location);
				ke = tt.tablet_extent;
				loc = tl.locateTablet(ctx, ke.getEndRow(), false, false).tablet_location;
				System.out.println(loc);
				r = new Range(list.get(i), ke.getEndRow());
				// ranges.add(r);
				ranges.add(ke.toDataRange());
				uuid = ke.getUUID().toString();
				System.out.println(uuid);
			}

			Text first, last;
			Key[] kk = read(list.get(list.size() - 1), conn, table);
			first = kk[0].getRow();
			last = kk[1].getRow();

			tt = tl.locateTablet(ctx, first, false, false);
			System.out.println(tt.tablet_location);
			ke = tt.tablet_extent;
			loc = tl.locateTablet(ctx, last, false, false).tablet_location;
			System.out.println(loc);
			r = new Range(first, last);
			ranges.add(r);
			System.out.println(ke.getUUID());

			System.out.println(ranges.get(0));
			// extent(conn, table, ranges.get(ranges.size() - 1), accInstance);
			extent(conn, table, ranges.get(0), accInstance);
		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Key[] read(Text end, Connector conn, String table) {

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner(table, auths);
			scan.setRange(new Range(end, null));
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			int j = 0;
			Key first = null, last;
			Key k = null;
			for (Entry<Key, Value> entry : scan) {
				k = entry.getKey();
				if (j++ == 1) {
					first = k;
				}
			}
			last = k;
			return new Key[] { first, last };

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}

	public static void extent(Connector conn, String table, Range range,
			Instance inst) {

		Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner(table, auths);
			scan.setRange(range);
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			ArrayList<Object> list = new ArrayList<Object>();
			for (Entry<Key, Value> entry : scan) {
				AccumuloRowId id = new AccumuloRowId(entry.getKey());
				ByteArrayId bid = new ByteArrayId(id.getAdapterId());

				AccumuloOperations ao = new BasicAccumuloOperations(conn,
						"ruks");
				AccumuloAdapterStore a = new AccumuloAdapterStore(ao);
				DataAdapter<?> adapter = a.getAdapter(bid);

				Object o = AccumuloUtils.decodeRow(entry.getKey(),
						entry.getValue(), adapter, index);

				list.add(o);

				System.out.println(o);
			}
			Coordinate[] cloud = new Coordinate[10];
			Random rand = new Random();
			for (int i = 0; i < cloud.length; i++) {
				cloud[i] = new Coordinate(100 + 50 * rand.nextDouble(),
						100 + 50 * rand.nextDouble());
			}
			ConvexHull c = new ConvexHull(cloud, new GeometryFactory());
			Geometry geometry = c.getConvexHull();
			System.out.println(geometry.getNumPoints());

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected static SimpleFeatureType createPointFeatureType() {

		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();

		builder.setName("Point");

		builder.add(ab.binding(Geometry.class).nillable(false)
				.buildDescriptor("geometry"));
		builder.add(ab.binding(Date.class).nillable(true)
				.buildDescriptor("TimeStamp"));
		builder.add(ab.binding(Double.class).nillable(false)
				.buildDescriptor("Latitude"));
		builder.add(ab.binding(Double.class).nillable(false)
				.buildDescriptor("Longitude"));
		builder.add(ab.binding(String.class).nillable(true)
				.buildDescriptor("TrajectoryID"));
		builder.add(ab.binding(String.class).nillable(true)
				.buildDescriptor("Comment"));

		return builder.buildFeatureType();
	}

	public static FeatureDataAdapter createDataAdapter(
			final SimpleFeatureType sft) {
		return new FeatureDataAdapter(sft);
	}
}
