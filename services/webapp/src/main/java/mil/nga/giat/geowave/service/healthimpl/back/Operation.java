package mil.nga.giat.geowave.service.healthimpl.back;

import java.util.Map.Entry;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
//import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class Operation
{
	@SuppressWarnings("deprecation")
	public static void main2(
			String[] args ) {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		Connector conn;
		try {
			conn = inst.getConnector(
					"root",
					"password");
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		TableOperations op = conn.tableOperations();

		try {
			System.out.println(op.getProperties("ruks_SPATIAL_VECTOR_IDX"));
		}
		catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {

			// Range r = new Range();
			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner(
					"ruks_SPATIAL_VECTOR_IDX",
					auths);

			for (Entry<Key, Value> entry : scan) {
				Key k = entry.getKey();
				// System.out.println(k);
				try {
					AccumuloRowId id = new AccumuloRowId(
							k);
					byte[] bb = id.getInsertionId();

					ByteArrayId insertionId = new ByteArrayId(
							bb);
					Index tempIdx = IndexType.SPATIAL_VECTOR.createDefaultIndex();
					long[] list = ((TieredSFCIndexStrategy) tempIdx.getIndexStrategy()).getCoordinatesPerDimension(insertionId);
					System.out.println(list[0] + " " + list[1]);
				}
				catch (Exception e) {
					System.out.println(e.getMessage());
				}

			}
			System.out.println("finished");

		}
		catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(
			String[] args ) {
		read();
	}

	public static void read() {
		String instanceName = "geowave";
		String zooServers = "127.0.0.1";
		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		Connector conn;
		try {
			conn = inst.getConnector(
					"root",
					"password");
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		TableOperations op = conn.tableOperations();

		try {
			System.out.println(op.getProperties("ruks_SPATIAL_VECTOR_IDX"));
		}
		catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {

			// Range r = new Range();
			Authorizations auths = new Authorizations();
			Scanner scan = conn.createScanner(
					"ruks_SPATIAL_VECTOR_IDX",
					auths);

			System.out.println(scan.getBatchSize());
			for (Entry<Key, Value> entry : scan) {
				Key k = entry.getKey();
				Value v = entry.getValue();
				// System.out.println(k);
			}
			System.out.println("finished");

		}
		catch (TableNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
