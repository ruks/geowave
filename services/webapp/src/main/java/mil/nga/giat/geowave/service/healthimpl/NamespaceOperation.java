package mil.nga.giat.geowave.service.healthimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

/**
 * Class to use namespace operations.
 * 
 * @author rukshan
 * 
 */
public class NamespaceOperation
{
	private Connector conn;

	/**
	 * 
	 * @param instanceName
	 * @param zooServers
	 * @param user
	 * @param pass
	 * @throws Exception
	 */
	public NamespaceOperation(
			String instanceName,
			String zooServers,
			String user,
			String pass )
			throws Exception {

		Instance inst = new ZooKeeperInstance(
				instanceName,
				zooServers);
		AuthenticationToken authToken = new PasswordToken(
				pass);
		this.conn = inst.getConnector(
				user,
				authToken);

	}

	/**
	 * Get all the namespace in geowave
	 * 
	 * @return
	 */
	public List<String> getNamespaces() {
		return AccumuloUtils.getNamespaces(conn);
	}

	/**
	 * get the namespace of a given table
	 * 
	 * @param table
	 * @return namespace
	 */
	public static String getNamespaceOfTable(
			String table ) {
		String ns;
		int i = table.indexOf('_');
		ns = table.substring(
				0,
				i);
		return ns;
	}

	/**
	 * Getting all the tables for a given namespace
	 * 
	 * @param ns
	 *            namespace need to find
	 * @return list of tables
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */
	public List<String> getTablesInNamespaces(
			String ns )
			throws AccumuloException,
			TableNotFoundException {

		// getting all the tables
		SortedSet<String> li = conn.tableOperations().list();
		List<String> nsli = new ArrayList<String>();

		for (String st : li) {
			if (st.startsWith(ns + "_")) {
				nsli.add(st); // add table if prefix is match with given
								// namespace
			}
		}
		return nsli;
	}

	public static void main(
			String[] args )
			throws Exception {
		// String instanceName = GeowavePropertyReader
		// .readProperty("instanceName");
		// String zooServers = GeowavePropertyReader.readProperty("zooServers");
		// String user = GeowavePropertyReader.readProperty("user");
		// String pass = GeowavePropertyReader.readProperty("pass");
		// NamespaceOperation ns = new NamespaceOperation(
		// instanceName,
		// zooServers,
		// user,
		// pass);
		System.out.println(NamespaceOperation.getNamespaceOfTable("ruks_SPATIAL_VECTOR_IDX"));
	}
}
