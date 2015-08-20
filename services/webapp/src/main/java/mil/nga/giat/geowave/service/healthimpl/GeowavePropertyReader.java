package mil.nga.giat.geowave.service.healthimpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GeowavePropertyReader
{

	/**
	 * Class to read properties
	 * 
	 * @param prob
	 *            property
	 * @return value of the property
	 */
	public static String readProperty(
			String prob ) {
		Properties prop = new Properties();
		InputStream input = null;
		String value = null;
		try {

			// load the property file
			input = GeowavePropertyReader.class.getClassLoader().getResourceAsStream(
					GeowaveConstant.propFile);

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			value = prop.getProperty(prob);

		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
		finally {
			if (input != null) {
				try {
					input.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return value;
	}

	public static void main(
			String[] args ) {
		System.out.println(readProperty("test"));
	}
}
