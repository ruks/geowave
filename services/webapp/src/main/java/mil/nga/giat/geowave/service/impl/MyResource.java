package mil.nga.giat.geowave.service.impl;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import mil.nga.giat.geowave.service.healthimpl.Data;
import mil.nga.giat.geowave.service.healthimpl.Monitor;

/**
 * Root resource (exposed at "test" path)
 */
@Path("test")
public class MyResource
{

	/**
	 * Method handling HTTP GET requests. The returned object will be sent to
	 * the client as "text/plain" media type.
	 * 
	 * @return String that will be returned as a text/plain response.
	 * @throws IOException
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getIt() {
		Data.getInstance();
		return Monitor.getLoadOverTime().size() + "";
	}

}
