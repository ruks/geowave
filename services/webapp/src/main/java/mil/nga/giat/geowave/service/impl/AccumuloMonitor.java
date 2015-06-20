package mil.nga.giat.geowave.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.service.healthimpl.Data;
import mil.nga.giat.geowave.service.healthimpl.Main;
import mil.nga.giat.geowave.service.healthimpl.Monitor;
import mil.nga.giat.geowave.service.jaxbbean.Node;

import org.apache.accumulo.core.util.Pair;

/**
 * Root resource (exposed at "monitor" path)
 */
@Path("/monitor")
public class AccumuloMonitor
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
	public boolean isRunning() {
		return Data.isStarted();
	}

	@GET
	@Path("/cors")
	@Produces("application/json")
	public Response cors() {
		List<Pair<Long, Double>> li = Monitor.getDataCacheHitRateOverTime();
		li.add(new Pair<Long, Double>(
				1L,
				1.0));
		li.add(new Pair<Long, Double>(
				2L,
				2.0));

		return Response.ok().entity(
				li).header(
				"Access-Control-Allow-Origin",
				"*").header(
				"Access-Control-Allow-Methods",
				"GET, POST, DELETE, PUT").allow(
				"OPTIONS").build();
	}

	@GET
	@Path("/test")
	@Produces("application/json")
	public Response json()
			throws Exception {

		int[] num = list();
		List<Node> nodes = new ArrayList<Node>();

		for (int id : num) {
			nodes.add(new Node(
					id,
					Math.random(),
					"server_" + id,
					Math.random(),
					Math.random(),
					Math.random(),
					Math.random()));
		}

		return Response.ok().entity(
				nodes).header(
				"Access-Control-Allow-Origin",
				"*").header(
				"Access-Control-Allow-Methods",
				"GET, POST, DELETE, PUT").allow(
				"OPTIONS").build();
	}

	@GET
	@Path("/tablets")
	@Produces("application/json")
	public Response tablets()
			throws Exception {

		// List<Node> nodes = new ArrayList<Node>();
		//
		// for (int i = 0; i < 10; i++) {
		//
		// nodes.add(new Node(
		// i,
		// 0,
		// "server_" + i,
		// Math.random(),
		// Math.random(),
		// Math.random(),
		// Math.random()));
		// }

		List<Node> nodeList = Main.startStat();

		return Response.ok().entity(
				nodeList).header(
				"Access-Control-Allow-Origin",
				"*").header(
				"Access-Control-Allow-Methods",
				"GET, POST, DELETE, PUT").allow(
				"OPTIONS").build();
	}

	@GET
	@Path("/getLoadOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getLoadOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getLoadOverTime();
		return list;
	}

	@GET
	@Path("/getIngestRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getIngestRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getIngestRateOverTime();
		return list;
	}

	@GET
	@Path("/getIngestByteRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getIngestByteRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getIngestByteRateOverTime();
		return list;
	}

	@GET
	@Path("/getMinorCompactionsOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getMinorCompactionsOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getMinorCompactionsOverTime();
		return list;
	}

	@GET
	@Path("/getMajorCompactionsOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getMajorCompactionsOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getMajorCompactionsOverTime();
		return list;
	}

	@GET
	@Path("/getLookupsOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getLookupsOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getLookupsOverTime();
		return list;
	}

	@GET
	@Path("/getQueryRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getQueryRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getQueryRateOverTime();
		return list;
	}

	@GET
	@Path("/getScanRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Integer>> getScanRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Integer>> list = Monitor.getScanRateOverTime();
		return list;
	}

	@GET
	@Path("/getQueryByteRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getQueryByteRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getQueryByteRateOverTime();
		return list;
	}

	@GET
	@Path("/getIndexCacheHitRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getIndexCacheHitRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getIndexCacheHitRateOverTime();
		return list;
	}

	@GET
	@Path("/getDataCacheHitRateOverTime")
	@Produces("application/json")
	public List<Pair<Long, Double>> getDataCacheHitRateOverTime() {
		Data.getInstance();
		List<Pair<Long, Double>> list = Monitor.getDataCacheHitRateOverTime();
		return list;
	}

	public int[] list() {
		int[] list = {
			1001,
			1003,
			1005,
			1007,
			1009,
			1011,
			1013,
			1015,
			1017,
			1019,
			1021,
			1023,
			1025,
			1027,
			1029,
			1031,
			1033,
			1035,
			1037,
			1039,
			1041,
			1043,
			1045,
			1047,
			1049,
			1051,
			1053,
			1055,
			1057,
			1059,
			1061,
			1063,
			1065,
			1067,
			1069,
			1071,
			1073,
			1075,
			1077,
			1079,
			1081,
			1083,
			1085,
			1087,
			1089,
			1091,
			1093,
			1095,
			1097,
			1099,
			1101,
			1103,
			1105,
			1107,
			1109,
			1111,
			1113,
			1115,
			1117,
			1119,
			1121,
			1123,
			1125,
			1127,
			1129,
			1131,
			1133,
			2013,
			2016,
			2020,
			2050,
			2060,
			2068,
			2070,
			2090,
			2100,
			2110,
			2122,
			2130,
			2150,
			2164,
			2170,
			2180,
			2185,
			2188,
			2201,
			2220,
			2232,
			2240,
			2261,
			2270,
			2280,
			2282,
			2290,
			4001,
			4003,
			4005,
			4007,
			4009,
			4011,
			4012,
			4013,
			4015,
			4017,
			4019,
			4021,
			4023,
			4025,
			4027,
			5001,
			5003,
			5005,
			5007,
			5009,
			5011,
			5013,
			5015,
			5017,
			5019,
			5021,
			5023,
			5025,
			5027,
			5029,
			5031,
			5033,
			5035,
			5037,
			5039,
			5041,
			5043,
			5045,
			5047,
			5049,
			5051,
			5053,
			5055,
			5057,
			5059,
			5061,
			5063,
			5065,
			5067,
			5069,
			5071,
			5073,
			5075,
			5077,
			5079,
			5081,
			5083,
			5085,
			5087,
			5089,
			5091,
			5093,
			5095,
			5097,
			5099,
			5101,
			5103,
			5105,
			5107,
			5109,
			5111,
			5113,
			5115,
			5117,
			5119,
			5121,
			5123,
			5125,
			5127,
			5129,
			5131,
			5133,
			5135,
			5137,
			5139,
			5141,
			5143,
			5145,
			5147,
			5149,
			6001,
			6003,
			6005,
			6007,
			6009,
			6011,
			6013,
			6015,
			6017,
			6019,
			6021,
			6023,
			6025,
			6027,
			6029,
			6031,
			6033,
			6035,
			6037,
			6039,
			6041,
			6043,
			6045,
			6047,
			6049,
			6051,
			6053,
			6055,
			6057,
			6059,
			6061,
			6063,
			6065,
			6067,
			6069,
			6071,
			6073,
			6075,
			6077,
			6079,
			6081,
			6083,
			6085,
			6087,
			6089,
			6091,
			6093,
			6095,
			6097,
			6099,
			6101,
			6103,
			6105,
			6107,
			6109,
			6111,
			6113,
			6115,
			8001,
			8003,
			8005,
			8007,
			8009,
			8011,
			8013,
			8014,
			8015,
			8017,
			8019,
			8021,
			8023,
			8025,
			8027,
			8029,
			8031,
			8033,
			8035,
			8037,
			8039,
			8041,
			8043,
			8045,
			8047,
			8049,
			8051,
			8053,
			8055,
			8057,
			8059,
			8061,
			8063,
			8065,
			8067,
			8069,
			8071,
			8073,
			8075,
			8077,
			8079,
			8081,
			8083,
			8085,
			8087,
			8089,
			8091,
			8093,
			8095,
			8097,
			8099,
			8101,
			8103,
			8105,
			8107,
			8109,
			8111,
			8113,
			8115,
			8117,
			8119,
			8121,
			8123,
			8125,
			9001,
			9003,
			9005,
			9007,
			9009,
			9011,
			9013,
			9015,
			10001,
			10003,
			10005,
			11001,
			12001,
			12003,
			12005,
			12007,
			12009,
			12011,
			12013,
			12015,
			12017,
			12019,
			12021,
			12023,
			12027,
			12029,
			12031,
			12033,
			12035,
			12037,
			12039,
			12041,
			12043,
			12045,
			12047,
			12049,
			12051,
			12053,
			12055,
			12057,
			12059,
			12061,
			12063,
			12065,
			12067,
			12069,
			12071,
			12073,
			12075,
			12077,
			12079,
			12081,
			12083,
			12085,
			12086,
			12087,
			12089,
			12091,
			12093,
			12095,
			12097,
			12099,
			12101,
			12103,
			12105,
			12107,
			12109,
			12111,
			12113,
			12115,
			12117,
			12119,
			12121,
			12123,
			12125,
			12127,
			12129,
			12131,
			12133,
			13001,
			13003,
			13005,
			13007,
			13009,
			13011,
			13013,
			13015,
			13017,
			13019,
			13021,
			13023,
			13025,
			13027,
			13029,
			13031,
			13033,
			13035,
			13037,
			13039,
			13043,
			13045,
			13047,
			13049,
			13051,
			13053,
			13055,
			13057,
			13059,
			13061,
			13063,
			13065,
			13067,
			13069,
			13071,
			13073,
			13075,
			13077,
			13079,
			13081,
			13083,
			13085,
			13087,
			13089,
			13091,
			13093,
			13095,
			13097,
			13099,
			13101,
			13103,
			13105,
			13107,
			13109,
			13111,
			13113,
			13115,
			13117,
			13119,
			13121,
			13123,
			13125,
			13127,
			13129,
			13131,
			13133,
			13135,
			13137,
			13139,
			13141,
			13143,
			13145,
			13147,
			13149,
			13151,
			13153,
			13155,
			13157,
			13159,
			13161,
			13163,
			13165,
			13167,
			13169,
			13171,
			13173,
			13175,
			13177,
			13179,
			13181,
			13183,
			13185,
			13187,
			13189,
			13191,
			13193,
			13195,
			13197,
			13199,
			13201,
			13205,
			13207,
			13209,
			13211,
			13213,
			13215,
			13217,
			13219,
			13221,
			13223,
			13225,
			13227,
			13229,
			13231,
			13233,
			13235,
			13237,
			13239,
			13241,
			13243,
			13245,
			13247,
			13249,
			13251,
			13253,
			13255,
			13257,
			13259,
			13261,
			13263,
			13265,
			13267,
			13269,
			13271,
			13273,
			13275,
			13277,
			13279,
			13281,
			13283,
			13285,
			13287,
			13289,
			13291,
			13293,
			13295,
			13297,
			13299,
			13301,
			13303,
			13305,
			13307,
			13309,
			13311,
			13313,
			13315,
			13317,
			13319,
			13321,
			15001,
			15003,
			15007,
			15009,
			16001,
			16003,
			16005,
			16007,
			16009,
			16011,
			16013,
			16015,
			16017,
			16019,
			16021,
			16023,
			16025,
			16027,
			16029,
			16031,
			16033,
			16035,
			16037,
			16039,
			16041,
			16043,
			16045,
			16047,
			16049,
			16051,
			16053,
			16055,
			16057,
			16059,
			16061,
			16063,
			16065,
			16067,
			16069,
			16071,
			16073,
			16075,
			16077,
			16079,
			16081,
			16083,
			16085,
			16087,
			17001,
			17003,
			17005,
			17007,
			17009,
			17011,
			17013,
			17015,
			17017,
			17019,
			17021,
			17023,
			17025,
			17027,
			17029,
			17031,
			17033,
			17035,
			17037,
			17039,
			17041,
			17043,
			17045,
			17047,
			17049,
			17051,
			17053,
			17055,
			17057,
			17059,
			17061,
			17063,
			17065,
			17067,
			17069,
			17071,
			17073,
			17075,
			17077,
			17079,
			17081,
			17083,
			17085,
			17087,
			17089,
			17091,
			17093,
			17095,
			17097,
			17099,
			17101,
			17103,
			17105,
			17107,
			17109,
			17111,
			17113,
			17115,
			17117,
			17119,
			17121,
			17123,
			17125,
			17127,
			17129,
			17131,
			17133,
			17135,
			17137,
			17139,
			17141,
			17143,
			17145,
			17147,
			17149,
			17151,
			17153,
			17155,
			17157,
			17159,
			17161,
			17163,
			17165,
			17167,
			17169,
			17171,
			17173,
			17175,
			17177,
			17179,
			17181,
			17183,
			17185,
			17187,
			17189,
			17191,
			17193,
			17195,
			17197,
			17199,
			17201,
			17203,
			18001,
			18003,
			18005,
			18007,
			18009,
			18011,
			18013,
			18015,
			18017,
			18019,
			18021,
			18023,
			18025,
			18027,
			18029,
			18031,
			18033,
			18035,
			18037,
			18039,
			18041,
			18043,
			18045,
			18047,
			18049,
			18051,
			18053,
			18055,
			18057,
			18059,
			18061,
			18063,
			18065,
			18067,
			18069,
			18071,
			18073,
			18075,
			18077,
			18079,
			18081,
			18083,
			18085,
			18087,
			18089,
			18091,
			18093,
			18095,
			18097,
			18099,
			18101,
			18103,
			18105,
			18107,
			18109,
			18111,
			18113,
			18115,
			18117,
			18119,
			18121,
			18123,
			18125,
			18127,
			18129,
			18131,
			18133,
			18135,
			18137,
			18139,
			18141,
			18143,
			18145,
			18147,
			18149,
			18151,
			18153,
			18155,
			18157,
			18159,
			18161,
			18163,
			18165,
			18167,
			18169,
			18171,
			18173,
			18175,
			18177,
			18179,
			18181,
			18183,
			19001,
			19003,
			19005,
			19007,
			19009,
			19011,
			19013,
			19015,
			19017,
			19019,
			19021,
			19023,
			19025,
			19027,
			19029,
			19031,
			19033,
			19035,
			19037,
			19039,
			19041,
			19043,
			19045,
			19047,
			19049,
			19051,
			19053,
			19055,
			19057,
			19059,
			19061,
			19063,
			19065,
			19067,
			19069,
			19071,
			19073,
			19075,
			19077,
			19079,
			19081,
			19083,
			19085,
			19087,
			19089,
			19091,
			19093,
			19095,
			19097,
			19099,
			19101,
			19103,
			19105,
			19107,
			19109,
			19111,
			19113,
			19115,
			19117,
			19119,
			19121,
			19123,
			19125,
			19127,
			19129,
			19131,
			19133,
			19135,
			19137,
			19139,
			19141,
			19143,
			19145,
			19147,
			19149,
			19151,
			19153,
			19155,
			19157,
			19159,
			19161,
			19163,
			19165,
			19167,
			19169,
			19171,
			19173,
			19175,
			19177,
			19179,
			19181,
			19183,
			19185,
			19187,
			19189,
			19191,
			19193,
			19195,
			19197,
			20001,
			20003,
			20005,
			20007,
			20009,
			20011,
			20013,
			20015,
			20017,
			20019,
			20021,
			20023,
			20025,
			20027,
			20029,
			20031,
			20033,
			20035,
			20037,
			20039,
			20041,
			20043,
			20045,
			20047,
			20049,
			20051,
			20053,
			20055,
			20057,
			20059,
			20061,
			20063,
			20065,
			20067,
			20069,
			20071,
			20073,
			20075,
			20077,
			20079,
			20081,
			20083,
			20085,
			20087,
			20089,
			20091,
			20093,
			20095,
			20097,
			20099,
			20101,
			20103,
			20105,
			20107,
			20109,
			20111,
			20113,
			20115,
			20117,
			20119,
			20121,
			20123,
			20125,
			20127,
			20129,
			20131,
			20133,
			20135,
			20137,
			20139,
			20141,
			20143,
			20145,
			20147,
			20149,
			20151,
			20153,
			20155,
			20157,
			20159,
			20161,
			20163,
			20165,
			20167,
			20169,
			20171,
			20173,
			20175,
			20177,
			20179,
			20181,
			20183,
			20185,
			20187,
			20189,
			20191,
			20193,
			20195,
			20197,
			20199,
			20201,
			20203,
			20205,
			20207,
			20209,
			21001,
			21003,
			21005,
			21007,
			21009,
			21011,
			21013,
			21015,
			21017,
			21019,
			21021,
			21023,
			21025,
			21027,
			21029,
			21031,
			21033,
			21035,
			21037,
			21039,
			21041,
			21043,
			21045,
			21047,
			21049,
			21051,
			21053,
			21055,
			21057,
			21059,
			21061,
			21063,
			21065,
			21067,
			21069,
			21071,
			21073,
			21075,
			21077,
			21079,
			21081,
			21083,
			21085,
			21087,
			21089,
			21091,
			21093,
			21095,
			21097,
			21099,
			21101,
			21103,
			21105,
			21107,
			21109,
			21111,
			21113,
			21115,
			21117,
			21119,
			21121,
			21123,
			21125,
			21127,
			21129,
			21131,
			21133,
			21135,
			21137,
			21139,
			21141,
			21143,
			21145,
			21147,
			21149,
			21151,
			21153,
			21155,
			21157,
			21159,
			21161,
			21163,
			21165,
			21167,
			21169,
			21171,
			21173,
			21175,
			21177,
			21179,
			21181,
			21183,
			21185,
			21187,
			21189,
			21191,
			21193,
			21195,
			21197,
			21199,
			21201,
			21203,
			21205,
			21207,
			21209,
			21211,
			21213,
			21215,
			21217,
			21219,
			21221,
			21223,
			21225,
			21227,
			21229,
			21231,
			21233,
			21235,
			21237,
			21239,
			22001,
			22003,
			22005,
			22007,
			22009,
			22011,
			22013,
			22015,
			22017,
			22019,
			22021,
			22023,
			22025,
			22027,
			22029,
			22031,
			22033,
			22035,
			22037,
			22039,
			22041,
			22043,
			22045,
			22047,
			22049,
			22051,
			22053,
			22055,
			22057,
			22059,
			22061,
			22063,
			22065,
			22067,
			22069,
			22071,
			22073,
			22075,
			22077,
			22079,
			22081,
			22083,
			22085,
			22087,
			22089,
			22091,
			22093,
			22095,
			22097,
			22099,
			22101,
			22103,
			22105,
			22107,
			22109,
			22111,
			22113,
			22115,
			22117,
			22119,
			22121,
			22123,
			22125,
			22127,
			23001,
			23003,
			23005,
			23007,
			23009,
			23011,
			23013,
			23015,
			23017,
			23019,
			23021,
			23023,
			23025,
			23027,
			23029,
			23031,
			24001,
			24003,
			24005,
			24009,
			24011,
			24013,
			24015,
			24017,
			24019,
			24021,
			24023,
			24025,
			24027,
			24029,
			24031,
			24033,
			24035,
			24037,
			24039,
			24041,
			24043,
			24045,
			24047,
			24510,
			25001,
			25003,
			25005,
			25007,
			25009,
			25011,
			25013,
			25015,
			25017,
			25019,
			25021,
			25023,
			25025,
			25027,
			26001,
			26003,
			26005,
			26007,
			26009,
			26011,
			26013,
			26015,
			26017,
			26019,
			26021,
			26023,
			26025,
			26027,
			26029,
			26031,
			26033,
			26035,
			26037,
			26039,
			26041,
			26043,
			26045,
			26047,
			26049,
			26051,
			26053,
			26055,
			26057,
			26059,
			26061,
			26063,
			26065,
			26067,
			26069,
			26071,
			26073,
			26075,
			26077,
			26079,
			26081,
			26083,
			26085,
			26087,
			26089,
			26091,
			26093,
			26095,
			26097,
			26099,
			26101,
			26103,
			26105,
			26107,
			26109,
			26111,
			26113,
			26115,
			26117,
			26119,
			26121,
			26123,
			26125,
			26127,
			26129,
			26131,
			26133,
			26135,
			26137,
			26139,
			26141,
			26143,
			26145,
			26147,
			26149,
			26151,
			26153,
			26155,
			26157,
			26159,
			26161,
			26163,
			26165,
			27001,
			27003,
			27005,
			27007,
			27009,
			27011,
			27013,
			27015,
			27017,
			27019,
			27021,
			27023,
			27025,
			27027,
			27029,
			27031,
			27033,
			27035,
			27037,
			27039,
			27041,
			27043,
			27045,
			27047,
			27049,
			27051,
			27053,
			27055,
			27057,
			27059,
			27061,
			27063,
			27065,
			27067,
			27069,
			27071,
			27073,
			27075,
			27077,
			27079,
			27081,
			27083,
			27085,
			27087,
			27089,
			27091,
			27093,
			27095,
			27097,
			27099,
			27101,
			27103,
			27105,
			27107,
			27109,
			27111,
			27113,
			27115,
			27117,
			27119,
			27121,
			27123,
			27125,
			27127,
			27129,
			27131,
			27133,
			27135,
			27137,
			27139,
			27141,
			27143,
			27145,
			27147,
			27149,
			27151,
			27153,
			27155,
			27157,
			27159,
			27161,
			27163,
			27165,
			27167,
			27169,
			27171,
			27173,
			28001,
			28003,
			28005,
			28007,
			28009,
			28011,
			28013,
			28015,
			28017,
			28019,
			28021,
			28023,
			28025,
			28027,
			28029,
			28031,
			28033,
			28035,
			28037,
			28039,
			28041,
			28043,
			28045,
			28047,
			28049,
			28051,
			28053,
			28055,
			28057,
			28059,
			28061,
			28063,
			28065,
			28067,
			28069,
			28071,
			28073,
			28075,
			28077,
			28079,
			28081,
			28083,
			28085,
			28087,
			28089,
			28091,
			28093,
			28095,
			28097,
			28099,
			28101,
			28103,
			28105,
			28107,
			28109,
			28111,
			28113,
			28115,
			28117,
			28119,
			28121,
			28123,
			28125,
			28127,
			28129,
			28131,
			28133,
			28135,
			28137,
			28139,
			28141,
			28143,
			28145,
			28147,
			28149,
			28151,
			28153,
			28155,
			28157,
			28159,
			28161,
			28163,
			29001,
			29003,
			29005,
			29007,
			29009,
			29011,
			29013,
			29015,
			29017,
			29019,
			29021,
			29023,
			29025,
			29027,
			29029,
			29031,
			29033,
			29035,
			29037,
			29039,
			29041,
			29043,
			29045,
			29047,
			29049,
			29051,
			29053,
			29055,
			29057,
			29059,
			29061,
			29063,
			29065,
			29067,
			29069,
			29071,
			29073,
			29075,
			29077,
			29079,
			29081,
			29083,
			29085,
			29087,
			29089,
			29091,
			29093,
			29095,
			29097,
			29099,
			29101,
			29103,
			29105,
			29107,
			29109,
			29111,
			29113,
			29115,
			29117,
			29119,
			29121,
			29123,
			29125,
			29127,
			29129,
			29131,
			29133,
			29135,
			29137,
			29139,
			29141,
			29143,
			29145,
			29147,
			29149,
			29151,
			29153,
			29155,
			29157,
			29159,
			29161,
			29163,
			29165,
			29167,
			29169,
			29171,
			29173,
			29175,
			29177,
			29179,
			29181,
			29183,
			29185,
			29186,
			29187,
			29189,
			29195,
			29197,
			29199,
			29201,
			29203,
			29205,
			29207,
			29209,
			29211,
			29213,
			29215,
			29217,
			29219,
			29221,
			29223,
			29225,
			29227,
			29229,
			29510,
			30001,
			30003,
			30005,
			30007,
			30009,
			30011,
			30013,
			30015,
			30017,
			30019,
			30021,
			30023,
			30025,
			30027,
			30029,
			30031,
			30033,
			30035,
			30037,
			30039,
			30041,
			30043,
			30045,
			30047,
			30049,
			30051,
			30053,
			30055,
			30057,
			30059,
			30061,
			30063,
			30065,
			30067,
			30069,
			30071,
			30073,
			30075,
			30077,
			30079,
			30081,
			30083,
			30085,
			30087,
			30089,
			30091,
			30093,
			30095,
			30097,
			30099,
			30101,
			30103,
			30105,
			30107,
			30109,
			30111,
			31001,
			31003,
			31005,
			31007,
			31009,
			31011,
			31013,
			31015,
			31017,
			31019,
			31021,
			31023,
			31025,
			31027,
			31029,
			31031,
			31033,
			31035,
			31037,
			31039,
			31041,
			31043,
			31045,
			31047,
			31049,
			31051,
			31053,
			31055,
			31057,
			31059,
			31061,
			31063,
			31065,
			31067,
			31069,
			31071,
			31073,
			31075,
			31077,
			31079,
			31081,
			31083,
			31085,
			31087,
			31089,
			31091,
			31093,
			31095,
			31097,
			31099,
			31101,
			31103,
			31105,
			31107,
			31109,
			31111,
			31113,
			31115,
			31117,
			31119,
			31121,
			31123,
			31125,
			31127,
			31129,
			31131,
			31133,
			31135,
			31137,
			31139,
			31141,
			31143,
			31145,
			31147,
			31149,
			31151,
			31153,
			31155,
			31157,
			31159,
			31161,
			31163,
			31165,
			31167,
			31169,
			31171,
			31173,
			31175,
			31177,
			31179,
			31181,
			31183,
			31185,
			32001,
			32003,
			32005,
			32007,
			32009,
			32011,
			32013,
			32015,
			32017,
			32019,
			32021,
			32023,
			32027,
			32029,
			32031,
			32033,
			32510,
			33001,
			33003,
			33005,
			33007,
			33009,
			33011,
			33013,
			33015,
			33017,
			33019,
			34001,
			34003,
			34005,
			34007,
			34009,
			34011,
			34013,
			34015,
			34017,
			34019,
			34021,
			34023,
			34025,
			34027,
			34029,
			34031,
			34033,
			34035,
			34037,
			34039,
			34041,
			35001,
			35003,
			35005,
			35006,
			35007,
			35009,
			35011,
			35013,
			35015,
			35017,
			35019,
			35021,
			35023,
			35025,
			35027,
			35028,
			35029,
			35031,
			35033,
			35035,
			35037,
			35039,
			35041,
			35043,
			35045,
			35047,
			35049,
			35051,
			35053,
			35055,
			35057,
			35059,
			35061,
			36001,
			36003,
			36005,
			36007,
			36009,
			36011,
			36013,
			36015,
			36017,
			36019,
			36021,
			36023,
			36025,
			36027,
			36029,
			36031,
			36033,
			36035,
			36037,
			36039,
			36041,
			36043,
			36045,
			36047,
			36049,
			36051,
			36053,
			36055,
			36057,
			36059,
			36061,
			36063,
			36065,
			36067,
			36069,
			36071,
			36073,
			36075,
			36077,
			36079,
			36081,
			36083,
			36085,
			36087,
			36089,
			36091,
			36093,
			36095,
			36097,
			36099,
			36101,
			36103,
			36105,
			36107,
			36109,
			36111,
			36113,
			36115,
			36117,
			36119,
			36121,
			36123,
			37001,
			37003,
			37005,
			37007,
			37009,
			37011,
			37013,
			37015,
			37017,
			37019,
			37021,
			37023,
			37025,
			37027,
			37029,
			37031,
			37033,
			37035,
			37037,
			37039,
			37041,
			37043,
			37045,
			37047,
			37049,
			37051,
			37053,
			37055,
			37057,
			37059,
			37061,
			37063,
			37065,
			37067,
			37069,
			37071,
			37073,
			37075,
			37077,
			37079,
			37081,
			37083,
			37085,
			37087,
			37089,
			37091,
			37093,
			37095,
			37097,
			37099,
			37101,
			37103,
			37105,
			37107,
			37109,
			37111,
			37113,
			37115,
			37117,
			37119,
			37121,
			37123,
			37125,
			37127,
			37129,
			37131,
			37133,
			37135,
			37137,
			37139,
			37141,
			37143,
			37145,
			37147,
			37149,
			37151,
			37153,
			37155,
			37157,
			37159,
			37161,
			37163,
			37165,
			37167,
			37169,
			37171,
			37173,
			37175,
			37177,
			37179,
			37181,
			37183,
			37185,
			37187,
			37189,
			37191,
			37193,
			37195,
			37197,
			37199,
			38001,
			38003,
			38005,
			38007,
			38009,
			38011,
			38013,
			38015,
			38017,
			38019,
			38021,
			38023,
			38025,
			38027,
			38029,
			38031,
			38033,
			38035,
			38037,
			38039,
			38041,
			38043,
			38045,
			38047,
			38049,
			38051,
			38053,
			38055,
			38057,
			38059,
			38061,
			38063,
			38065,
			38067,
			38069,
			38071,
			38073,
			38075,
			38077,
			38079,
			38081,
			38083,
			38085,
			38087,
			38089,
			38091,
			38093,
			38095,
			38097,
			38099,
			38101,
			38103,
			38105,
			39001,
			39003,
			39005,
			39007,
			39009,
			39011,
			39013,
			39015,
			39017,
			39019,
			39021,
			39023,
			39025,
			39027,
			39029,
			39031,
			39033,
			39035,
			39037,
			39039,
			39041,
			39043,
			39045,
			39047,
			39049,
			39051,
			39053,
			39055,
			39057,
			39059,
			39061,
			39063,
			39065,
			39067,
			39069,
			39071,
			39073,
			39075,
			39077,
			39079,
			39081,
			39083,
			39085,
			39087,
			39089,
			39091,
			39093,
			39095,
			39097,
			39099,
			39101,
			39103,
			39105,
			39107,
			39109,
			39111,
			39113,
			39115,
			39117,
			39119,
			39121,
			39123,
			39125,
			39127,
			39129,
			39131,
			39133,
			39135,
			39137,
			39139,
			39141,
			39143,
			39145,
			39147,
			39149,
			39151,
			39153,
			39155,
			39157,
			39159,
			39161,
			39163,
			39165,
			39167,
			39169,
			39171,
			39173,
			39175,
			40001,
			40003,
			40005,
			40007,
			40009,
			40011,
			40013,
			40015,
			40017,
			40019,
			40021,
			40023,
			40025,
			40027,
			40029,
			40031,
			40033,
			40035,
			40037,
			40039,
			40041,
			40043,
			40045,
			40047,
			40049,
			40051,
			40053,
			40055,
			40057,
			40059,
			40061,
			40063,
			40065,
			40067,
			40069,
			40071,
			40073,
			40075,
			40077,
			40079,
			40081,
			40083,
			40085,
			40087,
			40089,
			40091,
			40093,
			40095,
			40097,
			40099,
			40101,
			40103,
			40105,
			40107,
			40109,
			40111,
			40113,
			40115,
			40117,
			40119,
			40121,
			40123,
			40125,
			40127,
			40129,
			40131,
			40133,
			40135,
			40137,
			40139,
			40141,
			40143,
			40145,
			40147,
			40149,
			40151,
			40153,
			41001,
			41003,
			41005,
			41007,
			41009,
			41011,
			41013,
			41015,
			41017,
			41019,
			41021,
			41023,
			41025,
			41027,
			41029,
			41031,
			41033,
			41035,
			41037,
			41039,
			41041,
			41043,
			41045,
			41047,
			41049,
			41051,
			41053,
			41055,
			41057,
			41059,
			41061,
			41063,
			41065,
			41067,
			41069,
			41071,
			42001,
			42003,
			42005,
			42007,
			42009,
			42011,
			42013,
			42015,
			42017,
			42019,
			42021,
			42023,
			42025,
			42027,
			42029,
			42031,
			42033,
			42035,
			42037,
			42039,
			42041,
			42043,
			42045,
			42047,
			42049,
			42051,
			42053,
			42055,
			42057,
			42059,
			42061,
			42063,
			42065,
			42067,
			42069,
			42071,
			42073,
			42075,
			42077,
			42079,
			42081,
			42083,
			42085,
			42087,
			42089,
			42091,
			42093,
			42095,
			42097,
			42099,
			42101,
			42103,
			42105,
			42107,
			42109,
			42111,
			42113,
			42115,
			42117,
			42119,
			42121,
			42123,
			42125,
			42127,
			42129,
			42131,
			42133,
			44001,
			44003,
			44005,
			44007,
			44009,
			45001,
			45003,
			45005,
			45007,
			45009,
			45011,
			45013,
			45015,
			45017,
			45019,
			45021,
			45023,
			45025,
			45027,
			45029,
			45031,
			45033,
			45035,
			45037,
			45039,
			45041,
			45043,
			45045,
			45047,
			45049,
			45051,
			45053,
			45055,
			45057,
			45059,
			45061,
			45063,
			45065,
			45067,
			45069,
			45071,
			45073,
			45075,
			45077,
			45079,
			45081,
			45083,
			45085,
			45087,
			45089,
			45091,
			46003,
			46005,
			46007,
			46009,
			46011,
			46013,
			46015,
			46017,
			46019,
			46021,
			46023,
			46025,
			46027,
			46029,
			46031,
			46033,
			46035,
			46037,
			46039,
			46041,
			46043,
			46045,
			46047,
			46049,
			46051,
			46053,
			46055,
			46057,
			46059,
			46061,
			46063,
			46065,
			46067,
			46069,
			46071,
			46073,
			46075,
			46077,
			46079,
			46081,
			46083,
			46085,
			46087,
			46089,
			46091,
			46093,
			46095,
			46097,
			46099,
			46101,
			46103,
			46105,
			46107,
			46109,
			46111,
			46113,
			46115,
			46117,
			46119,
			46121,
			46123,
			46125,
			46127,
			46129,
			46135,
			46137,
			47001,
			47003,
			47005,
			47007,
			47009,
			47011,
			47013,
			47015,
			47017,
			47019,
			47021,
			47023,
			47025,
			47027,
			47029,
			47031,
			47033,
			47035,
			47037,
			47039,
			47041,
			47043,
			47045,
			47047,
			47049,
			47051,
			47053,
			47055,
			47057,
			47059,
			47061,
			47063,
			47065,
			47067,
			47069,
			47071,
			47073,
			47075,
			47077,
			47079,
			47081,
			47083,
			47085,
			47087,
			47089,
			47091,
			47093,
			47095,
			47097,
			47099,
			47101,
			47103,
			47105,
			47107,
			47109,
			47111,
			47113,
			47115,
			47117,
			47119,
			47121,
			47123,
			47125,
			47127,
			47129,
			47131,
			47133,
			47135,
			47137,
			47139,
			47141,
			47143,
			47145,
			47147,
			47149,
			47151,
			47153,
			47155,
			47157,
			47159,
			47161,
			47163,
			47165,
			47167,
			47169,
			47171,
			47173,
			47175,
			47177,
			47179,
			47181,
			47183,
			47185,
			47187,
			47189,
			48001,
			48003,
			48005,
			48007,
			48009,
			48011,
			48013,
			48015,
			48017,
			48019,
			48021,
			48023,
			48025,
			48027,
			48029,
			48031,
			48033,
			48035,
			48037,
			48039,
			48041,
			48043,
			48045,
			48047,
			48049,
			48051,
			48053,
			48055,
			48057,
			48059,
			48061,
			48063,
			48065,
			48067,
			48069,
			48071,
			48073,
			48075,
			48077,
			48079,
			48081,
			48083,
			48085,
			48087,
			48089,
			48091,
			48093,
			48095,
			48097,
			48099,
			48101,
			48103,
			48105,
			48107,
			48109,
			48111,
			48113,
			48115,
			48117,
			48119,
			48121,
			48123,
			48125,
			48127,
			48129,
			48131,
			48133,
			48135,
			48137,
			48139,
			48141,
			48143,
			48145,
			48147,
			48149,
			48151,
			48153,
			48155,
			48157,
			48159,
			48161,
			48163,
			48165,
			48167,
			48169,
			48171,
			48173,
			48175,
			48177,
			48179,
			48181,
			48183,
			48185,
			48187,
			48189,
			48191,
			48193,
			48195,
			48197,
			48199,
			48201,
			48203,
			48205,
			48207,
			48209,
			48211,
			48213,
			48215,
			48217,
			48219,
			48221,
			48223,
			48225,
			48227,
			48229,
			48231,
			48233,
			48235,
			48237,
			48239,
			48241,
			48243,
			48245,
			48247,
			48249,
			48251,
			48253,
			48255,
			48257,
			48259,
			48261,
			48263,
			48265,
			48267,
			48269,
			48271,
			48273,
			48275,
			48277,
			48279,
			48281,
			48283,
			48285,
			48287,
			48289,
			48291,
			48293,
			48295,
			48297,
			48299,
			48301,
			48303,
			48305,
			48307,
			48309,
			48311,
			48313,
			48315,
			48317,
			48319,
			48321,
			48323,
			48325,
			48327,
			48329,
			48331,
			48333,
			48335,
			48337,
			48339,
			48341,
			48343,
			48345,
			48347,
			48349,
			48351,
			48353,
			48355,
			48357,
			48359,
			48361,
			48363,
			48365,
			48367,
			48369,
			48371,
			48373,
			48375,
			48377,
			48379,
			48381,
			48383,
			48385,
			48387,
			48389,
			48391,
			48393,
			48395,
			48397,
			48399,
			48401,
			48403,
			48405,
			48407,
			48409,
			48411,
			48413,
			48415,
			48417,
			48419,
			48421,
			48423,
			48425,
			48427,
			48429,
			48431,
			48433,
			48435,
			48437,
			48439,
			48441,
			48443,
			48445,
			48447,
			48449,
			48451,
			48453,
			48455,
			48457,
			48459,
			48461,
			48463,
			48465,
			48467,
			48469,
			48471,
			48473,
			48475,
			48477,
			48479,
			48481,
			48483,
			48485,
			48487,
			48489,
			48491,
			48493,
			48495,
			48497,
			48499,
			48501,
			48503,
			48505,
			48507,
			49001,
			49003,
			49005,
			49007,
			49009,
			49011,
			49013,
			49015,
			49017,
			49019,
			49021,
			49023,
			49025,
			49027,
			49029,
			49031,
			49033,
			49035,
			49037,
			49039,
			49041,
			49043,
			49045,
			49047,
			49049,
			49051,
			49053,
			49055,
			49057,
			50001,
			50003,
			50005,
			50007,
			50009,
			50011,
			50013,
			50015,
			50017,
			50019,
			50021,
			50023,
			50025,
			50027,
			51001,
			51003,
			51005,
			51007,
			51009,
			51011,
			51013,
			51015,
			51017,
			51019,
			51021,
			51023,
			51025,
			51027,
			51029,
			51031,
			51033,
			51035,
			51036,
			51037,
			51041,
			51043,
			51045,
			51047,
			51049,
			51051,
			51053,
			51057,
			51059,
			51061,
			51063,
			51065,
			51067,
			51069,
			51071,
			51073,
			51075,
			51077,
			51079,
			51081,
			51083,
			51085,
			51087,
			51089,
			51091,
			51093,
			51095,
			51097,
			51099,
			51101,
			51103,
			51105,
			51107,
			51109,
			51111,
			51113,
			51115,
			51117,
			51119,
			51121,
			51125,
			51127,
			51131,
			51133,
			51135,
			51137,
			51139,
			51141,
			51143,
			51145,
			51147,
			51149,
			51153,
			51155,
			51157,
			51159,
			51161,
			51163,
			51165,
			51167,
			51169,
			51171,
			51173,
			51175,
			51177,
			51179,
			51181,
			51183,
			51185,
			51187,
			51191,
			51193,
			51195,
			51197,
			51199,
			51510,
			51515,
			51520,
			51530,
			51540,
			51550,
			51570,
			51580,
			51590,
			51595,
			51600,
			51610,
			51620,
			51630,
			51640,
			51650,
			51660,
			51670,
			51678,
			51680,
			51683,
			51685,
			51690,
			51700,
			51710,
			51720,
			51730,
			51735,
			51740,
			51750,
			51760,
			51770,
			51775,
			51790,
			51800,
			51810,
			51820,
			51830,
			51840,
			53001,
			53003,
			53005,
			53007,
			53009,
			53011,
			53013,
			53015,
			53017,
			53019,
			53021,
			53023,
			53025,
			53027,
			53029,
			53031,
			53033,
			53035,
			53037,
			53039,
			53041,
			53043,
			53045,
			53047,
			53049,
			53051,
			53053,
			53055,
			53057,
			53059,
			53061,
			53063,
			53065,
			53067,
			53069,
			53071,
			53073,
			53075,
			53077,
			54001,
			54003,
			54005,
			54007,
			54009,
			54011,
			54013,
			54015,
			54017,
			54019,
			54021,
			54023,
			54025,
			54027,
			54029,
			54031,
			54033,
			54035,
			54037,
			54039,
			54041,
			54043,
			54045,
			54047,
			54049,
			54051,
			54053,
			54055,
			54057,
			54059,
			54061,
			54063,
			54065,
			54067,
			54069,
			54071,
			54073,
			54075,
			54077,
			54079,
			54081,
			54083,
			54085,
			54087,
			54089,
			54091,
			54093,
			54095,
			54097,
			54099,
			54101,
			54103,
			54105,
			54107,
			54109,
			55001,
			55003,
			55005,
			55007,
			55009,
			55011,
			55013,
			55015,
			55017,
			55019,
			55021,
			55023,
			55025,
			55027,
			55029,
			55031,
			55033,
			55035,
			55037,
			55039,
			55041,
			55043,
			55045,
			55047,
			55049,
			55051,
			55053,
			55055,
			55057,
			55059,
			55061,
			55063,
			55065,
			55067,
			55069,
			55071,
			55073,
			55075,
			55077,
			55078,
			55079,
			55081,
			55083,
			55085,
			55087,
			55089,
			55091,
			55093,
			55095,
			55097,
			55099,
			55101,
			55103,
			55105,
			55107,
			55109,
			55111,
			55113,
			55115,
			55117,
			55119,
			55121,
			55123,
			55125,
			55127,
			55129,
			55131,
			55133,
			55135,
			55137,
			55139,
			55141,
			56001,
			56003,
			56005,
			56007,
			56009,
			56011,
			56013,
			56015,
			56017,
			56019,
			56021,
			56023,
			56025,
			56027,
			56029,
			56031,
			56033,
			56035,
			56037,
			56039,
			56041,
			56043,
			56045,
			72001,
			72003,
			72005,
			72007,
			72009,
			72011,
			72013,
			72015,
			72017,
			72019,
			72021,
			72023,
			72025,
			72027,
			72029,
			72031,
			72033,
			72035,
			72037,
			72039,
			72041,
			72043,
			72045,
			72047,
			72049,
			72051,
			72053,
			72054,
			72055,
			72057,
			72059,
			72061,
			72063,
			72065,
			72067,
			72069,
			72071,
			72073,
			72075,
			72077,
			72079,
			72081,
			72083,
			72085,
			72087,
			72089,
			72091,
			72093,
			72095,
			72097,
			72099,
			72101,
			72103,
			72105,
			72107,
			72109,
			72111,
			72113,
			72115,
			72117,
			72119,
			72121,
			72123,
			72125,
			72127,
			72129,
			72131,
			72133,
			72135,
			72137,
			72139,
			72141,
			72143,
			72145,
			72147,
			72149,
			72151,
			72153
		};

		return list;
	}
}
