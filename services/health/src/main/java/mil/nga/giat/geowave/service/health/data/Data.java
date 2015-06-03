//package mil.nga.giat.geowave.service.health.data;
//
//import java.io.BufferedReader;
//import java.io.FileNotFoundException;
//import java.io.InputStreamReader;
//import java.net.URL;
//
//import javax.xml.xpath.XPath;
//import javax.xml.xpath.XPathConstants;
//import javax.xml.xpath.XPathExpressionException;
//import javax.xml.xpath.XPathFactory;
//
//import org.w3c.dom.NodeList;
//import org.xml.sax.InputSource;
//
///**
// * Servlet implementation class MainServlet
// */
//public class Data {
//
//	/**
//	 * Default constructor.
//	 */
//	public Data() {
//		// TODO Auto-generated constructor stub
//	}
//
//	public static void main(String[] args) {
//		Data d = new Data();
//		System.out.println(d.read());
//	}
//
//	public String read() {
//		try {
//			URL path = new URL("http://localhost:50095/");
//
//			BufferedReader in = new BufferedReader(new InputStreamReader(
//			 path.openStream()));
//
//
//			return parse(in);
//		} catch (Exception e) {
////			System.out.println(e.getMessage());
//			return null;
//		}
//	}
//
//	public String parse(BufferedReader input) throws XPathExpressionException,
//			FileNotFoundException {
//		XPath xpath = XPathFactory.newInstance().newXPath();
//		String expression = "//body//*/script";
//		InputSource inputSource = new InputSource(input);
//		
//		NodeList nodes = (NodeList) xpath.evaluate(expression, inputSource,
//				XPathConstants.NODESET);
//		System.out.println(nodes.getLength());
//		int cnt = 0;
//		String json = "{";
//		for (int i = 0; i < nodes.getLength(); i++) {
//
//			String val = nodes.item(i).getTextContent();
//			val = val.replace("$(function () {", "");
//			int in = val.indexOf("$.plot");
//			// System.out.println(in);
//			if (in > 0) {
//				val = val.substring(0, in);
//				val = val.trim();
//
//				if (!val.equals("") && !val.equals("\n") && !val.equals(" ")) {
//					// cnt++;
//					if (i == 1) {
//						String vals[] = val.split(";");
//						System.out.println(vals[0].trim().replace("d0",
//								"d" + cnt));
//						vals[0] = vals[0].replace(";", "");
//						json += "\"d" + cnt + "\" : "
//								+ vals[0].trim().split("=")[1] + ",";
//
//						cnt++;
//
//						System.out.println(vals[1].trim().replace("d1",
//								"d" + cnt));
//						vals[1] = vals[1].replace(";", "");
//						json += "\"d" + cnt + "\" : "
//								+ vals[1].trim().split("=")[1] + ",";
//
//					} else {
//
//						// String v=val.replace("d0", "d" + cnt);
//						System.out.println(val.replace("d0", "d" + cnt));
//						// write.println(val.replace("d0", "d" + cnt));
//						val = val.replace(";", "");
//						json += "\"d" + cnt + "\" : " + val.split("=")[1] + ",";
//					}
//					cnt++;
//
//				}
//			}
//			System.out.println();
//		}
//		json += "\"a\":123}";
//		// write.println(json);
//		return json;
//	}
// }