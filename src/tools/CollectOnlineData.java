package tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/**
 * 
 * @author shejny
 *
 *	Main tool entry point.
 *	Parse command line parameters and initiates the collection.
 */
public class CollectOnlineData {

	public CollectOnlineData() {
	}

	public static void main(String[] args) {
		parseCommandLine(args);
		if (arguments.get("help") != null || options.get("h") != null || (arguments.size() < 4 && arguments.get("config") == null)) {
			System.out.println("Usage:\njava -jar adwords.jar --config <ads.properties path> --refreshtoken \"refreshtoken\" --obsWindow 8 --lag 24 --dbName js871 --provider [adwords|bing] --debug true --removeDownloadedReportFile true \n\nor the minimum parameters (using values from config file)\n\n");
			System.out.println("java -jar adwords.jar --config ads.properties --provider [adwords|bing]");
			System.out.println("Parameter explanation:		\n\n"+
				"--refreshToken  Refresh token for the client\n"+
				"--obsWindow     Observation window. Program should download the statistics for the duration of XXX hours\n"+
				"--lag			 number of hours the collection is delayed\n"+
				"--dbName        Name of the database to save results in (collection name is \"costs_adwords\")\n"+
				"--domainName    Name of the subdomain within AdWords account for which to download data from.\n"+
				"--config        full or relative path to ads.propeties file with all configuration entries\n"+
				"--provider      [adwords|bing|all] tell the tool which API to use. 'all' means both (all) implemented API providers\n"+
				"--reportDirectory      directory (used by Bing Ads where to store temporary downloaded files)\n"+
				"--removeDownloadedReportFile [true|false] indicate if to keep or delete downloaded temporary files\n"+
				"--loadFromFile  <fileName> a CSV file from which to load the data (do not download from API but use the CSV file instead). This parameter is not compatible with '--provider all'  option\n"+
				".");
			System.exit(0);
		}
		boolean debug = options.get("verbose") != null;
		System.setProperty("com.sun.xml.ws.transport.http.client.HttpTransportPipe.dump", arguments.getOrDefault("debug", adsProperties.getProperty("collection.SOAP.debug.enable","false")));
		String provider = arguments.getOrDefault("provider", adsProperties.getProperty("collection.provider", "all")).toLowerCase();
		if ("all".equalsIgnoreCase(provider)) {
			arguments.remove("loadFromFile"); // this is not compatible with provider == all
			Collectable collector = new Adwords(cfgFile, adsProperties);
			collector.setDebug(debug);
			arguments.put("provider", "adwords");
			int count = collector.collect(arguments);
			System.out.println("Collected "+count+" values from "+collector.getDescription());
			collector = new Bing(adsProperties);
			collector.setDebug(debug);
			arguments.put("provider", "bing");
			count = collector.collect(arguments);
			System.out.println("Collected "+count+" values from "+collector.getDescription());
		} else {
			Collectable collector = "bing".equalsIgnoreCase(arguments.get("provider")) ? new Bing(adsProperties) : new Adwords(cfgFile, adsProperties);
			collector.setDebug(debug);
			int count = collector.collect(arguments);
			System.out.println("Collected "+count+" values from "+collector.getDescription());
		}
	}

	private static List<String>       parameters = new ArrayList<>();
	private static Map<String,String> arguments  = new HashMap<>();
	private static Map<String,String> options    = new HashMap<>();
	private static Properties 		  adsProperties;
	private static File				  cfgFile;
	
	/**
	 * process the command line input
	 * @param args
	 */
	private static void parseCommandLine(String[] args) {
		if (args != null) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].startsWith("--")) {
					String key = args[i].replaceFirst("--", "");
					String val = i < args.length - 1 ? args[++i] : "";
					arguments.put(key,  val);
				} else if (args[i].startsWith("-")) {
					String key = args[i].replaceFirst("-", "");
					String val =  "";
					options.put(key,  val);
				} else {
					parameters.add(args[i]);
				}
			}
		}
		if (arguments.get("config") != null) {
			String fileName = arguments.get("config");
			cfgFile = new File(fileName);
			if (cfgFile.exists()) {
				adsProperties = new Properties();
				try {
					adsProperties.load(new FileInputStream(cfgFile));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		if (adsProperties == null) {
			adsProperties = new Properties();
			try {
				System.out.println("Loading deafult properties...");
				adsProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("/ads.properties"));
			} catch (Exception e) {
				e.printStackTrace();
				try {
					adsProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("/ads.properties"));
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
	}
}
