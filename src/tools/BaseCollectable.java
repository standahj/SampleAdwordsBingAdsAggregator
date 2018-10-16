package tools;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;

/**
 * Common functions for both Adwords and Bing collections
 * 
 * @author shejny
 *
 */
public abstract class BaseCollectable {

	public static final SimpleDateFormat TIMESTAMP_PARSER = new SimpleDateFormat("dd-MMM-yy");
	public static final SimpleDateFormat TIMESTAMP_PARSER_1 = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat TIMESTAMP_PARSER_2 = new SimpleDateFormat("yyyy-MMM-dd");
	public static final SimpleDateFormat TIMESTAMP_PARSER_3 = new SimpleDateFormat("dd-MM-yyyy");
	public static final SimpleDateFormat TIMESTAMP_PARSER_4 = new SimpleDateFormat("dd-MMM-yyyy");
	public static final SimpleDateFormat TIMESTAMP_PARSER_H = new SimpleDateFormat("dd-MMM-yy HH");
	public static final SimpleDateFormat TIMESTAMP_PARSER_1H = new SimpleDateFormat("yyyy-MM-dd HH");
	public static final SimpleDateFormat TIMESTAMP_PARSER_2H = new SimpleDateFormat("yyyy-MMM-dd HH");
	public static final SimpleDateFormat TIMESTAMP_PARSER_3H = new SimpleDateFormat("dd-MM-yyyy HH");
	public static final SimpleDateFormat TIMESTAMP_PARSER_4H = new SimpleDateFormat("dd-MMM-yyyy HH");

	public static final SimpleDateFormat TIMESTAMP_GREGORIAN_PARSER = new SimpleDateFormat("MM/dd/yyyy");
	public static final SimpleDateFormat TIMESTAMP_GREGORIAN_PARSER_H = new SimpleDateFormat("MM/dd/yyyy HH");

	public static final SimpleDateFormat TIMESTAMP_PARSER_5 = new SimpleDateFormat("MMM dd, yyyy");
	public static final SimpleDateFormat TIMESTAMP_PARSER_5H = new SimpleDateFormat("MMM dd, yyyy HH");


	protected String refreshToken;
	protected int    obsWindow = 12;
	protected String dbName;
	protected String domainName;
	protected String provider;
	protected int    lag = 24;
	protected String loadFromFile = null;
	
	protected MongoClient 				mongoClient;
	protected MongoDatabase 			mongoDb;
	protected MongoCollection<Document> table;

	protected CSVReader 			 	csvReader = null;
	protected Properties 				adsProperties; 

	private Map<String,Long>		 allAdGroupsMap = new HashMap<>();
	private Map<String,Long>		 allAdsMap = new HashMap<>();
	private Map<String,Long> 		 allCampaignsMap = new HashMap<>();

	static {
		// ensure the UTC date
		TIMESTAMP_PARSER.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_1.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_2.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_3.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_4.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_5.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_H.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_1H.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_2H.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_3H.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_4H.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_PARSER_5H.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_GREGORIAN_PARSER.setTimeZone(TimeZone.getTimeZone("UTC"));
		TIMESTAMP_GREGORIAN_PARSER_H.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	public BaseCollectable() {
	}

	/**
	 * Dates come as a day string plus hour of day. Day string may depend on user locale set in Google profile, so this function
	 * tries to discover and parse the most common formats.
	 * 
	 * It also uses from/to range to examine the string and detect the day, month, year and it's order in the date String.
	 * 
	 * @param strDate
	 * @param hour
	 * @param from
	 * @param to
	 * @return
	 */
	private Date parseDate(String strDate, String hour, Calendar from, Calendar to) {
		String fmt = null;
		if (strDate != null && from != null && to != null) {
			try {
				int YF = from.get(Calendar.YEAR);
				int MF = from.get(Calendar.MONTH)+1;
				int DF = from.get(Calendar.DAY_OF_MONTH);
				int YT = to.get(Calendar.YEAR);
				int MT = to.get(Calendar.MONTH)+1;
				int DT = to.get(Calendar.DAY_OF_MONTH);
				String[] t = strDate.split("[-/]");
				String[] f = new String[3];
				int A = Integer.parseInt(t[0]);
				int B = Integer.parseInt(t[1]);
				int C = Integer.parseInt(t[2]);
				String s = strDate.substring(t[0].length(), t[0].length()+1);
				if (YF == A || YT == A) {
					f[0] = "yyyy";
					if (((MF == B || MT == B) && (DF == C || DT == C)) || C > 12) {
						f[1] = "MM";
						f[2] = "dd";
					} else {
						f[1] = "dd";
						f[2] = "MM";
					}
				} else 	if (YF == C || YT == C) {
					f[2] = "yyyy";
					if (((MF == B || MT == B) && (DF == A || DT == A)) || A > 12) {
						f[1] = "MM";
						f[0] = "dd";
					} else {
						f[1] = "dd";
						f[0] = "MM";
					}
				} else	if (YF == B || YT == B) {
					f[1] = "yyyy";
					if ((MF == C || MT == C) && (DF == A || DT == A)) {
						f[2] = "MM";
						f[0] = "dd";
					} else {
						f[2] = "dd";
						f[0] = "MM";
					}
				}
				fmt = f[0]+s+f[1]+s+f[2];
			} catch (Exception pe) {

			}
		} 
		Date date = null;
		if (hour != null && hour.length() > 0 && !"--".equals(hour)) {
			strDate = strDate + " " + hour;
			try {
				SimpleDateFormat customDF = new SimpleDateFormat(fmt+" HH");
				customDF.setTimeZone(TimeZone.getTimeZone("UTC"));
				date = customDF.parse(strDate);
			} catch (Exception fe) {
				try {
					date = TIMESTAMP_PARSER_H.parse(strDate);
				} catch (Exception e) {
					try {
						date = TIMESTAMP_PARSER_1H.parse(strDate);
					} catch (Exception e1) {
						try {
							date = TIMESTAMP_PARSER_4H.parse(strDate);
						} catch (Exception e2) {
							try {
								date = TIMESTAMP_PARSER_3H.parse(strDate);
							} catch (Exception e3) {
								try {
									date = TIMESTAMP_PARSER_2H.parse(strDate);
								} catch (Exception e4) {
									try {
										date = TIMESTAMP_PARSER_5H.parse(strDate);
									} catch (Exception e5) {
										try {
											date = TIMESTAMP_GREGORIAN_PARSER_H.parse(strDate);
										} catch (Exception eg) {

										}
									}
								}
							}
						}
					}
				}
			} 
		} else {
			try {
				SimpleDateFormat customDF = new SimpleDateFormat(fmt);
				customDF.setTimeZone(TimeZone.getTimeZone("UTC"));
				date = customDF.parse(strDate);
			} catch (Exception fe) {
				try {
					date = TIMESTAMP_PARSER.parse(strDate);
				} catch (Exception e) {
					try {
						date = TIMESTAMP_PARSER_1.parse(strDate);
					} catch (Exception e1) {
						try {
							date = TIMESTAMP_PARSER_4.parse(strDate);
						} catch (Exception e2) {
							try {
								date = TIMESTAMP_PARSER_3.parse(strDate);
							} catch (Exception e3) {
								try {
									date = TIMESTAMP_PARSER_2.parse(strDate);
								} catch (Exception e4) {
									try {
										date = TIMESTAMP_PARSER_5.parse(strDate);
									} catch (Exception e5) {
										try {
											date = TIMESTAMP_GREGORIAN_PARSER.parse(strDate);
										} catch (Exception eg) {

										}
									}
								}
							}
						}
					}
				}
			}
		}
		Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
//		utc.setTime(date);
//		Date utcDate = utc.getTime();
		return date;
	}

	/**
	 * Common initialization of key parameters for which the cmd line entry overrides the config file provided defaults
	 * 
	 * @param arguments
	 */
	protected void initialize(Map<String,String> arguments) {
		provider	= NotNull(arguments.get("provider"), "all").toLowerCase();
		refreshToken = NotNull(arguments.get("refreshToken"), adsProperties.getProperty("api."+provider.toLowerCase()+".refreshToken"));
		dbName 		= NotNull(arguments.get("dbName"), adsProperties.getProperty("collection.dbName"));
//		domainName 	= NotNull(arguments.get("domainName"));
		try { 
			String lagStr = NotNull(arguments.getOrDefault("lag", adsProperties.getProperty("collection.lag")));
			lag = Integer.parseInt(lagStr); 
		} catch (Exception pe) {
			System.out.println("WARNING: Could not parse --lag parameter ("+pe.getMessage()+"), using default value: "+lag);
		}
		try { 
			String obsWindowStr = NotNull(arguments.getOrDefault("obsWindow", adsProperties.getProperty("collection.obsWindow")));
			obsWindow = Integer.parseInt(obsWindowStr); 
		} catch (Exception pe) {
			System.out.println("WARNING: Could not parse --obsWindow parameter ("+pe.getMessage()+"), using default value: "+obsWindow);
		}
		loadFromFile = arguments.get("loadFromFile");
	}

	protected String NotNull(String value) {
		if (value == null) {
			throw new InvalidParameterException("Required parameter is null, run program with --help to see mandatory arguments.");
		}
		return value;
	}

	protected String NotNull(String value, String altValue) {
		if (value == null) {
			if (altValue == null) {
				throw new InvalidParameterException("Required parameter is null, run program with --help to see mandatory arguments.");
			}
			return altValue;
		}
		return value;
	}
	
	/**
	 * Create a database & collection if it does not exist yet
	 */
	protected void initializeMongoDb() {
		mongoClient = new MongoClient( "localhost" , 27017 );
		mongoDb = mongoClient.getDatabase(dbName);
		String collectionName = "adwords".equalsIgnoreCase(provider) ? "costs_adwords" : "costs_bing";
		table = mongoDb.getCollection(collectionName);
		if (table == null) {
			mongoDb.createCollection(collectionName);
			table = mongoDb.getCollection(collectionName);
		}

	}

	/**
	 * test if the database / collection already contain the same document
	 * @param d
	 * @return
	 */
	public boolean checkDuplicate(Document d) {
		boolean duplicate = false;
		try {
			FindIterable<Document> documentList = 
					table.find(and(eq("campaign_id", d.getLong("campaign_id")), 
							eq("adgroup_id", d.getLong("adgroup_id")), 
							eq("ad_id",d.getLong("ad_id")),
							eq("timestamp",d.getDate("timestamp"))));
			duplicate = documentList != null && documentList.iterator().hasNext();
		} catch (Exception x) {

		}
		LOG("Is duplicate ("+d+") is : "+duplicate);
		return duplicate;
	}

	/**
	 * Build the adwords document from a report row
	 * 
	 * @param csvRow
	 * @param from
	 * @param to
	 * @return
	 */
	public Document _buildDocumentAdwordsReport(String[] csvRow, Calendar from, Calendar to) {
		Document document = null;
		document = csvRow != null ? 
				new Document("campaign_id", getField("Campaign id", csvRow, Long.class))
				.append("timestamp", parseDate((String)getField("Day", csvRow, String.class), (String)getField("Hour of day", csvRow, String.class), from, to))
				.append("campaign", getField("Campaign", csvRow, String.class))
				.append("adgroup", getField("Ad group", csvRow, String.class))
				.append("adgroup_id", getField("Ad group id", csvRow, Long.class))
				.append("ad", getFieldWithDefault("Headline", "Description", csvRow, String.class))
				.append("ad_id", getField("Ad id", csvRow, Long.class))
				.append("href", getFieldWithDefault("Final URL", "Display URL", csvRow, String.class))
				.append("impressions", getField("Impressions", csvRow, Long.class))
				.append("clicks", getField("Clicks", csvRow, Long.class))
				.append("cpc", getField("Avg. CPC", csvRow, Double.class))
				.append("cost", getField("Cost", csvRow, String.class))
				: null;
				if (document != null) {
					addNonCoreFields(document, ADWORDS_CORE_FIELDS, csvRow);
				}
				return document;
	}

	private static final Map<String, Integer> ADWORDS_CORE_FIELDS = new HashMap<>();
	/**
	 * Build adword document from report row using multiple column names that in different reports denote the same value.
	 * @param csvRow
	 * @param from
	 * @param to
	 * @return
	 */
	public Document buildDocumentAdwordsReport(String[] csvRow, Calendar from, Calendar to) {
		Document document = null;
		document = csvRow != null ? 
				new Document("campaign_id", getFieldWithDefault("CampaignId", "Campaign ID", csvRow, Long.class))
				.append("timestamp", parseDate((String)getFieldWithDefault("Date", "Day", csvRow, String.class), (String)getFieldWithDefault("HourOfDay", "Hour of day", csvRow, String.class), from, to))
				.append("campaign", getFieldWithDefault("CampaignName", "Campaign", csvRow, String.class))
				.append("adgroup", getFieldWithDefault("AdGroupName", "Ad group", csvRow, String.class))
				.append("adgroup_id", getFieldWithDefault("AdGroupId", "Ad group id", "Ad group ID", csvRow, Long.class))
				.append("ad", getField(new String[]{"Ad", "Headline", "Headline 1", "Description"}, csvRow, String.class))
				.append("ad_id", getFieldWithDefault("Id", "Ad id", "Ad ID",csvRow, Long.class))
				.append("href", getField(new String[]{"CreativeFinalUrls", "FinalUrl", "DisplayUrl","Final URL", "Display URL"}, csvRow, String.class))
				.append("impressions", getField("Impressions", csvRow, Long.class))
				.append("clicks", getField("Clicks", csvRow, Long.class))
				.append("cpc", getFieldWithDefault("CostPerConversion", "Avg. CPC", "Cost / conv.", csvRow, Double.class))
				.append("cost", getField("Cost", csvRow, Double.class))
				: null;
				if (document != null) {
					if (document.getString("href") == null || "--".equals(document.getString("href"))) {
						document.put("href", getFieldWithDefault("Final URL", "Display URL", csvRow, String.class));
					}
					addNonCoreFields(document, ADWORDS_CORE_FIELDS, csvRow);
				}
				return document;
	}

	private Map<String, Integer> headerIndexes = new HashMap<>();
	protected boolean debug = false;
	/**
	 * In order to reliably access the report values with different reports using different column order,
	 * build a map of header indexes so field can be accessed also by header name and not only by it's index.
	 * 
	 * @param headers
	 */
	protected void buildHeaderIndexes(String[] headers) {
		headerIndexes.clear();
		StringBuilder hdrs = new StringBuilder("Available report columns: ");
		for (int i = 0; i < headers.length; i++) {
			headerIndexes.put(headers[i], i);
			hdrs.append(headers[i]).append(", ");
		}
		LOG(hdrs.toString());
	}

	public Object getField(String fieldName, String[] row, Class<?> valueClass) {
		ADWORDS_CORE_FIELDS.put(fieldName, 1);
		BINGADS_CORE_FIELDS.put(fieldName, 1);
		Integer idx = headerIndexes.get(fieldName);
		String value = idx != null && row.length >= idx ? row[idx] : "--";
		return castValue(value, valueClass);
	}

	private Object castValue(String value, Class<?> valueClass) {
		if (value == null || "".equals(value) || "--".equals(value)) {
			return value;
		}
		Object castValue = value;
		switch (valueClass.getName()) {
		case "java.lang.Long" : {
			try {
				castValue = Long.parseLong(firstElement(value));
			} catch (Exception pe) { }
		}
		break;
		case "java.lang.Integer" :{
			try {
				castValue = Integer.parseInt(firstElement(value));
			} catch (Exception pe) { pe.printStackTrace(); }
		} 
		break;
		case "java.lang.Double" : {
			try {
				castValue = Double.parseDouble(firstElement(value).replaceAll("[^0-9\\.]+", ""));
			} catch (Exception pe) { pe.printStackTrace(); }
		} 
		break;
			default: 
				castValue = value;
		}
		return castValue;
	}

	private String firstElement(String value) {
		if (value.startsWith("[")) {
			return value.replaceAll("[\\[\\]]", "").split(",")[0];
		}
		return value;
	}
	
	protected boolean isEmpty(Object v) {
		boolean empty = v == null;
		if (!empty) {
			empty = ("--".equals(v) || " --".equals(v) || "".equals(v));
		}
		return empty;
	}
	
	public Object getFieldWithDefault(String fieldName, String defaultFieldName, String[] row, Class<?> valueClass) {
		ADWORDS_CORE_FIELDS.put(fieldName, 1);
		ADWORDS_CORE_FIELDS.put(defaultFieldName, 1);
		BINGADS_CORE_FIELDS.put(fieldName, 1);
		BINGADS_CORE_FIELDS.put(defaultFieldName, 1);
		Integer idx = headerIndexes.get(fieldName);
		String value = idx != null && row.length >= idx ? row[idx] : "--";
		if (isEmpty(value)) {
			idx = headerIndexes.get(defaultFieldName);
			value = idx != null && row.length >= idx ? row[idx] : "--";
		}
		return castValue(value, valueClass);
	}

	public Object getField(String[] fieldNames, String[] row, Class<?> valueClass) {
		String value = "--";
		for (String fieldName : fieldNames) {
			ADWORDS_CORE_FIELDS.put(fieldName, 1);
			BINGADS_CORE_FIELDS.put(fieldName, 1);
			Integer idx = headerIndexes.get(fieldName);
			value = idx != null && row.length >= idx ? row[idx] : "--";
			if (!isEmpty(value)) {
				break;
			}
		}
		return castValue(value, valueClass);
	}

	public Object getFieldWithDefault(String fieldName, String defaultFieldName, String defaultFieldName2, String[] row, Class<?> valueClass) {
		ADWORDS_CORE_FIELDS.put(fieldName, 1);
		ADWORDS_CORE_FIELDS.put(defaultFieldName, 1);
		ADWORDS_CORE_FIELDS.put(defaultFieldName2, 1);
		BINGADS_CORE_FIELDS.put(fieldName, 1);
		BINGADS_CORE_FIELDS.put(defaultFieldName, 1);
		BINGADS_CORE_FIELDS.put(defaultFieldName2, 1);
		Integer idx = headerIndexes.get(fieldName);
		String value = idx != null && row.length >= idx ? row[idx] : "--";
		if (isEmpty(value)) {
			idx = headerIndexes.get(defaultFieldName);
			value = idx != null && row.length >= idx ? row[idx] : "--";
		}
		if (isEmpty(value)) {
			idx = headerIndexes.get(defaultFieldName2);
			value = idx != null && row.length >= idx ? row[idx] : "--";
		}
		return castValue(value, valueClass);
	}

	/**
	 * Insert document to MongoDB
	 * @param d
	 */
	public void insertDocument(Document d) {
		if (table != null) {
			if (!checkDuplicate(d)) {
				table.insertOne(d);
			}
		} else {
			throw new RuntimeException("MongoDB collection not initialized or not created (variable table is null). provider = "+provider);
		}
	}

	/**
	 * Insert list of documents to mongo DB
	 * @param d
	 */
	public void insertDocuments(List<Document> d) {
		if (table != null) {
			for (Document x : d) {
				insertDocument(x);
			}
		} else {
			throw new RuntimeException("MongoDB collection not initialized or not created (variable table is null). provider = "+provider);
		}
	}

	/*
		// find all documents in collection
		MongoCursor<Document> cursor = table.find().iterator();
		try {
			while (cursor.hasNext()) {
				LOG(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}


		// queries
		Block<Document> printBlock = new Block<Document>() {
			@Override
			public void apply(final Document document) {
				LOG(document.toJson());
			}
		};

		FindIterable<Document> documentList = table.find(and(gt("i", 50), lte("i", 100)));
		documentList.forEach(printBlock);
		/ *
	 * The following example updates the first document that meets the filter i equals 10 and sets the value of i to 110:
	 * /
		table.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));
		//Update Multiple Documents
		//To update all documents matching the filter, use the updateMany method.

		//The following example increments the value of i by 100 for all documents where =i is less than 100:

		UpdateResult updateResult = table.updateMany(lt("i", 100), inc("i", 100));
		LOG(updateResult.getModifiedCount());

		Document document = new Document("name", "Café Con Leche")
				.append("contact", new Document("phone", "228-555-0149")
						.append("email", "cafeconleche@example.com")
						.append("location",Arrays.asList(-73.92502, 40.8279556)))
				.append("stars", 3)
				.append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));

		table.insertOne(document);
		table.replaceOne(
				eq("name", "Orange Patisserie and Gelateria"),
				new Document("stars", 5)
				.append("contact", "TBD")
				.append("categories", Arrays.asList("Cafe", "Pastries", "Ice Cream")),
				new UpdateOptions().upsert(true).bypassDocumentValidation(true));
	 */


	/*
	public Document buildDocument(Campaign googleCampaign, AdGroup googleGroup, Ad googleAd) {
		Document document = googleCampaign != null ? 
				new Document("campaign_id", googleCampaign.getId())
				.append("timestamp", googleCampaign.getStartDate())
				.append("campaign", googleCampaign.getName())
				.append("adgroup", googleGroup.getName())
				.append("adgroup_id", googleGroup.getId())
				.append("ad", googleAd.getAdType())
				.append("ad_id", googleAd.getId())
				.append("href", googleCampaign.getName())
				.append("impressions", googleGroup.getName())
				.append("clicks", googleCampaign.getName())
				.append("cpc", googleCampaign.getName())
				.append("cost", googleCampaign.getName())
				: null;
				return document;
	}
	 */


	protected boolean isDocumentInTimeRange(Document document, Calendar minRange, Calendar maxRange, boolean ignoreHour) {
		boolean isInRange = false;
		Date docDate = document.getDate("timestamp");
		Date minTime = minRange.getTime();
		Date maxTime = maxRange.getTime();
		if (docDate != null) {
			int a = docDate.compareTo(minTime);
			boolean b = docDate.after(minTime);
			int c = docDate.compareTo(maxTime);
			boolean d = docDate.before(maxTime);
			isInRange = ((a == 0 || b) && (c == 0 || d));
		}
		if (!isInRange && ignoreHour) {
			Calendar docCalendar = Calendar.getInstance();
			docCalendar.setTime(docDate);
			int docDay = docCalendar.get(Calendar.DAY_OF_MONTH);
			isInRange = docDay == minRange.get(Calendar.DAY_OF_MONTH);
		}
		LOG("inRange(minRange="+minTime+", maxRange="+maxTime+", ignoreHour="+ignoreHour+") of document timestamp="+docDate+" is: "+isInRange);
		return isInRange;
	}

	private static final Map<String,Integer> BINGADS_CORE_FIELDS = new HashMap<>();
	protected Document buildDocumentBingReport(String[] csvRow, Calendar from, Calendar to) {
		// AdId	AdTitle	Title part 1	Title part 2	AdDescription	AdGroupId	AdGroupName	AdGroupStatus	AdStatus	CampaignId	CampaignName	Clicks	CostPerConversion	ExtendedCost	ConversionRate	Impressions	DisplayUrl	DestinationUrl	FinalUrl	GregorianDate Hour
		Document document = null;
		Date date = parseDate((String)getField("GregorianDate", csvRow, String.class), (String)getField("Hour", csvRow, String.class), from, to);
		document = csvRow != null ? 
				new Document("campaign_id", getField("CampaignId", csvRow, Long.class))
				.append("timestamp", date)
				.append("campaign", getField("CampaignName", csvRow, String.class))
				.append("adgroup", getField("AdGroupName", csvRow, String.class))
				.append("adgroup_id", getField("AdGroupId", csvRow, Long.class))
				.append("ad", getFieldWithDefault("Title part 1", "AdDescription", csvRow, String.class))
				.append("ad_id", getField("AdId", csvRow, Long.class))
				.append("href", getFieldWithDefault("FinalUrl", "DisplayUrl", csvRow, String.class))
				.append("impressions", getField("Impressions", csvRow, Long.class))
				.append("clicks", getField("Clicks", csvRow, Long.class))
				.append("cpc", getField("CostPerConversion", csvRow, Double.class))
				.append("cost", getField("Spend", csvRow, Double.class))
				: null;
				if (document != null) {
					addNonCoreFields(document, BINGADS_CORE_FIELDS, csvRow);
				}
				return document;
	}

	protected Document _buildDocumentBingReport(String[] csvRow, Calendar from, Calendar to) {
		Document document = null;
		Date date = parseDate((String)getField("Gregorian date", csvRow, String.class), (String)getField("Hour", csvRow, String.class), from, to);
		document = csvRow != null ? 
				new Document("campaign_id", getField("Campaign ID", csvRow, Long.class))
				.append("timestamp", date)
				.append("campaign", getField("Campaign name", csvRow, String.class))
				.append("adgroup", getField("Ad group", csvRow, String.class))
				.append("adgroup_id", getField("Ad group ID", csvRow, Long.class))
				.append("ad", getFieldWithDefault("Ad title", "Title part 1", "Ad description", csvRow, String.class))
				.append("ad_id", getField("Ad ID", csvRow, Long.class))
				.append("href", getFieldWithDefault("Final URL", "Display URL", csvRow, String.class))
				.append("impressions", getField("Impressions", csvRow, Long.class))
				.append("clicks", getField("Clicks", csvRow, Long.class))
				.append("cpc", getField("Average CPC", csvRow, Double.class))
				.append("cost", getField("Spend", csvRow, Double.class))
				: null;
				if (document != null) {
					addNonCoreFields(document, BINGADS_CORE_FIELDS, csvRow);
				}
				return document;
	}

	private void addNonCoreFields(Document document, Map<String,Integer> coreFields, String[] csvRow) {
		for (String field : headerIndexes.keySet()) {
			if (coreFields.get(field) == null) {
				document.append(field.replaceAll("\\/", "x").replaceAll("\\.", ""), getField(field, csvRow, String.class));
			}
		}
	}

	protected void fillIdsIfRequired(Document document) {
		if (isEmpty(document.get("campaign_id"))) {
			Long c = allCampaignsMap.get(document.get("campaign"));
			document.append("campaign_id", c);
		}
		if (isEmpty(document.get("adgroup_id"))) {
			Long g = allAdGroupsMap.get(document.get("adgroup"));
			document.append("adgroup_id", g);
		}
		if (isEmpty(document.get("ad_id"))) {
			Long a = allAdsMap.get(document.get("ad"));
			document.append("ad_id", a);
		}
	}

	protected void setIdsIfRequired(Document document) {
		if (!isEmpty(document.get("campaign_id"))) {
			try {
				allCampaignsMap.put(document.getString("campaign"), document.getLong("campaign_id"));
			} catch (Exception hide) {}
		}
		if (!isEmpty(document.get("adgroup_id"))) {
			try {
				allAdGroupsMap.put(document.getString("adgroup"), document.getLong("adgroup_id"));
			} catch (Exception hide) {}
		}
		if (!isEmpty(document.get("ad_id"))) {
			try {
				allAdsMap.put(document.getString("ad"), document.getLong("ad_id"));
			} catch (Exception hide) {}
		}
	}
	
	/**
	 * Experimental code - as the Ads Performance report is only on daily bases, but AdGroups Performance can be specified at hourly bases,
	 * This function tries to extrapolate the clicks and traffic based on the hourly value for the entire ad group and allocate it proportionately to 
	 * the individual Ad clicks, impressions, cost ratios
	 * 
	 *   This code is not currently used because it produces numbers that are not in the online GUI and are confusing client 
	 */
	private static final SimpleDateFormat HOUR_FMT = new SimpleDateFormat("HH");
	protected List<Document> extrapolateHourlyPerformance(List<Document> ads, List<Document> adGroups) {
		List<Document> extrapolated = new ArrayList<Document>(32);
		Map<String, List<Document>> adGroupsByDay = new HashMap<>();
		for (Document ag : adGroups) {
			String d = TIMESTAMP_PARSER_1.format(ag.getDate("timestamp"));
			String agName = ag.getString("adgroup");
			List<Document> gbd = adGroupsByDay.get(d+" "+agName);
			if (gbd == null) {
				gbd = new ArrayList<Document>(12);
				adGroupsByDay.put(d+" "+agName, gbd);
			}
			gbd.add(ag);
		}
		for (Document a : ads) {
			String day = TIMESTAMP_PARSER_1.format(a.getDate("timestamp"));
			String agName = a.getString("adgroup");
			String adName = a.getLong("ad_id") != null ? ""+a.getLong("ad_id") : a.getString("ad");
			List<Document> gbd = adGroupsByDay.get(day+" "+agName);
			if (gbd != null) {
				long impressionTotal = 0;
				long clickTotal = 0;
				double costTotal = 0d;
				double cpcTotal = 0d;
				Map<String, Document> agByHour = new HashMap<>();
				for (Document ag : gbd) {
					String h = HOUR_FMT.format(ag.getDate("timestamp"));
					Long hourImpression = getLong(ag.get("impressions"));
					Long hourClick = getLong(ag.get("clicks"));
					Double hourCost = getDouble(ag.get("cost"));
					Double hourCpc = getDouble(ag.get("cpc"));
					impressionTotal += hourImpression != null ? hourImpression.longValue() : 0;
					clickTotal += hourClick != null ? hourClick.longValue() : 0;
					costTotal += hourCost != null ? hourCost.doubleValue() : 0d;
					cpcTotal += hourCpc != null ? hourCpc.doubleValue() : 0d;
					agByHour.put(h, ag);
				}
				List<Document> adByHour = new ArrayList<>(gbd.size());
				int impressionSubTotal = 0; 
				int clickSubTotal = 0; 
				int count = agByHour.size();
				for (String hour : agByHour.keySet()) {
					Document ag = agByHour.get(hour);
					long agImpression = getLong(ag.get("impressions"));
					long agClicks = getLong(ag.get("clicks"));
					double agCost = getDouble(ag.get("cost"));
					double agCpc = getDouble(ag.get("cpc"));
					double ratioImpression = impressionTotal > 0 ? (double)agImpression / (double)impressionTotal : 1.0d;
					double ratioClicks = clickTotal > 0 ? (double)agClicks / (double)clickTotal : 1.0d;
					double ratioCost   = costTotal > 0 ? (double)agCost / (double)costTotal : 1.0d;
					double ratioCpc = cpcTotal > 0 ? (double)agCpc / (double)cpcTotal : 1.0d;
					long impressions = count > 1 ? Math.round(ratioImpression * getLong(a.get("impressions"))) : Math.max(impressionTotal - impressionSubTotal, 0);
					long clicks		 = count > 1 ? Math.round(ratioClicks * getLong(a.get("clicks"))) : Math.max(clickTotal - clickSubTotal, 0);
					double cpc		 = Math.round(100d*ratioCpc*getDouble(a.get("cpc")))/100d;
					double cost		 = Math.round(100d*ratioCost*getDouble(a.get("cost")))/100d;
					impressionSubTotal += impressions;
					clickSubTotal += clicks;
					Document ad = new Document("campaign_id", a.getLong("campaign_id"))
							.append("timestamp", parseDate(day, hour, null, null))
							.append("campaign", a.getString("campaign"))
							.append("adgroup", a.getString("adgroup"))
							.append("adgroup_id", a.getLong("adgroup_id"))
							.append("ad", a.getString("ad"))
							.append("ad_id", a.getLong("ad_id"))
							.append("href", a.getString("href"))
							.append("impressions", impressions)
							.append("clicks", clicks)
							.append("cpc", cpc)
							.append("cost", cost);
					adByHour.add(ad);
					count--;
				}
				extrapolated.addAll(adByHour);
			} else {
				extrapolated.add(a);
			}
		}
		return extrapolated;
	}

	public long getLong(Object value) {
		return (Long.class.isInstance(value)) ? ((Long)value).longValue() : 0L;
	}

	public double getDouble(Object value) {
		return (Double.class.isInstance(value)) ? ((Double)value).doubleValue() : 0.0d;
	}
	
	protected void LOG(String msg) {
		if (debug ) {
			System.out.println(msg);
		}
	}
	
	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}