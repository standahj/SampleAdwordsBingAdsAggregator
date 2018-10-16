package tools;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bson.Document;

import com.microsoft.bingads.AuthorizationData;
import com.microsoft.bingads.OAuthDesktopMobileAuthCodeGrant;
import com.microsoft.bingads.reporting.AccountThroughAdGroupReportScope;
import com.microsoft.bingads.reporting.AdGroupPerformanceReportColumn;
import com.microsoft.bingads.reporting.AdGroupPerformanceReportRequest;
import com.microsoft.bingads.reporting.AdPerformanceReportColumn;
import com.microsoft.bingads.reporting.AdPerformanceReportRequest;
import com.microsoft.bingads.reporting.ArrayOfAdGroupPerformanceReportColumn;
import com.microsoft.bingads.reporting.ArrayOfAdPerformanceReportColumn;
import com.microsoft.bingads.reporting.ArrayOflong;
import com.microsoft.bingads.reporting.Date;
import com.microsoft.bingads.reporting.NonHourlyReportAggregation;
import com.microsoft.bingads.reporting.ReportAggregation;
import com.microsoft.bingads.reporting.ReportFormat;
import com.microsoft.bingads.reporting.ReportRequest;
import com.microsoft.bingads.reporting.ReportTime;
import com.microsoft.bingads.reporting.ReportingDownloadParameters;
import com.microsoft.bingads.reporting.ReportingServiceManager;
import com.opencsv.CSVReader;

public class Bing extends BaseCollectable implements Collectable {

	private AuthorizationData authorizationData;

	// These credentials may be invalid now
	private static String DeveloperToken 		= "1083W3328Y824757";
	// ClientId is the registered application ID
	private static String ClientId 		 		= "5909254d-7ad8-460b-a594-8b36ac7b072a";
	private static String CustomerId 			= "159420464";
	private static String AccountId 			= "139127317";

	private String  reportDirectory 			= ".";
	private boolean removeDownloadedReportFile 	= true;

	public Bing() {
		adsProperties = new Properties();
		try {
			adsProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("/ads.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.provider = "bing";
	}

	public Bing(Properties adsProperties) {
		this.adsProperties = adsProperties;
		this.provider = "bing";
	}

	@Override
	protected void initialize(Map<String,String> arguments) {
		super.initialize(arguments);
		reportDirectory	= NotNull(arguments.get("reportDirectory"), adsProperties.getProperty("collection.bing.reportDirectory","."));
		removeDownloadedReportFile = Boolean.parseBoolean(NotNull(arguments.get("removeDownloadedReportFile"), adsProperties.getProperty("collection.bing.removeDownloadedReportFile", "true")));
	}

	/**
	 * collect the Ads statistics
	 */
	@Override
	public int collect(Map<String, String> arguments) {
		initialize(arguments);
		int count = 0;

		// Setup the authorization
		initializeAuthorizationData();

		ReportingServiceManager reportingManager = new ReportingServiceManager(authorizationData);
		reportingManager.setStatusPollIntervalInMilliseconds(5000);

		// Setup the observable window range
		Calendar minRange = Calendar.getInstance();
		minRange.add(Calendar.HOUR, 0-lag-obsWindow);
		Calendar maxRange = Calendar.getInstance();
		maxRange.add(Calendar.HOUR, 0-lag);
		LOG("Query range from: ("+minRange.getTime()+" - "+maxRange.getTime()+")");

		// collect the data
		// do we load from offline or online source ?
		boolean bLoadFromFile = false;
		File fromFile = null;
		if (loadFromFile != null) {
			try {
				String[] files = loadFromFile.split(";");
				fromFile = new File(files[files.length-1]);
				bLoadFromFile = fromFile.exists() && fromFile.isFile();
				if (files.length > 1) {
					for (int f = 0; f < files.length - 1; f++) {
						processFile(new File(files[f]), minRange, maxRange, false, false, null);
					}
				}

			} catch (Exception ea) {
				bLoadFromFile = false;
			}
		}
		
		if (!bLoadFromFile) {
			// Load From API (online)
			
			// 1. Prepare report request for AdPerformance
			ReportRequest reportRequest = getAdReport(minRange, maxRange);
			// 2. Set Download options
			ReportingDownloadParameters reportingDownloadParameters = new ReportingDownloadParameters();
			reportingDownloadParameters.setReportRequest(reportRequest);
			reportingDownloadParameters.setResultFileDirectory(new File(reportDirectory));
			reportingDownloadParameters.setResultFileName(System.currentTimeMillis()+"_AdReport.csv");
			reportingDownloadParameters.setOverwriteResultFile(true);
			// 3. Download report file from BingAds API (unfortunately Microsoft insists on using the file, there is no stream option)
			// We may optionally cancel the downloadFileAsync operation after a specified time interval.
			File resultAdReportFile = null;;
			try {
				resultAdReportFile = reportingManager.downloadFileAsync(
						reportingDownloadParameters, 
						null).get(3600000, TimeUnit.MILLISECONDS);

			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
			}
			
			// 4. Prepare the AdGroupPerformance request
			reportRequest = getAdGroupReport(minRange, maxRange);
			File resultAdGroupReportFile = null;

			// 5. Set Download options
			reportingDownloadParameters.setReportRequest(reportRequest);
			reportingDownloadParameters.setResultFileName(System.currentTimeMillis()+"_AdGroupReport.csv");
			
			// 6. Download the report file
			try {
				resultAdGroupReportFile = reportingManager.downloadFileAsync(
						reportingDownloadParameters, 
						null).get(3600000, TimeUnit.MILLISECONDS);

			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
			}

			// 7. prepare the DB
			initializeMongoDb();

			// 8. process downloaded files
			List<Document> adList = new ArrayList<>(32);
			count += processFile(resultAdReportFile, minRange, maxRange, true, true, adList);
			List<Document> adGroupList = new ArrayList<>(32);
			count += processFile(resultAdGroupReportFile, minRange, maxRange, false, true, adGroupList);
			
			List<Document> allDocs = extrapolateHourlyPerformance(adList, adGroupList);
			insertDocuments(allDocs);
			// 9. cleanup
			mongoClient.close();
			if (removeDownloadedReportFile) {
				if (resultAdReportFile != null) {
					resultAdReportFile.deleteOnExit();
				}
				if (resultAdGroupReportFile != null) {
					resultAdGroupReportFile.deleteOnExit();
				}
			}
			
		} else {
			// 1. Prepare DB
			initializeMongoDb();
			
			// 2. load from provided offline file
			List<Document> adGroupList = new ArrayList<>(32);
			count += processFile(fromFile, minRange, maxRange, false, true, null);
			
			// 3. cleanup
			mongoClient.close();
		}
		return count;
	}

	@Override
	public String getDescription() {
		return "Bing";
	}

	private void initializeAuthorizationData() {
		// Client ID is this tool's registered ID
		OAuthDesktopMobileAuthCodeGrant oAuthDesktopMobileAuthCodeGrant = new OAuthDesktopMobileAuthCodeGrant(adsProperties.getProperty("api.bing.clientId", ClientId));
		oAuthDesktopMobileAuthCodeGrant.requestAccessAndRefreshTokens(adsProperties.getProperty("api.bing.refreshToken"));
		authorizationData = new AuthorizationData();
		authorizationData.setDeveloperToken(adsProperties.getProperty("api.bing.developerToken", DeveloperToken));
		authorizationData.setAuthentication(oAuthDesktopMobileAuthCodeGrant);
		String customerId = adsProperties.getProperty("api.bing.customerId", CustomerId);
		authorizationData.setCustomerId(Long.parseLong(customerId));
		String accountId  = adsProperties.getProperty("api.bing.accountId", AccountId);
		authorizationData.setAccountId(Long.parseLong(accountId));
	}
	
	/**
	 * Process the downloaded report file - parse CSV and load it to MongoDB.
	 * Check for duplicates and ignore records that fall outside of the observable window
	 * @param reportFile
	 * @param minRange .. start of the observable window
	 * @param maxRange .. end of the observable window
	 * @return
	 */
	private int processFile(File reportFile, Calendar minRange, Calendar maxRange, boolean ignoreHour, boolean doInsert, List<Document> returnList) {
		int count = 0;
		boolean isV9 = false; // ReportingService uses v9, report loaded fom UI has different columns, this helps distinguish the origin of file 
		try {
			csvReader = new CSVReader(new FileReader(reportFile));
			boolean isHeaderFooter = true;
			String[] row = null;
			int rows = 0;
			String[] headers = null;
			while ((row = csvReader.readNext()) != null) {
				// skip prologue / footer (both separated from data by empty row)
				if (row.length == 0 || (row.length == 1 && "".equals(row[0]))) {
					isHeaderFooter = !isHeaderFooter;
					continue;
				}
				if (!isHeaderFooter) {
					if (headers == null) {
						headers = row;
						buildHeaderIndexes(headers);
						StringBuilder dbgPrint = new StringBuilder(reportFile.getName()+": Available report columns: ");
						for (String h : headers) {
							if ("AdGroupName".equals(h)) { // UI downloaded file will contain "Ad Group name" instead
								isV9 = true;
							}
							dbgPrint.append(h).append(", ");
						}
						LOG(dbgPrint.toString());
						continue;
					}
					rows++;
					Document document = isV9 ? buildDocumentBingReport(row, minRange, maxRange) : _buildDocumentBingReport(row, minRange, maxRange);
					if (!doInsert) {
						setIdsIfRequired(document);
					} else {
						fillIdsIfRequired(document);
					}
					if (doInsert && isDocumentInTimeRange(document, minRange, maxRange, ignoreHour)) {
						if (returnList != null) {
							returnList.add(document);
						} else {
							if (!checkDuplicate(document)) {
								insertDocument(document);
							}
						}
						count++;
					}
				}
			}
			LOG(reportFile.getName()+" downloaded rows: "+rows+", unique, in obsWindow rows: "+count);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return count;
	}

	/**
	 * Prepare the request for AdPerformanceReport using custom time range  from/to
	 * @param from
	 * @param to
	 * @return
	 */
	private ReportRequest getAdReport(Calendar from, Calendar to) {
		AdPerformanceReportRequest request = new AdPerformanceReportRequest();
		request.setFormat(ReportFormat.CSV);
		request.setReportName("Ad Report");
		request.setAggregation(NonHourlyReportAggregation.DAILY);
		request.setReturnOnlyCompleteData(false);

		AccountThroughAdGroupReportScope scope = new AccountThroughAdGroupReportScope();
		ArrayOflong accountIds = new ArrayOflong();
		accountIds.getLongs().add(authorizationData.getAccountId());
		scope.setAccountIds(accountIds);
		scope.setCampaigns(null);
		scope.setAdGroups(null);         
		request.setScope(scope);

		ArrayOfAdPerformanceReportColumn value = new ArrayOfAdPerformanceReportColumn();
		List<AdPerformanceReportColumn> columns = value.getAdPerformanceReportColumns();
		String adFields = adsProperties.getProperty("api.bing.adPerformanceReport.fields");
		if (adFields != null && adFields.length() > 2) {
			for (String fieldId : adFields.split(",")) {
				columns.add(AdPerformanceReportColumn.fromValue(fieldId));
			}
		} else {
			columns.add(AdPerformanceReportColumn.AD_ID);
			columns.add(AdPerformanceReportColumn.AD_TITLE);
			columns.add(AdPerformanceReportColumn.TITLE_PART_1);
			columns.add(AdPerformanceReportColumn.TITLE_PART_2);
			columns.add(AdPerformanceReportColumn.AD_DESCRIPTION);
			columns.add(AdPerformanceReportColumn.AD_GROUP_ID);
			columns.add(AdPerformanceReportColumn.AD_GROUP_NAME);
			columns.add(AdPerformanceReportColumn.AD_GROUP_STATUS);
			columns.add(AdPerformanceReportColumn.AD_STATUS);
			columns.add(AdPerformanceReportColumn.CAMPAIGN_ID);
			columns.add(AdPerformanceReportColumn.CAMPAIGN_NAME);
			columns.add(AdPerformanceReportColumn.CLICKS);
			columns.add(AdPerformanceReportColumn.COST_PER_CONVERSION);
			columns.add(AdPerformanceReportColumn.SPEND);
			columns.add(AdPerformanceReportColumn.CONVERSION_RATE);
			columns.add(AdPerformanceReportColumn.IMPRESSIONS);
			columns.add(AdPerformanceReportColumn.DESTINATION_URL);
			columns.add(AdPerformanceReportColumn.FINAL_URL);
			columns.add(AdPerformanceReportColumn.DISPLAY_URL);
			columns.add(AdPerformanceReportColumn.TIME_PERIOD);
		}
		request.setColumns(value);

		ReportTime reportTime = new ReportTime();
		Date start = new Date();
		Date end = new Date();
		start.setDay(from.get(Calendar.DAY_OF_MONTH));
		start.setMonth(from.get(Calendar.MONTH)+1);
		start.setYear(from.get(Calendar.YEAR));
		end.setDay(to.get(Calendar.DAY_OF_MONTH));
		end.setMonth(to.get(Calendar.MONTH)+1);
		end.setYear(to.get(Calendar.YEAR));
		reportTime.setCustomDateRangeStart(start);
		reportTime.setCustomDateRangeEnd(end);
		request.setTime(reportTime);

		return request;
	}

	/**
	 * Prepare the request for AdPerformanceReport using custom time range from/to
	 * @param from
	 * @param to
	 * @return
	 */
	private ReportRequest getAdGroupReport(Calendar from, Calendar to) {
		AdGroupPerformanceReportRequest request = new AdGroupPerformanceReportRequest();
		request.setFormat(ReportFormat.CSV);
		request.setReportName("Ad Group Report");
		request.setAggregation(ReportAggregation.HOURLY);
		request.setReturnOnlyCompleteData(false);

		AccountThroughAdGroupReportScope scope = new AccountThroughAdGroupReportScope();
		ArrayOflong accountIds = new ArrayOflong();
		accountIds.getLongs().add(authorizationData.getAccountId());
		scope.setAccountIds(accountIds);
		scope.setCampaigns(null);
		scope.setAdGroups(null);         
		request.setScope(scope);

		ArrayOfAdGroupPerformanceReportColumn value = new ArrayOfAdGroupPerformanceReportColumn();
		List<AdGroupPerformanceReportColumn> columns = value.getAdGroupPerformanceReportColumns();
		String adFields = adsProperties.getProperty("api.bing.adGroupPerformanceReport.fields");
		if (adFields != null && adFields.length() > 2) {
			for (String fieldId : adFields.split(",")) {
				columns.add(AdGroupPerformanceReportColumn.fromValue(fieldId));
			}
		} else {
			columns.add(AdGroupPerformanceReportColumn.AD_GROUP_ID);
			columns.add(AdGroupPerformanceReportColumn.AD_GROUP_NAME);
			columns.add(AdGroupPerformanceReportColumn.CAMPAIGN_ID);
			columns.add(AdGroupPerformanceReportColumn.CAMPAIGN_NAME);
			columns.add(AdGroupPerformanceReportColumn.TIME_PERIOD);
			columns.add(AdGroupPerformanceReportColumn.CLICKS);
			columns.add(AdGroupPerformanceReportColumn.COST_PER_CONVERSION);
			columns.add(AdGroupPerformanceReportColumn.SPEND);
			columns.add(AdGroupPerformanceReportColumn.CONVERSION_RATE);
			columns.add(AdGroupPerformanceReportColumn.IMPRESSIONS);
		}
		request.setColumns(value);

		ReportTime reportTime = new ReportTime();
		Date start = new Date();
		Date end = new Date();
		start.setDay(from.get(Calendar.DAY_OF_MONTH));
		start.setMonth(from.get(Calendar.MONTH)+1);
		start.setYear(from.get(Calendar.YEAR));
		end.setDay(to.get(Calendar.DAY_OF_MONTH));
		end.setMonth(to.get(Calendar.MONTH)+1);
		end.setYear(to.get(Calendar.YEAR));
		reportTime.setCustomDateRangeStart(start);
		reportTime.setCustomDateRangeEnd(end);
		request.setTime(reportTime);

		return request;
	}

	/*
    private ReportRequest getKeywordPerformanceReportRequest(){
        KeywordPerformanceReportRequest report = new KeywordPerformanceReportRequest();

        report.setFormat(ReportFormat.TSV);
        report.setReportName("My Keyword Performance Report");
        report.setReturnOnlyCompleteData(false);
        report.setAggregation(ReportAggregation.DAILY);

        ArrayOflong accountIds = new ArrayOflong();
        accountIds.getLongs().add(authorizationData.getAccountId());

        report.setScope(new AccountThroughAdGroupReportScope());
        report.getScope().setAccountIds(accountIds);
        report.getScope().setCampaigns(null);
        report.getScope().setAdGroups(null);         

        report.setTime(new ReportTime());
        report.getTime().setPredefinedTime(ReportTimePeriod.YESTERDAY);

        // You may either use a custom date range or predefined time.
        //report.getTime().setCustomDateRangeStart(new Date());
        //report.getTime().getCustomDateRangeStart().setMonth(9);
        //report.getTime().getCustomDateRangeStart().setDay(1);
        //report.getTime().getCustomDateRangeStart().setYear(2017);
        //report.getTime().setCustomDateRangeEnd(new Date());
        //report.getTime().getCustomDateRangeEnd().setMonth(9);
        //report.getTime().getCustomDateRangeEnd().setDay(30);
        //report.getTime().getCustomDateRangeEnd().setYear(2017);

        // If you specify a filter, results may differ from data you see in the Bing Ads web application
        //report.setFilter(new KeywordPerformanceReportFilter());
        //ArrayList<DeviceTypeReportFilter> deviceTypes = new ArrayList<DeviceTypeReportFilter>();
        //deviceTypes.add(DeviceTypeReportFilter.COMPUTER);
        //deviceTypes.add(DeviceTypeReportFilter.SMART_PHONE);
        //report.getFilter().setDeviceType(deviceTypes);

        // Specify the attribute and data report columns.

        ArrayOfKeywordPerformanceReportColumn keywordPerformanceReportColumns = new ArrayOfKeywordPerformanceReportColumn();
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.TIME_PERIOD);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.ACCOUNT_ID);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.CAMPAIGN_ID);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.KEYWORD);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.KEYWORD_ID);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.DEVICE_TYPE);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.BID_MATCH_TYPE);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.CLICKS);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.IMPRESSIONS);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.CTR);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.AVERAGE_CPC);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.SPEND);
        keywordPerformanceReportColumns.getKeywordPerformanceReportColumns().add(KeywordPerformanceReportColumn.QUALITY_SCORE);
        report.setColumns(keywordPerformanceReportColumns);

        // You may optionally sort by any KeywordPerformanceReportColumn, and optionally
        // specify the maximum number of rows to return in the sorted report. 

        KeywordPerformanceReportSort keywordPerformanceReportSort = new KeywordPerformanceReportSort();
        keywordPerformanceReportSort.setSortColumn(KeywordPerformanceReportColumn.CLICKS);
        keywordPerformanceReportSort.setSortOrder(SortOrder.ASCENDING);
        ArrayOfKeywordPerformanceReportSort keywordPerformanceReportSorts = new ArrayOfKeywordPerformanceReportSort();
        keywordPerformanceReportSorts.getKeywordPerformanceReportSorts().add(keywordPerformanceReportSort);
        report.setSort(keywordPerformanceReportSorts);

        report.setMaxRows(10);

        return report;
    }
	 */
}
