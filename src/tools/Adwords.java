package tools;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bson.Document;

import com.google.api.ads.adwords.axis.factory.AdWordsServices;
import com.google.api.ads.adwords.axis.v201702.cm.*;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration;
import com.google.api.ads.adwords.lib.factory.AdWordsServicesInterface;
import com.google.api.ads.adwords.lib.jaxb.v201702.DateRange;
import com.google.api.ads.adwords.lib.jaxb.v201702.DownloadFormat;
import com.google.api.ads.adwords.lib.jaxb.v201702.ReportDefinition;
import com.google.api.ads.adwords.lib.jaxb.v201702.ReportDefinitionDateRangeType;
import com.google.api.ads.adwords.lib.jaxb.v201702.ReportDefinitionReportType;
import com.google.api.ads.adwords.axis.utils.v201702.SelectorBuilder;
import com.google.api.ads.adwords.lib.selectorfields.v201702.cm.AdGroupAdField;
import com.google.api.ads.adwords.lib.selectorfields.v201702.cm.AdGroupField;
import com.google.api.ads.adwords.lib.selectorfields.v201702.cm.CampaignField;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponseException;
import com.google.api.ads.adwords.lib.utils.ReportException;
import com.google.api.ads.adwords.lib.utils.v201702.ReportDownloaderInterface;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.auth.OfflineCredentials.ForApiBuilder;
import com.google.api.ads.common.lib.conf.ConfigurationLoadException;
import com.google.api.ads.common.lib.exception.OAuthException;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;

public class Adwords extends BaseCollectable implements Collectable {

	private Credential 				 oAuth2Credential;
	private AdWordsSession 			 session;
	private AdWordsServicesInterface adWordsServices;
	private File 					 cfgFile;

	private List<AdGroup>			 allAdGroups;
	private List<Ad>	 			 allAds;
	private List<Campaign> 			 allCampaigns;
	private static final int 		 PAGE_SIZE = 100;

	public Adwords(File cfgFile, Properties adsProperties) {
		this.cfgFile	   = cfgFile;
		this.adsProperties = adsProperties;
		this.provider 	   = "adwords";  
	}

	public Adwords(Properties adsProperties) {
		this(null, adsProperties);
	}

	@Override
	public String getDescription() {
		return "Adwords";
	}

	@Override
	public int collect(Map<String, String> arguments) {
		initialize(arguments);
		int count = 0;
		try {
			// Setup the authorization
			authenticationBuilder();

			// Setup the observable window range
			Calendar minRange = Calendar.getInstance();
			minRange.add(Calendar.HOUR, 0-lag-obsWindow);
			Calendar maxRange = Calendar.getInstance();
			maxRange.add(Calendar.HOUR, 0-lag);
			DateRange dateRange = new DateRange();
			dateRange.setMin(RANGE_FORMATTER.format(minRange.getTime()));
			dateRange.setMax(RANGE_FORMATTER.format(maxRange.getTime()));
			System.out.println("Query range from: "+dateRange.getMin()+" to: "+dateRange.getMax()+" ("+minRange.getTime()+" - "+maxRange.getTime()+")");

			// collect the data
			// do we load from offline or online source ?
			boolean bLoadFromFile = false;
			File fromFileAdGroup = null;
			File fromFileAd		 = null;
			if (loadFromFile != null) {
				try {
					String[] files = loadFromFile.split(";");
					fromFileAdGroup = new File(files[files.length-1]);
					bLoadFromFile = fromFileAdGroup.exists() && fromFileAdGroup.isFile();
					if (files.length > 2) {
						fromFileAd = new File(files[files.length-2]);
						for (int f = 0; f < files.length - 2; f++) {
							CSVReader cr = new CSVReader(new FileReader(new File(files[f])));
							String[] row = null;
							int rows = 0;
							String[] headers = cr.readNext();
							if (headers.length == 1) {
								headers = cr.readNext();
							}
							buildHeaderIndexes(headers);
							while ((row = cr.readNext()) != null) {
								Document document = buildDocumentAdwordsReport(row, minRange, maxRange);
								setIdsIfRequired(document);
							}
						}
					}
				} catch (Exception ea) {
					bLoadFromFile = false;
					ea.printStackTrace();
				}
			}

			csvReader = bLoadFromFile && fromFileAd != null ? new CSVReader(new FileReader(fromFileAd)) : // offline 
										getAdCriteria(dateRange);				  // online

			// initialize database
			initializeMongoDb();
			List<Document> adList = new ArrayList<>(32);
			// process the data
			if (csvReader != null) {
				String[] row = null;
				int rows = 0;
				String[] headers = csvReader.readNext();
				buildHeaderIndexes(headers);
				while ((row = csvReader.readNext()) != null) {
					rows++;
					Document document = buildDocumentAdwordsReport(row, minRange, maxRange);
					fillIdsIfRequired(document);
					if (isDocumentInTimeRange(document, minRange, maxRange, true)) {
							//insertDocument(document);
							adList.add(document);
							count++;
					}
				}
				System.out.println("AdGroupReport downloaded rows: "+rows+", unique, in obsWindow rows: "+count);
			}
			// AdGroupReport is applicable only for online service
			List<Document> adGroupList = new ArrayList<>(32);
			
			csvReader = bLoadFromFile && fromFileAdGroup != null ? new CSVReader(new FileReader(fromFileAdGroup)) : // offline 
						getAdGroupCriteria(dateRange);
			if (csvReader != null) {
				String[] row = null;
				int rows = 0;
				String[] headers = csvReader.readNext();
				buildHeaderIndexes(headers);
				while ((row = csvReader.readNext()) != null) {
					rows++;
					Document document = buildDocumentAdwordsReport(row, minRange, maxRange);
					fillIdsIfRequired(document);
					if (isDocumentInTimeRange(document, minRange, maxRange, false)) {
							//insertDocument(document);
							adGroupList.add(document);
							count++;
					}
				}
				System.out.println("AdGroupReport downloaded rows: "+rows+", unique, in obsWindow rows: "+count);
			}
			//List<Document> allDocs = extrapolateHourlyPerformance(adList, adGroupList);
			insertDocuments(adList);
			insertDocuments(adGroupList);
			// cleanup
			mongoClient.close();

		} catch (OAuthException | ValidationException | ConfigurationLoadException | ReportException | IOException e) {
			e.printStackTrace();
		}

		return count;
	}

	/*
	 * ************************************************************************************************************
	 */
	public void authenticationBuilder() throws OAuthException, ValidationException, ConfigurationLoadException {
		// Generate a refreshable OAuth2 credential.
		ForApiBuilder credentialBuilder = new OfflineCredentials.Builder().forApi(Api.ADWORDS);
		credentialBuilder = cfgFile != null && cfgFile.exists() ? credentialBuilder.fromFile(cfgFile) : credentialBuilder.fromFile();
		credentialBuilder = refreshToken != null && refreshToken.length() > 2 ? credentialBuilder.withRefreshToken(refreshToken) : credentialBuilder;
		oAuth2Credential = credentialBuilder.build().generateCredential();

		// Construct an AdWordsSession.
		AdWordsSession.Builder adwordsSessionBuilder = new AdWordsSession.Builder();
		adwordsSessionBuilder = cfgFile != null && cfgFile.exists() ? adwordsSessionBuilder.fromFile(cfgFile) : adwordsSessionBuilder.fromFile();
		session = adwordsSessionBuilder.withOAuth2Credential(oAuth2Credential).build();

		adWordsServices = AdWordsServices.getInstance();

	}

	private static final SimpleDateFormat RANGE_FORMATTER = new SimpleDateFormat("yyyyMMdd");
	public CSVReader getAdCriteria(DateRange dateRange) throws ReportException {
		CSVReader csvReader = null;
		// Create selector.
		List<String> fields = Lists.newArrayList("CampaignId","CampaignName",
				"AdGroupId","AdGroupName",
				"Date", 
				"Id",
				"Headline",
				"DisplayUrl","CreativeFinalUrls",
				"Impressions",
				"Clicks",
				"CostPerConversion",
				"Cost"
				);
		com.google.api.ads.adwords.lib.jaxb.v201702.Selector selector = new com.google.api.ads.adwords.lib.jaxb.v201702.Selector();
		selector.getFields().addAll(fields);
		selector.setDateRange(dateRange);
		// Create report definition.
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setReportName("Campaign performance report #" + System.currentTimeMillis());
		reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.CUSTOM_DATE);
		reportDefinition.setReportType(ReportDefinitionReportType.AD_PERFORMANCE_REPORT);
		reportDefinition.setDownloadFormat(DownloadFormat.CSV);
		reportDefinition.setSelector(selector);

		// Optional: Set the reporting configuration of the session to suppress header, column name, or
		// summary rows in the report output. You can also configure this via your ads.properties
		// configuration file. See AdWordsSession.Builder.from(Configuration) for details.
		// In addition, you can set whether you want to explicitly include or exclude zero impression
		// rows.
		ReportingConfiguration reportingConfiguration =
				new ReportingConfiguration.Builder()
				.skipReportHeader(true)
				.skipColumnHeader(false)
				.skipReportSummary(true)
				// Enable to allow rows with zero impressions to show.
				.includeZeroImpressions(true)
				.build();
		session.setReportingConfiguration(reportingConfiguration);

		reportDefinition.setSelector(selector);

		ReportDownloaderInterface reportDownloader = adWordsServices.getUtility(session, ReportDownloaderInterface.class);

		try {
			// Set the property api.adwords.reportDownloadTimeout or call
			// ReportDownloader.setReportDownloadTimeout to set a timeout (in milliseconds)
			// for CONNECT and READ in report downloads.
			ReportDownloadResponse response = reportDownloader.downloadReport(reportDefinition);
			csvReader = new CSVReader(new StringReader(response.getAsString()));
		} catch (ReportDownloadResponseException | IOException e) {
			System.out.printf("Report was not downloaded due to: %s%n", e);
		}
		return csvReader;
	}

	public CSVReader getAdGroupCriteria(DateRange dateRange) throws ReportException {
		CSVReader csvReader = null;
		// Create selector.
		List<String> fields = Lists.newArrayList("CampaignId","CampaignName",
				"AdGroupId","AdGroupName",
				"Date", "HourOfDay", 
				"Impressions",
				"Clicks",
				"CostPerConversion",
				"Cost"
				);
		com.google.api.ads.adwords.lib.jaxb.v201702.Selector selector = new com.google.api.ads.adwords.lib.jaxb.v201702.Selector();
		selector.getFields().addAll(fields);
		selector.setDateRange(dateRange);
		// Create report definition.
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setReportName("Ad Group performance report #" + System.currentTimeMillis());
		reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.CUSTOM_DATE);
		reportDefinition.setReportType(ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT);
		reportDefinition.setDownloadFormat(DownloadFormat.CSV);
		reportDefinition.setSelector(selector);


		// Optional: Set the reporting configuration of the session to suppress header, column name, or
		// summary rows in the report output. You can also configure this via your ads.properties
		// configuration file. See AdWordsSession.Builder.from(Configuration) for details.
		// In addition, you can set whether you want to explicitly include or exclude zero impression
		// rows.
		ReportingConfiguration reportingConfiguration =
				new ReportingConfiguration.Builder()
				.skipReportHeader(true)
				.skipColumnHeader(false)
				.skipReportSummary(true)
				// Enable to allow rows with zero impressions to show.
				.includeZeroImpressions(true)
				.build();
		session.setReportingConfiguration(reportingConfiguration);

		reportDefinition.setSelector(selector);

		ReportDownloaderInterface reportDownloader = adWordsServices.getUtility(session, ReportDownloaderInterface.class);

		try {
			// Set the property api.adwords.reportDownloadTimeout or call
			// ReportDownloader.setReportDownloadTimeout to set a timeout (in milliseconds)
			// for CONNECT and READ in report downloads.
			ReportDownloadResponse response = reportDownloader.downloadReport(reportDefinition);
			csvReader = new CSVReader(new StringReader(response.getAsString()));
		} catch (ReportDownloadResponseException | IOException e) {
			System.out.printf("Report was not downloaded due to: %s%n", e);
		}
		return csvReader;
	}

	/*
	private void loadContext() {
		getCampaigns();
		allCampaignsMap = new HashMap<>();
		allAdGroupsMap = new HashMap<>();
		allAdsMap = new HashMap<>();
		for (Campaign c : allCampaigns) {
			allCampaignsMap.put(c.getName(), c);
			try {
				getAdGroups(c);
				for (AdGroup ag : allAdGroups) {
					allAdGroupsMap.put(ag.getName(), ag);
					getAds(ag.getId());
					for (Ad a : allAds) {
						allAdsMap.put(((TextAd)a).getHeadline() != null && ((TextAd)a).getHeadline().length() > 0 ? ((TextAd)a).getHeadline() : ((TextAd)a).getDescription1(), a);
					}
				}
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void getAdGroups(Campaign campaign) throws ApiException, RemoteException {
		AdGroupServiceInterface adGroupService = adWordsServices.get(session, AdGroupServiceInterface.class);
		if (allAdGroups != null) {
			allAdGroups.clear();
		}
		int offset = 0;
		boolean morePages = true;

		// Create selector.
		SelectorBuilder builder = new SelectorBuilder();
		Selector selector = builder
				.fields(AdGroupField.Id, AdGroupField.Name)
				.orderAscBy(AdGroupField.Name)
				.offset(offset)
				.limit(PAGE_SIZE)
				.equals(AdGroupField.CampaignId, campaign.getId().toString())
				.build();

		while (morePages) {
			// Get all ad groups.
			AdGroupPage page = adGroupService.get(selector);

			// Display ad groups.
			if (page.getEntries() != null) {
				if (allAdGroups == null) {
					allAdGroups = new ArrayList<>(page.getTotalNumEntries());
				}
				for (AdGroup adGroup : page.getEntries()) {
					allAdGroups.add(adGroup);
				}
			} else {
				System.out.println("No ad groups were found.");
			}

			offset += PAGE_SIZE;
			selector = builder.increaseOffsetBy(PAGE_SIZE).build();
			morePages = offset < page.getTotalNumEntries();
		}
		if (allAdGroups == null) {
			allAdGroups = new ArrayList<>(0);
		}
	}

	public void getAds(Long adGroupId) throws ApiException, RemoteException {
		// Get the AdGroupAdService.
		AdGroupAdServiceInterface adGroupAdService = adWordsServices.get(session, AdGroupAdServiceInterface.class);
		if (allAds != null) {
			allAds.clear();
		}
		int offset = 0;
		boolean morePages = true;

		// Create selector.
		SelectorBuilder builder = new SelectorBuilder();
		Selector selector = builder
				.fields(AdGroupAdField.Id, AdGroupAdField.AdGroupId, AdGroupAdField.Status)
				.orderAscBy(AdGroupAdField.Id)
				.offset(offset)
				.limit(PAGE_SIZE)
				.equals(AdGroupAdField.AdGroupId, adGroupId.toString())
				.in(AdGroupAdField.Status, "ENABLED", "PAUSED", "DISABLED")
				//				.equals("AdType", "TEXT_AD")
				.build();

		while (morePages) {
			// Get all ads.
			AdGroupAdPage page = adGroupAdService.get(selector);

			// Display ads.
			if (page.getEntries() != null && page.getEntries().length > 0) {
				if (allAds == null) {
					allAds = new ArrayList<>(page.getTotalNumEntries());
				}
				for (AdGroupAd adGroupAd : page.getEntries()) {
					allAds.add(adGroupAd.getAd());
				}
			} else {
				System.out.println("No ads were found.");
			}

			offset += PAGE_SIZE;
			selector = builder.increaseOffsetBy(PAGE_SIZE).build();
			morePages = offset < page.getTotalNumEntries();
		}
		if (allAds == null) {
			allAds = new ArrayList<>(0);
		}
	}

	public void getCampaigns() {
		CampaignServiceInterface campaignService = adWordsServices.get(session, CampaignServiceInterface.class);
		if (	allCampaigns != null) {
			allCampaigns.clear();
		}

		int offset = 0;

		// Create selector.
		SelectorBuilder builder = new SelectorBuilder();
		Selector selector = builder
				.fields(CampaignField.Id, CampaignField.Name)
				.orderAscBy(CampaignField.Name)
				.offset(offset)
				.limit(PAGE_SIZE)
				.build();

		CampaignPage page = null;
		do {
			// Get all campaigns.
			try {
				page = campaignService.get(selector);
			} catch (RemoteException e) {
				e.printStackTrace();
			}

			// Display campaigns.
			if (page.getEntries() != null) {
				if (allCampaigns == null) {
					allCampaigns = new ArrayList<>(page.getTotalNumEntries());
				}
				for (Campaign campaign : page.getEntries()) {
					allCampaigns.add(campaign);
				}
			} else {
				System.out.println("No campaigns were found.");
			}

			offset += PAGE_SIZE;
			selector = builder.increaseOffsetBy(PAGE_SIZE).build();
		} while (offset < page.getTotalNumEntries());
		
		if (allCampaigns == null) {
			allCampaigns = new ArrayList<>(0);
		}
	}
*/
}
