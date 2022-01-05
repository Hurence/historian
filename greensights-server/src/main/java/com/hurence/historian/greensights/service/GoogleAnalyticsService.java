package com.hurence.historian.greensights.service;


import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.model.*;
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;
import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes;
import com.google.api.services.analyticsreporting.v4.model.*;
import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.request.ComputeRequest;
import com.hurence.historian.greensights.model.solr.WebPageAnalysis;
import com.hurence.historian.greensights.util.DateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@Service
public class GoogleAnalyticsService {

    private static final String APPLICATION_NAME = "greensights";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final Logger log = LogManager.getLogger(GoogleAnalyticsService.class);


    @Autowired
    private PageSizeService pageSizeService;




    /**
     * get energy impact metrics from google analytics
     *
     * @return
     */
    public List<EnergyImpactMetric> retrieveMetrics(ComputeRequest computeRequest) {

        List<EnergyImpactMetric> energyImpactMetrics = new ArrayList<>();

        try {
            AnalyticsReporting analyticsReporting = initializeAnalyticsReporting();
            Analytics analytics = initializeAnalytics();

            List<ViewProperty> allViewProperties = getAllViewProperties(analytics);
            for (ViewProperty viewProperty : allViewProperties) {
                GetReportsResponse report = getReport(analyticsReporting, viewProperty.getViewId(), computeRequest);
                List<EnergyImpactMetric> metrics = getMetrics(report, viewProperty);
                metrics.forEach(energyImpactMetric -> {
                    // we need page size from another service
                    WebPageAnalysis webPageAnalysis = pageSizeService.getPageSize(energyImpactMetric.getFullUrl());
                    energyImpactMetric.setPageSizeInBytes(webPageAnalysis.getPageSizeInBytes());
                    energyImpactMetric.setDateRangeStart(computeRequest.getStartDate());
                    energyImpactMetric.setDateRangeEnd(computeRequest.getEndDate());
                   energyImpactMetric.setMetricDate(DateUtils.fromDateRequest(computeRequest.getEndDate()));

                });
                energyImpactMetrics.addAll(metrics);
            }


        } catch (Exception e) {
            log.error(e.getMessage());
        }

        return energyImpactMetrics;
    }


    @Value("${scraper.jsonKeyFile}")
    private String KEY_FILE_LOCATION;

    private String PROPERTY_ID = "3122248625"; //"196132511";
    private String VIEW_ID = "196132511";


    /**
     * Initializes an Analytics service object.
     *
     * @return An authorized Analytics service object.
     * @throws IOException
     * @throws GeneralSecurityException
     */
    private Analytics/*Reporting*/ initializeAnalytics() throws GeneralSecurityException, IOException {

        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = GoogleCredential
                .fromStream(new FileInputStream(KEY_FILE_LOCATION))
                .createScoped(AnalyticsScopes.all());

        // Construct the Analytics service object.
        return new Analytics.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
    }


    /**
     * Initializes an Analytics Reporting API V4 service object.
     *
     * @return An authorized Analytics Reporting API V4 service object.
     * @throws IOException
     * @throws GeneralSecurityException
     */
    private  AnalyticsReporting initializeAnalyticsReporting() throws GeneralSecurityException, IOException {

        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = GoogleCredential
                .fromStream(new FileInputStream(KEY_FILE_LOCATION))
                .createScoped(AnalyticsReportingScopes.all());

        // Construct the Analytics Reporting service object.
        return new AnalyticsReporting.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
    }

    @Data
    @AllArgsConstructor
    class ViewProperty {

        private String accountId;
        private String accountName;
        private String propertyId;
        private String propertyName;
        private String viewId;
        private String viewName;
        private String websiteUrl;
    }




    private List<ViewProperty> getAllViewProperties(Analytics analytics) throws IOException {
        List<ViewProperty> viewProperties = new ArrayList<>();

        // Query for the list of all accounts associated with the service account.
        Accounts accounts = analytics.management().accounts().list().execute();

        if (accounts.getItems().isEmpty()) {
            log.error("No accounts found");
        } else {
            for (Account account : accounts.getItems()) {
                String accountId = account.getId();
                String accountName = account.getName();

                // Query for the list of properties associated with the first account.
                Webproperties properties = analytics.management().webproperties()
                        .list(accountId).execute();

                if (properties.getItems().isEmpty()) {
                    log.error("No Web properties found for account : " + accountName);
                } else {
                    for (Webproperty property : properties.getItems()) {
                        String websiteUrl = property.getWebsiteUrl();
                        String propertyId = property.getId();
                        String propertyName = property.getName();

                        // Query for the list views (profiles) associated with the property.
                        Profiles profiles = analytics.management().profiles()
                                .list(accountId, propertyId).execute();

                        if (profiles.getItems().isEmpty()) {
                            log.error("No views found");
                        } else {

                            profiles.getItems().forEach(view -> {

                                String viewId = view.getId();
                                String viewName = view.getName();

                                ViewProperty viewProperty = new ViewProperty(
                                        accountId,
                                        accountName,
                                        propertyId,
                                        propertyName,
                                        viewId,
                                        viewName,
                                        websiteUrl);
                                viewProperties.add(viewProperty);
                            });
                        }
                    }

                }
            }
        }
        return viewProperties;
    }





    /**
     * Queries the Analytics Reporting API V4.
     *
     * @param service An authorized Analytics Reporting API V4 service object.
     * @param viewId
     * @return GetReportResponse The Analytics Reporting API V4 response.
     * @throws IOException
     */
    private GetReportsResponse getReport(AnalyticsReporting service, String viewId, ComputeRequest computeRequest) throws IOException {
        // Create the DateRange object.
        DateRange dateRange = new DateRange();
        dateRange.setStartDate(computeRequest.getStartDate());
        dateRange.setEndDate(computeRequest.getEndDate());

        // Create the Metrics object.
        Metric pageViews = new Metric()
                .setExpression("ga:pageviews")
                .setAlias("pageviews");

        Metric avgTimeOnPage = new Metric()
                .setExpression("ga:avgTimeOnPage")
                .setAlias("avgTimeOnPage");


        Dimension deviceCategory = new Dimension().setName("ga:deviceCategory");
        Dimension country = new Dimension().setName("ga:country");
        Dimension pagePath = new Dimension().setName("ga:pagePath");


        // Create the ReportRequest object.
        ReportRequest request = new ReportRequest()
                .setViewId(viewId)
                .setDateRanges(Arrays.asList(dateRange))
                .setMetrics(Arrays.asList(pageViews,avgTimeOnPage))
                .setDimensions(Arrays.asList(deviceCategory, country, pagePath));

        ArrayList<ReportRequest> requests = new ArrayList<ReportRequest>();
        requests.add(request);

        // Create the GetReportsRequest object.
        GetReportsRequest getReport = new GetReportsRequest()
                .setReportRequests(requests);

        // Call the batchGet method.
        GetReportsResponse response = service.reports().batchGet(getReport).execute();

        // Return the response.
        return response;
    }



    private  List<EnergyImpactMetric> getMetrics(GetReportsResponse response, ViewProperty viewProperty) {

        List<EnergyImpactMetric> energyImpactMetrics = new ArrayList<>();

        for (Report report: response.getReports()) {
            ColumnHeader header = report.getColumnHeader();
            List<String> dimensionHeaders = header.getDimensions();
            List<MetricHeaderEntry> metricHeaders = header.getMetricHeader().getMetricHeaderEntries();
            List<ReportRow> rows = report.getData().getRows();

            if (rows == null) {
                log.info("No data found for " + VIEW_ID);
                return Collections.emptyList();
            }

            for (ReportRow row: rows) {
                EnergyImpactMetric metric = new EnergyImpactMetric();
                List<String> dimensions = row.getDimensions();
                List<DateRangeValues> metrics = row.getMetrics();

                for (int i = 0; i < dimensionHeaders.size() && i < dimensions.size(); i++) {
                    if(dimensionHeaders.get(i).equals("ga:deviceCategory"))
                        metric.setDeviceCategory(dimensions.get(i));
                    if(dimensionHeaders.get(i).equals("ga:country"))
                        metric.setCountry(dimensions.get(i));
                    if(dimensionHeaders.get(i).equals("ga:pagePath"))
                        metric.setPagePath(dimensions.get(i));
                }

                for (int j = 0; j < metrics.size(); j++) {
                  //  log.info("Date Range (" + j + "): ");
                    DateRangeValues values = metrics.get(j);
                    for (int k = 0; k < values.getValues().size() && k < metricHeaders.size(); k++) {
                        if(metricHeaders.get(k).getName().equals("pageviews"))
                            metric.setPageViews(Integer.parseInt(values.getValues().get(k)));
                        if(metricHeaders.get(k).getName().equals("avgTimeOnPage"))
                            metric.setAvgTimeOnPageInSec((long) Float.parseFloat(values.getValues().get(k)));
                    }
                }
                metric.setRootUrl(viewProperty.getWebsiteUrl());
                if(metric.getAvgTimeOnPageInSec() == 0)
                    metric.setAvgTimeOnPageInSec(EnergyImpactMetric.AVERAGE_TIME_ON_PAGE_IN_SEC);

                energyImpactMetrics.add(metric);
            }

        }
        return  energyImpactMetrics;
    }

    /**
     * Parses and prints the Analytics Reporting API V4 response.
     *
     * @param response An Analytics Reporting API V4 response.
     */
    private  void printResponse(GetReportsResponse response) {

        for (Report report: response.getReports()) {
            ColumnHeader header = report.getColumnHeader();
            List<String> dimensionHeaders = header.getDimensions();
            List<MetricHeaderEntry> metricHeaders = header.getMetricHeader().getMetricHeaderEntries();
            List<ReportRow> rows = report.getData().getRows();

            if (rows == null) {
                log.info("No data found for " + VIEW_ID);
                return;
            }

            for (ReportRow row: rows) {
                List<String> dimensions = row.getDimensions();
                List<DateRangeValues> metrics = row.getMetrics();

                for (int i = 0; i < dimensionHeaders.size() && i < dimensions.size(); i++) {
                    log.info(dimensionHeaders.get(i) + ": " + dimensions.get(i));
                }

                for (int j = 0; j < metrics.size(); j++) {
                    log.info("Date Range (" + j + "): ");
                    DateRangeValues values = metrics.get(j);
                    for (int k = 0; k < values.getValues().size() && k < metricHeaders.size(); k++) {
                        log.info(metricHeaders.get(k).getName() + ": " + values.getValues().get(k));
                    }
                }
            }
        }
    }



    private GaData getResults(Analytics analytics, String profileId) throws IOException {

        // Query the Core Reporting API for the number of sessions
        // in the past seven days.
        return analytics.data().ga()
                .get("ga:" + profileId, "7daysAgo", "today", "ga:pageviews,ga:avgTimeOnPage")
                .execute();
    }

    private void printResults(GaData results) {
        // Parse the response from the Core Reporting API for
        // the profile name and number of sessions.
        if (results != null && !results.getRows().isEmpty()) {
            log.info("View (Profile) Name: "
                    + results.getProfileInfo().getProfileName());
            log.info("Total Sessions: " + results.getRows().get(0).get(0));
        } else {
            log.info("No results found");
        }
    }
}
