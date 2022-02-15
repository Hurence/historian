package com.hurence.historian.greensights.model.solr;

import com.hurence.historian.greensights.model.WebPageEnergyImpactMetric;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.solr.core.mapping.Indexed;
import org.springframework.data.solr.core.mapping.SolrDocument;

import java.util.Date;

@Data
@SolrDocument(collection = "greensights")
public class WebPageActivity {

    @Id
    @Indexed(name = "id", type = "string")
    private String id;

    @Indexed("doc_type_s")
    private final String doc_type = "webpage_activity_analysis";

    @Indexed("page_size_in_bytes_l")
    private long pageSizeInBytes;

    @Indexed("avg_time_on_page_in_sec_l")
    private long avgTimeOnPageInSec;

    @Indexed("page_views_i")
    private int pageViews;

    @Indexed("root_url_s")
    private String rootUrl;

    @Indexed("page_path_s")
    private String pagePath;

    @Indexed("country_s")
    private String country;

    @Indexed("metric_date_dt")
    private Date metricDate;

    @Indexed("date_range_start_s")
    private String dateRangeStart;

    @Indexed("date_range_end_s")
    private String dateRangeEnd;

    @Indexed("device_category_s")
    private String deviceCategory;

    @Indexed("energy_impact_in_kwh_d")
    private double energyImpactInKwh;

    @Indexed("co2_eq_in_kg_d")
    private double co2EqInKg;


    /**
     * convert an EnergyImpactMetric to a WebPageActivityAnalysis
     * @param metric
     * @return
     */
    public static WebPageActivity fromEnergyImpactMetric(WebPageEnergyImpactMetric metric){
        WebPageActivity analysis = new WebPageActivity();
        analysis.setPagePath(metric.getPagePath());
        analysis.setAvgTimeOnPageInSec(metric.getAvgTimeOnPageInSec());
        analysis.setPageViews(metric.getPageViews());
        analysis.setCountry(metric.getCountry());
        analysis.setEnergyImpactInKwh(metric.getEnergyImpactInKwh());
        analysis.setCo2EqInKg(metric.getCo2EqInKg());
        analysis.setDeviceCategory(metric.getDeviceCategory());
        analysis.setPageSizeInBytes(metric.getPageSizeInBytes());
        analysis.setRootUrl(metric.getRootUrl());
        analysis.setDateRangeStart(metric.getDateRangeStart());
        analysis.setDateRangeEnd(metric.getDateRangeEnd());
        analysis.setMetricDate(metric.getMetricDate());

        return analysis;
    }

}
