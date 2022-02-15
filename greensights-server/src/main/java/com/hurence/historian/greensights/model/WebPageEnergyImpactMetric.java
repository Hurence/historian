package com.hurence.historian.greensights.model;


import com.hurence.historian.greensights.model.referential.OneByteModel;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


@Data
public class WebPageEnergyImpactMetric {

    private String rootUrl;
    private String country;
    private String pagePath;
    private String deviceCategory;     // loaded from GA
    private int pageViews = 1;            // loaded from GA

    @Value("${greensights.analytics.defaults.avgTimeOnPageInSec:44}")
    private long avgTimeOnPageInSec;    // loaded from GA

    @Value("${greensights.analytics.defaults.avgPageSizeInBytes:2000}")
    private long pageSizeInBytes = -1L;      // loaded from web scrapping

    private String dateRangeStart;
    private String dateRangeEnd;

    private Date metricDate;


    /**
     * The energy impact of one action will be computed with the onebytemodel formula
     *
     * @return
     */
    public double getEnergyImpactInKwh() {
        return OneByteModel.getEnergyImpactInKwh(deviceCategory,avgTimeOnPageInSec,pageSizeInBytes,pageViews);
    }

    /**
     * @return CO2 equivalence
     */
    public double getCo2EqInKg() {
        return OneByteModel.getCo2EqInKg(getEnergyImpactInKwh(), country);
    }

    public Map<String, String> getLabels() {

        HashMap<String, String> labels = new HashMap<>();
        labels.put("root_url", rootUrl);
        labels.put("page_path", pagePath);
        labels.put("country", OneByteModel.getZoneFromCountry(country));
        labels.put("device_category", deviceCategory);
        labels.put("scope", "metric");
        return labels;
    }

    public String getFullUrl() {
        return getRootUrl() + getPagePath();
    }
}
