package com.hurence.historian.greensights.model;


import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.hurence.historian.greensights.model.OneByteModel.*;

@Data
public class EnergyImpactMetric {

    public static final long AVERAGE_TIME_ON_PAGE_IN_SEC = 44L;
    private String rootUrl;
    private String country;
    private String pagePath;
    private String deviceCategory;     // loaded from GA
    private int pageViews = 1;            // loaded from GA
    private long avgTimeOnPageInSec = AVERAGE_TIME_ON_PAGE_IN_SEC;    // loaded from GA
    private long pageSizeInBytes = -1L;      // loaded from web scrapping

    private String dateRangeStart;
    private String dateRangeEnd;

    private Date metricDate;




    /**
     * The energy impact of one action will be computed with the following formula :
     * <p>
     * EnergyImpactInKwh = timeSpentOnActionInMin * deviceImpact + dataSizeInByte * (dataCenterImpact + networkImpact)
     * <p>
     * So we need to know :
     * <p>
     * - time spent on the action
     * - the type of device
     * - the data size involved in the action
     * <p>
     * According to
     * the [1byteModel](https://theshiftproject.org/wp-content/uploads/2019/10/Lean-ICT-Materials-Liens-%C3%A0-t%C3%A9l%C3%A9charger-r%C3%A9par%C3%A9-le-29-10-2019.pdf)
     * given by TheshiftProject
     *
     * @return
     */
    public double getEnergyImpactInKwh() {

        // let's default with a destop on a wired network
        double deviceImpact = ENERGY_IMPACT_DEVICE_DESKTOP;
        double networkImpact = ENERGY_IMPACT_FOR_FAN_WIRED;

        // switch to smartphone on mobile network
        if (!deviceCategory.equalsIgnoreCase("desktop")) {
            deviceImpact = ENERGY_IMPACT_DEVICE_SMARTPHONE;
            networkImpact = ENERGY_IMPACT_FOR_MOBILE_NETWORK;
        }

        // convert to min
        double timeSpentOnActionInMin = avgTimeOnPageInSec / 60.0;

        double singlePageImpact = timeSpentOnActionInMin * deviceImpact + pageSizeInBytes * (ENERGY_IMPACT_DUE_TO_DATACENTERS + networkImpact);

        return pageViews * singlePageImpact;
    }

    /**
     * @return CO2 equivalence
     */
    public double getCo2EqInKg() {
        return getEnergyImpactInKwh() * OneByteModel.getCarbonIntensityFactor(country);
    }

    public Map<String, String> getLabels() {

        HashMap<String, String> labels = new HashMap<>();
        labels.put("root_url", rootUrl);
        labels.put("page_path", pagePath);
        labels.put("country", country);
        labels.put("device_category", deviceCategory);
        return labels;
    }

    public String getFullUrl() {
        return getRootUrl() + getPagePath();
    }
}
