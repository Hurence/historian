package com.hurence.historian.greensights.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
public class UserLastHourWebBrowsingMetric {

    private String userUUID;
    private String country;
    private Date metricDate;
    private String deviceCategory;

    private int pageViews;
    private long transferredDataInBytes;
    private long timeSpentBrowsingInSec;
    private long avgTimeOnPageInSec;
    private long avgPageSizeInBytes;
    private double energyImpactInKwh;
    private double co2EqInKg;

    private double numberOfChargedSmartphonesEq;
    private double kmsByCarEq;
    private double neededTreesEq;


    public Map<String, String> getLabels() {

        HashMap<String, String> labels = new HashMap<>();
        labels.put("user_uuid", userUUID);
        labels.put("country", OneByteModel.getZoneFromCountry(country));
        labels.put("device_category", deviceCategory);
        labels.put("scope", "user");
        return labels;
    }

}
