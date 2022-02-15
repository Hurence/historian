package com.hurence.historian.greensights.model;


import com.hurence.historian.greensights.model.referential.OneByteModel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
public class UserWebBrowsingMetric {

    private String userUUID;
    private String organization = "none";
    private String country;
    private Date metricDate;
    private String deviceCategory;

    private int domainViews;
    private long transferredDataInBytes;
    private long timeSpentBrowsingInSec;

    /**
     * The energy impact of one action will be computed with the onebytemodel formula
     *
     * @return
     */
    public double getEnergyImpactInKwh() {
        return OneByteModel.getEnergyImpactInKwh(deviceCategory, timeSpentBrowsingInSec, transferredDataInBytes, 1);
    }

    /**
     * @return CO2 equivalence
     */
    public double getCo2EqInKg() {
        return OneByteModel.getCo2EqInKg(getEnergyImpactInKwh(), country);
    }

    public Map<String, String> getLabels() {

        HashMap<String, String> labels = new HashMap<>();
        labels.put("user_uuid", userUUID);
        labels.put("country", OneByteModel.getZoneFromCountry(country));
        labels.put("device_category", deviceCategory);
        labels.put("scope", "user");
        return labels;
    }

}
