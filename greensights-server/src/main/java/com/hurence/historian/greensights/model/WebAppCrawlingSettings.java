package com.hurence.historian.greensights.model;

import lombok.Data;


/**
 * https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/service-java
 */
@Data
public class WebAppCrawlingSettings {
    private String webAppName;
    private String jsonKeyFile;
}
