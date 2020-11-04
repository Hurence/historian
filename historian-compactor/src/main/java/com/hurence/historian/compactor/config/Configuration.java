package com.hurence.historian.compactor.config;


import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Configuration implements Serializable {

    private String solrZkHost = null;
    private String solrCollection = null;
    private Map<String, String> sparkConfig = new HashMap<String, String>();
    private int compactionSchedulingPeriod = -1;
    private boolean compactionSchedulingStartNow = true;

    public String getSolrZkHost() {
        return solrZkHost;
    }

    // Also the ConfigurationBuilder must be used, this method is public for
    // CompactorIT test access only
    public void setSolrZkHost(String solrZkHost) {
        this.solrZkHost = solrZkHost;
    }

    public String getSolrCollection() {
        return solrCollection;
    }

    void setSolrCollection(String solrCollection) {
        this.solrCollection = solrCollection;
    }

    public int getCompactionSchedulingPeriod() {
        return compactionSchedulingPeriod;
    }

    void setCompactionSchedulingPeriod(int compactionSchedulingPeriod) {
        this.compactionSchedulingPeriod = compactionSchedulingPeriod;
    }

    public boolean isCompactionSchedulingStartNow() {
        return compactionSchedulingStartNow;
    }

    void setCompactionSchedulingStartNow(boolean compactionSchedulingStartNow) {
        this.compactionSchedulingStartNow = compactionSchedulingStartNow;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public Map<String, String> getSparkConfig() {
        return sparkConfig;
    }

    void setSparkConfig(Map<String, String> sparkConfig) {
        this.sparkConfig = sparkConfig;
    }
}
