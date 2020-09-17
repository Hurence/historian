package com.hurence.historian.compactor.config;


import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Configuration {

    private String solrZkHost = null;
    private String solrCollection = null;
    private int compactionSchedulingPeriod = -1;
    private boolean compactionSchedulingStartNow = true;

    public String getSolrZkHost() {
        return solrZkHost;
    }

    public void setSolrZkHost(String solrZkHost) {
        this.solrZkHost = solrZkHost;
    }

    public String getSolrCollection() {
        return solrCollection;
    }

    public void setSolrCollection(String solrCollection) {
        this.solrCollection = solrCollection;
    }

    public int getCompactionSchedulingPeriod() {
        return compactionSchedulingPeriod;
    }

    public void setCompactionSchedulingPeriod(int compactionSchedulingPeriod) {
        this.compactionSchedulingPeriod = compactionSchedulingPeriod;
    }

    public boolean isCompactionSchedulingStartNow() {
        return compactionSchedulingStartNow;
    }

    public void setCompactionSchedulingStartNow(boolean compactionSchedulingStartNow) {
        this.compactionSchedulingStartNow = compactionSchedulingStartNow;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
