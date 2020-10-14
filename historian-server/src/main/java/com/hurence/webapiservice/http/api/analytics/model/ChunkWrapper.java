package com.hurence.webapiservice.http.api.analytics.model;

import com.hurence.timeseries.analysis.clustering.ChunkClusterable;

import java.util.HashMap;
import java.util.Map;

public class ChunkWrapper implements ChunkClusterable {

    private String sax;
    private String id;
    private Map<String,String> tags = new HashMap<>();

    public ChunkWrapper(String id, String sax,Map<String,String> tags ) {
        this.sax = sax;
        this.id = id;
        this.tags = tags;
    }

    @Override
    public String getSax() {
        return sax;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public double[] getPoint() {
        assert sax != null;
        double[] saxAsDoubles = new double[sax.length()];

        for (int i = 0; i < sax.length(); i++) {
            saxAsDoubles[i] = (Character.getNumericValue(sax.charAt(i)) - 10.0) ;/// 7.0;
        }

        return saxAsDoubles;
    }
}
