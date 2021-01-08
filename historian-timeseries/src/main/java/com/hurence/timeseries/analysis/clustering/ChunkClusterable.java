package com.hurence.timeseries.analysis.clustering;


import org.apache.commons.math3.ml.clustering.Clusterable;

import java.util.Map;


public interface ChunkClusterable extends Clusterable {

    String getSax();

    String getId();

    Map<String, String> getTags();

    void setTags(Map<String, String> tags);
}
