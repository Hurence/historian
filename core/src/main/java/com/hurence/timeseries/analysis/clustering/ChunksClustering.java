package com.hurence.timeseries.analysis.clustering;


import org.apache.commons.math3.ml.clustering.Clusterable;

import java.util.List;

public interface ChunksClustering {
    List<ChunkClusterable> cluster(List<ChunkClusterable> chunks);
}
