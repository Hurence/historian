package com.hurence.timeseries.analysis.clustering;


import com.hurence.timeseries.analysis.clustering.measures.JaccardDistanceMeasure;
import com.hurence.timeseries.analysis.clustering.measures.LevenshteinDistanceMeasure;
import com.hurence.timeseries.model.Chunk;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
public class KMeansChunksClustering implements ChunksClustering {

    public enum Distance{
        DEFAULT,
        JACCARD,
        LEVENSTHEIN
    }

    @Builder.Default int k = 3;
    @Builder.Default int maxIterations = 1000;
    @Builder.Default Distance distance = Distance.DEFAULT;



    private static Logger logger = LoggerFactory.getLogger(KMeansChunksClustering.class.getName());

    @Override
    public List<ChunkClusterable> cluster(List<ChunkClusterable> chunks) {

        logger.info("starting sax clustering with Jaccard Distance");
        KMeansPlusPlusClusterer<ChunkClusterable> clusterer =
                new KMeansPlusPlusClusterer<>(k, maxIterations);//, new LevenshteinDistanceMeasure());

        List<CentroidCluster<ChunkClusterable>> clusterResults = clusterer.cluster(chunks);
        logger.info("done sax clustering");


        AtomicInteger i = new AtomicInteger();
        clusterResults.forEach(chunkCentroidCluster -> {
            for (ChunkClusterable chunk : chunkCentroidCluster.getPoints()) {
                final Map<String, String> tags = new HashMap<>(chunk.getTags());
                tags.put("sax_cluster", String.valueOf(i.get()));
                chunk.setTags(tags);
            }
            i.addAndGet(1);
        });



        return chunks;
    }
}
