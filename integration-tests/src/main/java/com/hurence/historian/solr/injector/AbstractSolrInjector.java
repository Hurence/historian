package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSolrInjector implements SolrInjector {

    protected int ddcThreshold = 0;
    private static String COLLECTION = SolrITHelper.COLLECTION_HISTORIAN;
    private List<ChunkModeleVersion0> extraCustomChunks = new ArrayList<>();

    protected byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    @Override
    public void injectChunks(SolrClient client) throws SolrServerException, IOException {
        final List<ChunkModeleVersion0> chunks = buildListOfChunks();
        chunks.addAll(extraCustomChunks);
        for(int i = 0; i < chunks.size(); i++) {
            ChunkModeleVersion0 chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected, "id" + i));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION, true, true);
    }

    public void addChunk(ChunkModeleVersion0 chunk) {
        extraCustomChunks.add(chunk);
    }

    public void addChunk(AbstractSolrInjector injector) {
        extraCustomChunks.addAll(injector.buildListOfChunks());
    }

    protected abstract List<ChunkModeleVersion0> buildListOfChunks();

    private SolrInputDocument buildSolrDocument(ChunkModeleVersion0 chunk, String id) {
        return chunk.buildSolrDocument(id);
    }
}
