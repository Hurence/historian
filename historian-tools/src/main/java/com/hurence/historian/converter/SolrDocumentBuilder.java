package com.hurence.historian.converter;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.modele.chunk.Chunk;
import com.hurence.timeseries.modele.chunk.ChunkVersion0;
import org.apache.solr.common.SolrInputDocument;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class SolrDocumentBuilder {

    public static SolrInputDocument fromChunk(Chunk chunk, SchemaVersion version) {
        switch (version) {
            case VERSION_0:
                ChunkVersion0 chunkV0 = (ChunkVersion0) chunk;
                final SolrInputDocument doc = new SolrInputDocument();
                doc.addField(HistorianChunkCollectionFieldsVersion0.NAME, chunk.getName());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_START, chunk.getStart());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_END, chunk.getEnd());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT, chunk.getCount());
                chunk.getTags().keySet().forEach(key -> {
                    doc.addField(key, chunk.getTag(key));
                });
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE, chunk.getValueAsString());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR, chunk.getYear());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH, chunk.getMonth());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY, chunk.getDay());
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN, chunkV0.getOrigin());
                doc.setField(HistorianChunkCollectionFieldsVersion0.ID, chunkV0.getId());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX, chunkV0.getMax());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN, chunkV0.getMin());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG, chunkV0.getAvg());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX, chunkV0.getSax());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND, chunkV0.getTrend());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_OUTLIER, chunkV0.getOutlier());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST, chunkV0.getFirst());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_LAST, chunkV0.getLast());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM, chunkV0.getSum());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_STDDEV, chunkV0.getStddev());
                return doc;
            default:
                throw new IllegalStateException("Creation of Solr Document for chunk version " + version +
                        " is not supported !");
        }
    }
    public static SolrInputDocument fromChunk(Chunk chunk) {
        return fromChunk(chunk, chunk.getVersion());
    }
}
