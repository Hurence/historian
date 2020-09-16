package com.hurence.historian.converter;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.model.Chunk;
import org.apache.solr.common.SolrInputDocument;

public class SolrDocumentBuilder {

    public static SolrInputDocument fromChunk(Chunk chunk, SchemaVersion version) {
        switch (version) {
            case VERSION_0:

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
                doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN, chunk.getChunkOrigin());
                doc.setField(HistorianChunkCollectionFieldsVersion0.ID, chunk.getId());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX, chunk.getMax());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN, chunk.getMin());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG, chunk.getAvg());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX, chunk.getSax());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND, chunk.isTrend());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_OUTLIER, chunk.isOutlier());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST, chunk.getFirst());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_LAST, chunk.getLast());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM, chunk.getSum());
                doc.setField(HistorianChunkCollectionFieldsVersion0.CHUNK_STDDEV, chunk.getStd());
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
