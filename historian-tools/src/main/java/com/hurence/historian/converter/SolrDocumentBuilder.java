package com.hurence.historian.converter;


import com.hurence.timeseries.model.Chunk;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionCurrent;
import org.apache.solr.common.SolrInputDocument;

public class SolrDocumentBuilder {


    public static SolrInputDocument fromChunk(Chunk chunk) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.NAME, chunk.getName());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_START, chunk.getStart());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_END, chunk.getEnd());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_COUNT, chunk.getCount());
        chunk.getTags().keySet().forEach(key -> {
            doc.addField(key, chunk.getTag(key));
        });
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_VALUE, chunk.getValueAsString());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_YEAR, chunk.getYear());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MONTH, chunk.getMonth());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_DAY, chunk.getDay());
        doc.addField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_ORIGIN, chunk.getChunkOrigin());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.ID, chunk.getId());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MAX, chunk.getMax());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MIN, chunk.getMin());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_AVG, chunk.getAvg());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SAX, chunk.getSax());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_TREND, chunk.isTrend());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_OUTLIER, chunk.isOutlier());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_FIRST, chunk.getFirst());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_LAST, chunk.getLast());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SUM, chunk.getSum());
        doc.setField(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_STDDEV, chunk.getStd());
        return doc;
    }

}
