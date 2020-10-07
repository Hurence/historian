package com.hurence.historian.converter;


import com.hurence.timeseries.model.Chunk;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionCurrent;
import com.hurence.timeseries.model.Definitions;
import org.apache.solr.common.SolrInputDocument;

import static com.hurence.timeseries.model.Definitions.*;

public class SolrDocumentBuilder {


    public static SolrInputDocument fromChunk(Chunk chunk) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SOLR_COLUMN_NAME, chunk.getName());
        doc.addField(SOLR_COLUMN_START, chunk.getStart());
        doc.addField(SOLR_COLUMN_END, chunk.getEnd());
        doc.addField(SOLR_COLUMN_COUNT, chunk.getCount());
        chunk.getTags().keySet().forEach(key -> {
            doc.addField(key, chunk.getTag(key));
        });
        doc.addField(SOLR_COLUMN_VALUE, chunk.getValueAsString());
        doc.addField(SOLR_COLUMN_YEAR, chunk.getYear());
        doc.addField(SOLR_COLUMN_MONTH, chunk.getMonth());
        doc.addField(SOLR_COLUMN_DAY, chunk.getDay());
        doc.addField(SOLR_COLUMN_ORIGIN, chunk.getOrigin());
        doc.setField(SOLR_COLUMN_ID, chunk.getId());
        doc.setField(Definitions.SOLR_COLUMN_VERSION, chunk.getId());
        doc.setField(SOLR_COLUMN_METRIC_KEY, chunk.getMetricKey());
        doc.setField(SOLR_COLUMN_QUALITY_MIN, chunk.getQualityMin());
        doc.setField(SOLR_COLUMN_QUALITY_MAX, chunk.getQualityMax());
        doc.setField(SOLR_COLUMN_QUALITY_FIRST, chunk.getQualityFirst());
        doc.setField(SOLR_COLUMN_QUALITY_SUM, chunk.getQualitySum());
        doc.setField(SOLR_COLUMN_QUALITY_AVG, chunk.getQualityAvg());
        doc.setField(SOLR_COLUMN_MAX, chunk.getMax());
        doc.setField(SOLR_COLUMN_MIN, chunk.getMin());
        doc.setField(SOLR_COLUMN_AVG, chunk.getAvg());
        doc.setField(SOLR_COLUMN_SAX, chunk.getSax());
        doc.setField(SOLR_COLUMN_TREND, chunk.isTrend());
        doc.setField(SOLR_COLUMN_OUTLIER, chunk.isOutlier());
        doc.setField(SOLR_COLUMN_FIRST, chunk.getFirst());
        doc.setField(SOLR_COLUMN_LAST, chunk.getLast());
        doc.setField(SOLR_COLUMN_SUM, chunk.getSum());
        doc.setField(SOLR_COLUMN_STD_DEV, chunk.getStdDev());
        return doc;
    }

}
