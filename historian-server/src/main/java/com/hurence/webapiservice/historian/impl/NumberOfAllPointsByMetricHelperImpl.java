package com.hurence.webapiservice.historian.impl;

import com.hurence.historian.modele.HistorianConf;
import com.hurence.historian.modele.solr.SolrFieldMapping;
import com.hurence.webapiservice.timeseries.extractor.MetricRequest;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.findNeededTagsName;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.joinListAsString;

public class NumberOfAllPointsByMetricHelperImpl implements NumberOfPointsByMetricHelper {

    SolrFieldMapping solrMapping;
    String chunkCollection;
    SolrQuery query;
    List<MetricRequest> requests;

    public NumberOfAllPointsByMetricHelperImpl(SolrFieldMapping solrMapping,
                                               String chunkCollection,
                                               SolrQuery query,
                                               List<MetricRequest> requests) {
        this.chunkCollection = chunkCollection;
        this.solrMapping = solrMapping;
        this.query = query;
        this.requests = requests;
    }

    @Override
    public StringBuilder getStreamExpression() {
        String streamExpression = "rollup(search(";
        StringBuilder exprBuilder = new StringBuilder(streamExpression).append(chunkCollection)
                .append(",q=").append(query.getQuery());
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append(",fq=").append(filterQuery);
            }
        }
        List<String> neededFields = findNeededTagsName(requests);
        neededFields.add(solrMapping.CHUNK_NAME);
        List<String> overFields = new ArrayList<>(neededFields);
        String overString = joinListAsString(overFields);
        neededFields.add(solrMapping.CHUNK_COUNT_FIELD);
        String flString = joinListAsString(neededFields);
        exprBuilder.append(",fl=\"").append(flString).append("\"")
                .append(",qt=\"/export\", sort=\"").append(solrMapping.CHUNK_NAME).append(" asc\")")
                .append(",over=\"").append(overString).append("\"")
                .append(", sum(").append(solrMapping.CHUNK_COUNT_FIELD).append("), count(*))");
        return exprBuilder;
    }


}
