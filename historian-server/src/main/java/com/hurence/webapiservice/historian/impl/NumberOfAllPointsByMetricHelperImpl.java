package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianFields.CHUNK_COUNT_FIELD;
import static com.hurence.historian.modele.HistorianFields.NAME;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.findNeededTagsName;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.joinListAsString;

public class NumberOfAllPointsByMetricHelperImpl implements NumberOfPointsByMetricHelper {

    String chunkCollection;
    SolrQuery query;
    List<MetricRequest> requests;

    public NumberOfAllPointsByMetricHelperImpl(String chunkCollection,
                                               SolrQuery query,
                                               List<MetricRequest> requests) {
        this.chunkCollection = chunkCollection;
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
        neededFields.add(NAME);
        List<String> overFields = new ArrayList<>(neededFields);
        String overString = joinListAsString(overFields);
        neededFields.add(CHUNK_COUNT_FIELD);
        String flString = joinListAsString(neededFields);
        exprBuilder.append(",fl=\"").append(flString).append("\"")
                .append(",qt=\"/export\", sort=\"").append(NAME).append(" asc\")")
                .append(",over=\"").append(overString).append("\"")
                .append(", sum(").append(CHUNK_COUNT_FIELD).append("), count(*))");
        return exprBuilder;
    }


}
