package com.hurence.webapiservice.historian.impl;

import com.hurence.historian.modele.solr.SolrFieldMapping;
import com.hurence.webapiservice.timeseries.extractor.MetricRequest;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianServiceFields.QUALITY_CHECK;
import static com.hurence.solr.util.StreamExprHelper.*;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.findNeededTagsName;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.joinListAsString;

public class NumberOfPointsWithQualityOkByMetricHelperImpl implements NumberOfPointsByMetricHelper {

    SolrFieldMapping solrMapping;
    String chunkCollection;
    SolrQuery query;
    List<MetricRequest> requests;

    public NumberOfPointsWithQualityOkByMetricHelperImpl(SolrFieldMapping solrMapping,
                                                         String chunkCollection,
                                                         SolrQuery query,
                                                         List<MetricRequest> requests){
        this.solrMapping = solrMapping;
        this.chunkCollection = chunkCollection;
        this.query = query;
        this.requests = requests;
    }

    @Override
    public String getStreamExpression() {
        List<String> neededFields = findNeededTagsName(requests);
        neededFields.add(solrMapping.CHUNK_NAME);
        List<String> overFields = new ArrayList<>(neededFields);
        overFields.add(QUALITY_CHECK);
        neededFields.add(solrMapping.CHUNK_COUNT_FIELD);
        String overString = joinListAsString(overFields);
        neededFields.add(getAggFieldForFilteringQuality());
        List<String> selectFields = new ArrayList<>(neededFields);
        selectFields.add(getIsQualityOkField(requests));
        String flString = joinListAsString(neededFields);
        String baseSearchQuery =  createSearch(
                chunkCollection,
                query.getQuery(),
                "\"" + flString + "\"",
                "\"/export\"",
                "\"" +solrMapping.CHUNK_NAME + " asc\""
        );
        String selectedWrapper = createSelect(baseSearchQuery, selectFields);
        List<String> fieldsAggInRoll = new ArrayList<>();
        fieldsAggInRoll.add("sum(\"" + solrMapping.CHUNK_COUNT_FIELD + ")");
        fieldsAggInRoll.add("count(*)");
        String rollUpExpr = createRollup(selectedWrapper,
                        "\"" + overString +  "\"",
                            fieldsAggInRoll
                );
//        String streamExpression = "rollup(select(search(";
//        StringBuilder exprBuilder = new StringBuilder(streamExpression).append(chunkCollection)
//                .append(",q=").append(query.getQuery());
//        if (query.getFilterQueries() != null) {
//            for (String filterQuery : query.getFilterQueries()) {
//                exprBuilder
//                        .append(",fq=").append(filterQuery);
//            }
//        }


//        exprBuilder.append(",fl=\"").append(flString).append("\"")
//                .append(",qt=\"/export\", sort=\"").append(solrMapping.CHUNK_NAME).append(" asc\"),")
//                .append(joinListAsString(selectFields))
//                .append("),");
//        exprBuilder.append("over=\"").append(overString).append("\"")
//                .append(", sum(").append(solrMapping.CHUNK_COUNT_FIELD).append("), count(*))");
//        return exprBuilder;
        return rollUpExpr;
    }

    private String getIsQualityOkField(List<MetricRequest> requests) {
        List<String> isQualityOkList = getIsQualityOkFieldForEachMetricRequest(requests);
        return joinQualityListAsString(isQualityOkList);
    }

    private String joinQualityListAsString1(List<String> isQualityOkList) {
        String finalString = "";
        for (int i = 1; i < isQualityOkList.size(); i++) {
            if (i == 1)
                 finalString = getOrExpression(isQualityOkList.get(0), isQualityOkList.get(1));
            else
                finalString = getOrExpression(finalString, isQualityOkList.get(i));
        }
        return finalString;
    }

    private String getOrExpression(String string1, String string2) {
        String finalString = "or(" + string1 +
                " , " + string2 + ")";
        return finalString;
    }

    private String joinQualityListAsString(List<String> isQualityOkList) {
        if (isQualityOkList.size() > 1)
            return joinQualityListAsString1(isQualityOkList) + " as " + QUALITY_CHECK;
        else
            return isQualityOkList.get(0) + " as " + QUALITY_CHECK;
    }

    private List<String> getIsQualityOkFieldForEachMetricRequest(List<MetricRequest> requests) {
        List<String> isQualityOkList = new ArrayList<>();
        requests.forEach(metricRequest -> {
            StringBuilder conditionForOneMetricRequest = new StringBuilder();
            conditionForOneMetricRequest.append("and (");
            conditionForOneMetricRequest.append("eq(");
            conditionForOneMetricRequest.append(solrMapping.CHUNK_NAME);
            conditionForOneMetricRequest.append(",");
            conditionForOneMetricRequest.append(metricRequest.getName());
            conditionForOneMetricRequest.append("), ");
            metricRequest.getTags().forEach((tagName, tagValue) ->
                    conditionForOneMetricRequest.append("eq("+tagName+","+tagValue+"), ")
            );
            conditionForOneMetricRequest.append("gteq("+ getAggFieldForFilteringQuality()+","
                    +metricRequest.getQuality().getQualityValue().toString()+"))");
            isQualityOkList.add(conditionForOneMetricRequest.toString());
        });
        return isQualityOkList;
    }

    private String getAggFieldForFilteringQuality() {
        //for the moment we filter every time on QUALITY_AVG
        return solrMapping.CHUNK_QUALITY_AVG_FIELD;
    }
}
