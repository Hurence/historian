package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.joinListAsString;
import static com.hurence.webapiservice.http.api.grafana.util.QualityConfig.getDefaultQualityAgg;

public class NumberOfPointsWithQualityOkByMetricHelperImpl implements NumberOfPointsByMetricHelper {

    List<MetricRequest> requests;

    public NumberOfPointsWithQualityOkByMetricHelperImpl(List<MetricRequest> requests){
        this.requests = requests;
    }

    @Override
    public StringBuilder getExpression(StringBuilder exprBuilder, List<String> neededFields) {
        List<String> overFields = new ArrayList<>(neededFields);
        overFields.add(QUALITY_CHECK);
        neededFields.add(CHUNK_COUNT_FIELD);
        String overString = joinListAsString(overFields);
        neededFields.add(getDefaultQualityAgg());
        List<String> selectFields = new ArrayList<>(neededFields);
        selectFields.add(getIsQualityOkField(requests));
        String flString = joinListAsString(neededFields);
        exprBuilder.append(",fl=\"").append(flString).append("\"")
                .append(",qt=\"/export\", sort=\"").append(NAME).append(" asc\"),")
                .append(joinListAsString(selectFields))
                .append("),");
        exprBuilder.append("over=\"").append(overString).append("\"")
                .append(", sum(").append(CHUNK_COUNT_FIELD).append("), count(*))");

        return exprBuilder;
    }


    @Override
    public String getStreamExpression() {
        return "rollup(select(search(";
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
            conditionForOneMetricRequest.append(NAME);
            conditionForOneMetricRequest.append(",");
            conditionForOneMetricRequest.append(metricRequest.getName());
            conditionForOneMetricRequest.append("), ");
            metricRequest.getTags().forEach((tagName, tagValue) ->
                    conditionForOneMetricRequest.append("eq("+tagName+","+tagValue+"), ")
            );
            conditionForOneMetricRequest.append("gteq("+getDefaultQualityAgg()+","
                    +metricRequest.getQuality().getQualityValue().toString()+"))");
            isQualityOkList.add(conditionForOneMetricRequest.toString());
        });
        return isQualityOkList;
    }
}
