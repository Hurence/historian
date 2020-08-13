package com.hurence.webapiservice.historian.impl;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianFields.CHUNK_COUNT_FIELD;
import static com.hurence.historian.modele.HistorianFields.NAME;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.joinListAsString;

public class NumberOfAllPointsByMetricHelperImpl implements NumberOfPointsByMetricHelper {

    @Override
    public StringBuilder getExpression(StringBuilder exprBuilder, List<String> neededFields) {

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

    @Override
    public String getStreamExpression() {
        return "rollup(search(";
    }
}
