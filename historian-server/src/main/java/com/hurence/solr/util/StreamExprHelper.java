package com.hurence.solr.util;

import java.util.List;
import static com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler.joinListAsString;


public class StreamExprHelper {

    private StreamExprHelper() {}

    public static String createRollup(String streamingExpr, String overFields, List<String> fieldsToAgg) {
        final StringBuilder rollUpExprStr = new StringBuilder("rollup(" + streamingExpr + ", " +
                "over=" + overFields);
        fieldsToAgg.forEach(f -> {
            rollUpExprStr.append(", ").append(f);
        });
        rollUpExprStr.append(")");
        return rollUpExprStr.toString();
    }

    public static String createSort(String streamingExpr, String byPart) {
        return "sort(" + streamingExpr + ", " + byPart + ")";
    }

    public static String createSelect(String streamingExpr, List<String> fieldsSelected) {
        final StringBuilder selectExprStr = new StringBuilder("select(" + streamingExpr + ", ");
        selectExprStr.append(joinListAsString(fieldsSelected));
        selectExprStr.append(")");
        return selectExprStr.toString();
    }

    public static String createSearch(String collection,
                                      String qSection,
                                      String[] fqSection,
                                      String flSection,
                                      String qtSection,
                                      String sortSection) {
        String fq = "";
        if (fqSection != null) {
            for (String filterQuery : fqSection) {
                fq = filterQuery;
            }
        }
        return "search(" + collection + ", q=" + qSection + ", fq=" + fq +
                ", fl=" + flSection + ", qt=" + qtSection + ", sort=" + sortSection +")";
    }
}
