package com.hurence.timeseries.model;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.*;
import java.util.stream.Collectors;

public class Definitions {

    /**
     * Definitions of java field names
     */
    public static final String FIELD_QUALITY = "quality";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_TAGS = "tags";
    public static final String FIELD_ID = "id";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_DAY = "day";
    public static final String FIELD_START = "start";
    public static final String FIELD_END = "end";
    public static final String FIELD_COUNT = "count";
    public static final String FIELD_AVG = "avg";
    public static final String FIELD_STD_DEV = "stdDev";
    public static final String FIELD_MIN = "min";
    public static final String FIELD_MAX = "max";
    public static final String FIELD_FIRST = "first";
    public static final String FIELD_LAST = "last";
    public static final String FIELD_SAX = "sax";
    public static final String FIELD_VALUE = "value";
    public static final String FIELD_ORIGIN = "origin";
    public static final String FIELD_QUALITY_MIN = "qualityMin";
    public static final String FIELD_QUALITY_MAX = "qualityMax";
    public static final String FIELD_QUALITY_FIRST = "qualityFirst";
    public static final String FIELD_QUALITY_SUM = "qualitySum";
    public static final String FIELD_QUALITY_AVG = "qualityAvg";
    public static final String FIELD_TREND = "trend";
    public static final String FIELD_OUTLIER = "outlier";
    public static final String FIELD_VERSION = "version";
    public static final String FIELD_YEAR = "year";
    public static final String FIELD_MONTH = "month";
    public static final String FIELD_SUM = "sum";
    public static final String FIELD_METRIC_KEY = "metricKey";


    /**
     * Definitions of solr column names
     */
    public static final String SOLR_COLUMN_ID = "id";
    public static final String SOLR_COLUMN_NAME = "name";
    public static final String SOLR_COLUMN_DAY = "chunk_day";
    public static final String SOLR_COLUMN_YEAR = "chunk_year";
    public static final String SOLR_COLUMN_MONTH = "chunk_month";
    public static final String SOLR_COLUMN_START = "chunk_start";
    public static final String SOLR_COLUMN_END = "chunk_end";
    public static final String SOLR_COLUMN_METRIC_KEY = "metric_key";
    public static final String SOLR_COLUMN_VALUE = "chunk_value";
    public static final String SOLR_COLUMN_COUNT = "chunk_count";
    public static final String SOLR_COLUMN_SUM = "chunk_sum";
    public static final String SOLR_COLUMN_AVG = "chunk_avg";
    public static final String SOLR_COLUMN_STD_DEV = "chunk_std_dev";
    public static final String SOLR_COLUMN_MIN = "chunk_min";
    public static final String SOLR_COLUMN_MAX = "chunk_max";
    public static final String SOLR_COLUMN_FIRST = "chunk_first";
    public static final String SOLR_COLUMN_LAST = "chunk_last";
    public static final String SOLR_COLUMN_SAX = "chunk_sax";
    public static final String SOLR_COLUMN_ORIGIN = "chunk_origin";
    public static final String SOLR_COLUMN_TREND = "chunk_trend";
    public static final String SOLR_COLUMN_OUTLIER = "chunk_outlier";
    public static final String SOLR_COLUMN_VERSION = "version";

    public static final String SOLR_COLUMN_QUALITY_MIN = "chunk_quality_min";
    public static final String SOLR_COLUMN_QUALITY_MAX = "chunk_quality_max";
    public static final String SOLR_COLUMN_QUALITY_FIRST = "chunk_quality_first";
    public static final String SOLR_COLUMN_QUALITY_SUM = "chunk_quality_sum";
    public static final String SOLR_COLUMN_QUALITY_AVG = "chunk_quality_avg";

    private static BiMap<String, String> fieldsToColumnsMap = createMap();
    public static final List<String> SOLR_COLUMNS = new ArrayList<>(fieldsToColumnsMap.keySet());
    public static final List<String> FIELDS = new ArrayList<>(fieldsToColumnsMap.values());

    public static String getFieldFromColumn(String column) { return fieldsToColumnsMap.get(column);}
    public static String getColumnFromField(String field) { return fieldsToColumnsMap.inverse().get(field);}

    private static BiMap<String, String> createMap() {
        BiMap<String, String> biMap = HashBiMap.create();
        biMap.put(SOLR_COLUMN_ID, FIELD_ID);
        biMap.put(SOLR_COLUMN_VALUE, FIELD_VALUE);
        biMap.put(SOLR_COLUMN_NAME, FIELD_NAME);
        biMap.put(SOLR_COLUMN_DAY, FIELD_DAY);
        biMap.put(SOLR_COLUMN_START, FIELD_START);
        biMap.put(SOLR_COLUMN_END, FIELD_END);
        biMap.put(SOLR_COLUMN_YEAR, FIELD_YEAR);
        biMap.put(SOLR_COLUMN_MONTH, FIELD_MONTH);
        biMap.put(SOLR_COLUMN_COUNT, FIELD_COUNT);
        biMap.put(SOLR_COLUMN_AVG, FIELD_AVG);
        biMap.put(SOLR_COLUMN_SUM, FIELD_SUM);
        biMap.put(SOLR_COLUMN_STD_DEV, FIELD_STD_DEV);
        biMap.put(SOLR_COLUMN_MIN, FIELD_MIN);
        biMap.put(SOLR_COLUMN_MAX, FIELD_MAX);
        biMap.put(SOLR_COLUMN_FIRST, FIELD_FIRST);
        biMap.put(SOLR_COLUMN_LAST, FIELD_LAST);
        biMap.put(SOLR_COLUMN_SAX, FIELD_SAX);
        biMap.put(SOLR_COLUMN_ORIGIN, FIELD_ORIGIN);
        biMap.put(SOLR_COLUMN_QUALITY_MIN, FIELD_QUALITY_MIN);
        biMap.put(SOLR_COLUMN_QUALITY_MAX, FIELD_QUALITY_MAX);
        biMap.put(SOLR_COLUMN_QUALITY_FIRST, FIELD_QUALITY_FIRST);
        biMap.put(SOLR_COLUMN_QUALITY_SUM, FIELD_QUALITY_SUM);
        biMap.put(SOLR_COLUMN_QUALITY_AVG, FIELD_QUALITY_AVG);
        biMap.put(SOLR_COLUMN_TREND, FIELD_TREND);
        biMap.put(SOLR_COLUMN_OUTLIER, FIELD_OUTLIER);
        biMap.put(SOLR_COLUMN_VERSION, FIELD_VERSION);
        biMap.put(SOLR_COLUMN_METRIC_KEY, FIELD_METRIC_KEY);
        return biMap;
    }
}
