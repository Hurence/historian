package com.hurence.historian.model;


/**
 * These fields are the fields use by the historian service. It manipulates chunks as JsonObject
 * that should have those properties. So if solr schema is different response from solr should be converted so
 * that we obtain those fields (if they are needed)
 */
public class FieldNamesInsideHistorianService {
    private FieldNamesInsideHistorianService() {}

    public static String NAME = "name";
    public static String ID = "id";
    public static String COMPACTIONS_RUNNING = "compactions_running";
    public static String CHUNK_VALUE = "chunk_value";
    public static String CHUNK_START = "chunk_start";
    public static String CHUNK_MAX = "chunk_max";
    public static String CHUNK_MIN = "chunk_min";
    public static String CHUNK_END = "chunk_end";
    public static String CHUNK_AVG = "chunk_avg";
    public static String CHUNK_COUNT = "chunk_count";
    public static String CHUNK_SAX = "chunk_sax";
    public static String CHUNK_TREND = "chunk_trend";
    public static String CHUNK_ORIGIN = "chunk_origin";
    public static String CHUNK_OUTLIER = "chunk_outlier";
    public static String CHUNK_FIRST = "chunk_first";
    public static String CHUNK_LAST = "chunk_last";
    public static String CHUNK_SUM = "chunk_sum";
    public static String CHUNK_YEAR = "chunk_year";
    public static String CHUNK_MONTH = "chunk_month";
    public static String CHUNK_DAY = "chunk_day";
    public static String CHUNK_HOUR = "chunk_hour";
    public static String CHUNK_STDDEV = "chunk_std_dev";

    public static String CHUNK_QUALITY_SUM_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_SUM;
    public static String CHUNK_QUALITY_FIRST_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_FIRST;
    public static String CHUNK_QUALITY_MIN_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MIN;
    public static String CHUNK_QUALITY_MAX_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MAX;
    public static String CHUNK_QUALITY_AVG_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_AVG;


}


