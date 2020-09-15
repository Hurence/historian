package com.hurence.historian.modele;


/**
 * Static class to put field names used by HistorianService.
 */
/*
 Does not move those fields inside HistorianService for any reason !
 Indeed it is hard to refactor HistorianService as there is many other generated classes from HistorianService.
 THe source code generated from HistorianService copy paste static variables... So When you refactor them it is not
 taking in account by your code referencing auto generated source code.
 */
public class HistorianChunkCollectionFieldsVersion0 {
    private HistorianChunkCollectionFieldsVersion0() {}

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
    public static String CHUNK_STDDEV = "chunk_stddev";
}


