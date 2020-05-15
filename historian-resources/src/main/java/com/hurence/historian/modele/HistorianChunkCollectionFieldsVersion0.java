package com.hurence.historian.modele;


import com.hurence.logisland.record.TimeseriesRecord;

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
    public static String CHUNK_START = TimeseriesRecord.CHUNK_START;
    public static String CHUNK_MAX = TimeseriesRecord.CHUNK_MAX;
    public static String CHUNK_MIN = TimeseriesRecord.CHUNK_MIN;
    public static String CHUNK_END = TimeseriesRecord.CHUNK_END;
    public static String CHUNK_AVG = TimeseriesRecord.CHUNK_AVG;
    public static String CHUNK_SIZE = TimeseriesRecord.CHUNK_SIZE;
    public static String CHUNK_SIZE_BYTES = TimeseriesRecord.CHUNK_SIZE_BYTES;
    public static String CHUNK_SAX = TimeseriesRecord.CHUNK_SAX;
    public static String CHUNK_TREND = TimeseriesRecord.CHUNK_TREND;
    public static String CHUNK_ORIGIN = TimeseriesRecord.CHUNK_ORIGIN;
    public static String CHUNK_OUTLIER = TimeseriesRecord.CHUNK_OUTLIER;
    public static String CHUNK_FIRST = TimeseriesRecord.CHUNK_FIRST_VALUE;
    public static String CHUNK_LAST = "chunk_last";
    public static String CHUNK_SUM = TimeseriesRecord.CHUNK_SUM;
    public static String CHUNK_YEAR = "chunk_year";
    public static String CHUNK_MONTH = "chunk_month";
    public static String CHUNK_DAY = "chunk_day";
    public static String CHUNK_HOUR = "chunk_hour";
    public static String CHUNK_STDDEV = "chunk_stddev";
}


