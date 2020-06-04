package com.hurence.historian.modele;


import com.hurence.logisland.record.TimeSeriesRecord;

/**
 * Static class to put field names used by HistorianService.
 */
/*
 Does not move those fields inside HistorianService for any reason !
 Indeed it is hard to refactor HistorianService as there is many other generated classes from HistorianService.
 THe source code generated from HistorianService copy paste static variables... So When you refactor them it is not
 taking in account by your code referencing auto generated source code.
 */
public class HistorianChunkCollectionFieldsVersionEVOA0 {
    private HistorianChunkCollectionFieldsVersionEVOA0() {}

    public static String NAME = "name";
    public static String ID = "id";
    public static String COMPACTIONS_RUNNING = "compactions_running";
    public static String CHUNK_VALUE = "chunk_value";
    public static String CHUNK_START = TimeSeriesRecord.CHUNK_START;
    public static String CHUNK_MAX = TimeSeriesRecord.CHUNK_MAX;
    public static String CHUNK_MIN = TimeSeriesRecord.CHUNK_MIN;
    public static String CHUNK_END = TimeSeriesRecord.CHUNK_END;
    public static String CHUNK_AVG = TimeSeriesRecord.CHUNK_AVG;
    public static String CHUNK_COUNT = TimeSeriesRecord.CHUNK_COUNT;
    public static String CHUNK_SIZE = TimeSeriesRecord.CHUNK_SIZE;
    public static String CHUNK_SIZE_BYTES = TimeSeriesRecord.CHUNK_SIZE_BYTES;
    public static String CHUNK_WINDOW_MS = TimeSeriesRecord.CHUNK_WINDOW_MS;
    public static String CODE_INSTALL = "code_install";
    public static String NUMERIC_TYPE = "numeric_type";
    public static String SENSOR = "sensor";
    public static String VALUE = "value";
    public static String TIMESTAMP = "timestamp";
    public static String QUALITY = "quality";
    public static String DELETE = "delete";
    public static String FILE_PATH = "file_path";
    public static String CHUNK_SAX = TimeSeriesRecord.CHUNK_SAX;
    public static String CHUNK_TREND = TimeSeriesRecord.CHUNK_TREND;
    public static String CHUNK_ORIGIN = TimeSeriesRecord.CHUNK_ORIGIN;
    public static String CHUNK_OUTLIER = TimeSeriesRecord.CHUNK_OUTLIER;
    public static String CHUNK_FIRST = TimeSeriesRecord.CHUNK_FIRST_VALUE;
    public static String CHUNK_LAST = "chunk_last";
    public static String CHUNK_SUM = TimeSeriesRecord.CHUNK_SUM;
    public static String CHUNK_YEAR = "year";
    public static String CHUNK_MONTH = "month";
    public static String CHUNK_DAY = "day";
    public static String CHUNK_HOUR = "hour";
    public static String CHUNK_WEEK = "week";
    public static String CHUNK_STDDEV = "chunk_stddev";
    public static String TAGS = "tagname";
}


