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
public class HistorianFields {
    private HistorianFields() {}

    public static String DATAPOINTS_RESPONSE_FIELD = "datapoints";
    public static String ERRORS_RESPONSE_FIELD = "error";
    public static String TARGET_RESPONSE_FIELD = "target";
    public static String POINTS_REQUEST_FIELD = "points";
    public static String FROM = "from";
    public static String TO = "to";
    public static String FIELDS = "fields";
    public static String NAMES = "names";
    public static String MAX_TOTAL_CHUNKS_TO_RETRIEVE = "total_max_chunks";
    public static String SAMPLING_ALGO = "sampling_algo";
    public static String BUCKET_SIZE = "bucket_size";
    public static String MAX_POINT_BY_METRIC = "max_points_to_return_by_metric";
    public static String TIME = "time";
    public static String TEXT = "text";
    public static String TAGS = "tags";
    public static String LIMIT = "limit";
    public static String MATCH_ANY = "matchAny";
    public static String TYPE = "type";
    public static String METRIC = "metric";

    //Response fields
    public static String TOTAL_POINTS = "total_points";
    public static String TIMESERIES = "timeseries";

    public static String CHUNKS = "chunks";
    public static String METRICS = "metrics";
    public static String ANNOTATIONS = "annotations";
    public static String TOTAL = "total";
    public static String TOTAL_HIT = "total_hit";
    public static String NAME = "name";
    public static String TIME_END_REQUEST_FIELD = "timeEnd";
    public static String RESPONSE_TOTAL_ADDED_POINTS = "total_added_points";
    public static String RESPONSE_TOTAL_ADDED_CHUNKS = "total_added_chunks";
    public static String RESPONSE_TAG_NAME_FIELD = "tagname";

    public static String RESPONSE_CHUNK_ID_FIELD = HistorianChunkCollectionFieldsVersion0.ID;
    public static String RESPONSE_CHUNK_VERSION_FIELD = "_version_";
    public static String RESPONSE_CHUNK_VALUE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE;
    public static String RESPONSE_CHUNK_MAX_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_MAX;
    public static String RESPONSE_CHUNK_MIN_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_MIN;
    public static String RESPONSE_CHUNK_START_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_START;
    public static String RESPONSE_CHUNK_END_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_END;
    public static String RESPONSE_CHUNK_FIRST_VALUE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST;
    public static String RESPONSE_CHUNK_AVG_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_AVG;
    public static String RESPONSE_CHUNK_SIZE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SIZE;
    public static String RESPONSE_CHUNK_SUM_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SUM;
    public static String RESPONSE_CHUNK_SAX_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SAX;
    public static String RESPONSE_CHUNK_WINDOW_MS_FIELD = TimeseriesRecord.CHUNK_WINDOW_MS;
    public static String RESPONSE_CHUNK_TREND_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_TREND;
    public static String RESPONSE_CHUNK_SIZE_BYTES_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SIZE_BYTES;

    //schema historian
    public static String CHUNK_YEAR = HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR;
    public static String CHUNK_MONTH = HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH;
    public static String CHUNK_DAY = HistorianChunkCollectionFieldsVersion0.CHUNK_DAY;
    public static String CHUNK_ORIGIN = HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN;
}


