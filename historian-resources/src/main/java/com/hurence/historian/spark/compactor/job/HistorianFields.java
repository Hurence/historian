package com.hurence.historian.spark.compactor.job;

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
    //Request fields
    public static String FROM_REQUEST_FIELD = "from";
    public static String TO_REQUEST_FIELD = "to";
    public static String FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD = "fields";
    public static String TAGS_TO_FILTER_ON_REQUEST_FIELD = "tags";
    public static String METRIC_NAMES_AS_LIST_REQUEST_FIELD = "names";
    public static String MAX_TOTAL_CHUNKS_TO_RETRIEVE_REQUEST_FIELD = "total_max_chunks";
    public static String SAMPLING_ALGO_REQUEST_FIELD = "sampling_algo";
    public static String BUCKET_SIZE_REQUEST_FIELD = "bucket_size";
    public static String MAX_POINT_BY_METRIC_REQUEST_FIELD = "max_points_to_return_by_metric";
    public static String TIME_REQUEST_FIELD = "time";
    public static String TEXT_REQUEST_FIELD = "text";
    public static String TAGS_REQUEST_FIELD = "tags";
    public static String MAX_ANNOTATION_REQUEST_FIELD = "limit";
    public static String MATCH_ANY_REQUEST_FIELD = "matchAny";
    public static String TYPE_REQUEST_FIELD = "type";

    //Response fields
    public static String TOTAL_POINTS_RESPONSE_FIELD = "total_points";
    public static String TIMESERIES_RESPONSE_FIELD = "timeseries";

    public static String RESPONSE_CHUNKS = "chunks";
    public static String RESPONSE_METRICS = "metrics";
    public static String RESPONSE_ANNOTATIONS = "annotations";
    public static String RESPONSE_TOTAL_FOUND = "total_hit";
    public static String RESPONSE_TOTAL_METRICS_RETURNED = "total_returned";
    public static String RESPONSE_TOTAL_METRICS = "total";
    public static String RESPONSE_METRIC_NAME_FIELD = "name";
    public static String RESPONSE_TAG_NAME_FIELD = "tagname";
    public static String RESPONSE_CHUNK_ID_FIELD = "id";
    public static String RESPONSE_CHUNK_VERSION_FIELD = "_version_";
    public static String RESPONSE_CHUNK_VALUE_FIELD = TimeseriesRecord.CHUNK_VALUE;
    public static String RESPONSE_CHUNK_MAX_FIELD = TimeseriesRecord.CHUNK_MAX;
    public static String RESPONSE_CHUNK_MIN_FIELD = TimeseriesRecord.CHUNK_MIN;
    public static String RESPONSE_CHUNK_START_FIELD = TimeseriesRecord.CHUNK_START;
    public static String RESPONSE_CHUNK_END_FIELD = TimeseriesRecord.CHUNK_END;
    public static String RESPONSE_CHUNK_FIRST_VALUE_FIELD = TimeseriesRecord.CHUNK_FIRST_VALUE;
    public static String RESPONSE_CHUNK_AVG_FIELD = TimeseriesRecord.CHUNK_AVG;
    public static String RESPONSE_CHUNK_SIZE_FIELD = TimeseriesRecord.CHUNK_SIZE;
    public static String RESPONSE_CHUNK_SUM_FIELD = TimeseriesRecord.CHUNK_SUM;
    public static String RESPONSE_CHUNK_SAX_FIELD = TimeseriesRecord.CHUNK_SAX;
    public static String RESPONSE_CHUNK_WINDOW_MS_FIELD = TimeseriesRecord.CHUNK_WINDOW_MS;
    public static String RESPONSE_CHUNK_TREND_FIELD = TimeseriesRecord.CHUNK_TREND;
    public static String RESPONSE_CHUNK_SIZE_BYTES_FIELD = TimeseriesRecord.CHUNK_SIZE_BYTES;
    public static String TIME_END_REQUEST_FIELD = "timeEnd";

    //schema historian
    public static String CHUNK_YEAR = "year";
    public static String CHUNK_MONTH = "month";
    public static String CHUNK_DAY = "day";
    public static String CHUNK_ORIGIN = "chunk_origin";
}

