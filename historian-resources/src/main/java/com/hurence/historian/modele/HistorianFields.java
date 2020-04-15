package com.hurence.historian.modele;

import com.hurence.logisland.record.FieldDictionary;
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
public class HistorianFields {
    private HistorianFields() {}
    //Request fields
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
    public static String RESPONSE_METRIC_NAME_FIELD = "name";
    public static String RESPONSE_TAG_NAME_FIELD = "tagname";
    public static String RESPONSE_CHUNK_ID_FIELD = "id";
    public static String RESPONSE_CHUNK_VERSION_FIELD = "_version_";
    public static String RESPONSE_CHUNK_VALUE_FIELD = TimeSeriesRecord.CHUNK_VALUE;
    public static String RESPONSE_CHUNK_MAX_FIELD = TimeSeriesRecord.CHUNK_MAX;
    public static String RESPONSE_CHUNK_MIN_FIELD = TimeSeriesRecord.CHUNK_MIN;
    public static String RESPONSE_CHUNK_START_FIELD = TimeSeriesRecord.CHUNK_START;
    public static String RESPONSE_CHUNK_END_FIELD = TimeSeriesRecord.CHUNK_END;
    public static String RESPONSE_CHUNK_FIRST_VALUE_FIELD = TimeSeriesRecord.CHUNK_FIRST_VALUE;
    public static String RESPONSE_CHUNK_AVG_FIELD = TimeSeriesRecord.CHUNK_AVG;
    public static String RESPONSE_CHUNK_SIZE_FIELD = TimeSeriesRecord.CHUNK_SIZE;
    public static String RESPONSE_CHUNK_SUM_FIELD = TimeSeriesRecord.CHUNK_SUM;
    public static String RESPONSE_CHUNK_SAX_FIELD = TimeSeriesRecord.CHUNK_SAX;
    public static String RESPONSE_CHUNK_WINDOW_MS_FIELD = TimeSeriesRecord.CHUNK_WINDOW_MS;
    public static String RESPONSE_CHUNK_TREND_FIELD = TimeSeriesRecord.CHUNK_TREND;
    public static String RESPONSE_CHUNK_SIZE_BYTES_FIELD = TimeSeriesRecord.CHUNK_SIZE_BYTES;
    public static String TIME_END_REQUEST_FIELD = "timeEnd";

    //schema historian
    public static String CHUNK_YEAR = "year";
    public static String CHUNK_MONTH = "month";
    public static String CHUNK_DAY = "day";
    public static String CHUNK_ORIGIN = "chunk_origin";
}


