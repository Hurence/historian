package com.hurence.historian.modele;


/**
 * those fields are the fields used by historian internally. It always correspond to the last version of the historian.
 * All tools should use
 */
public class HistorianFields {
    private HistorianFields() {}

    public static String DATAPOINTS = "datapoints";
    public static String POINTS = "points";
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
    public static String AGGREGATION = "aggregation";
    public static String QUALITY_VALUE = "quality_value";
    public static String QUALITY_AGG  = "quality_agg";
    public static String QUALITY_RETURN  = "quality_return";

    //Response fields
    public static String TOTAL_POINTS = "total_points";
    public static String TIMESERIES = "timeseries";
    public static String CHUNKS = "chunks";
    public static String METRICS = "metrics";
    public static String ANNOTATIONS = "annotations";
    public static String TOTAL = "total";
    public static String TOTAL_HIT = "total_hit";
    public static String NAME = "name";

    public static final String FIELD = "field";
    public static final String QUERY = "query";

    public static String TIME_END_REQUEST_FIELD = "timeEnd";
    public static String TOTAL_ADDED_POINTS = "total_added_points";
    public static String TOTAL_ADDED_CHUNKS = "total_added_chunks";
    public static final String RESPONSE_VALUES = "values";

    public static String CHUNK_ID_FIELD = HistorianChunkCollectionFieldsVersion0.ID;
    public static String CHUNK_VERSION_FIELD = "_version_";
    public static String CHUNK_VALUE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE;
    public static String CHUNK_MAX_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_MAX;
    public static String CHUNK_MIN_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_MIN;
    public static String CHUNK_START_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_START;
    public static String CHUNK_END_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_END;
    public static String CHUNK_FIRST_VALUE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST;
    public static String CHUNK_AVG_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_AVG;
    public static String CHUNK_COUNT_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT;
    public static String CHUNK_SUM_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SUM;
    public static String CHUNK_SAX_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SAX;
    public static String CHUNK_TREND_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_TREND;
    public static String CHUNK_QUALITY_AVG_FIELD = HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_AVG;
    public static String CHUNK_QUALITY_MIN_FIELD = HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MIN;
    public static String CHUNK_QUALITY_MAX_FIELD = HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MAX;
    public static String CHUNK_QUALITY_SUM_FIELD = HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_SUM;
    public static String CHUNK_QUALITY_FIRST_FIELD = HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_FIRST;

    //schema historian
    public static String CHUNK_YEAR = HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR;
    public static String CHUNK_MONTH = HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH;
    public static String CHUNK_DAY = HistorianChunkCollectionFieldsVersion0.CHUNK_DAY;
    public static String CHUNK_ORIGIN = HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN;

    /*
        Below this comment should be put fields that should be moved outside this class.
     */
    //Csv fields
    public static String ERRORS_RESPONSE_FIELD = "error";
    public static int MAX_LINES_FOR_CSV_FILE = 5000;
    public static String FILE = "file";
    public static String CAUSE = "cause";
    public static String GROUPED_BY = "groupedBy";
    public static String GROUPED_BY_IN_RESPONSE = "grouped_by";
    public static String REPORT = "report";
    public static String ERRORS = "errors";
    public static String CSV = "csv";
    public static String IMPORT_TYPE = "import_type";
    public static String DEFAULT_NAME_FIELD = "name";
    public static String CORRECT_POINTS = "correctPoints";
    //Mapping fields
    public static String MAPPING_TIMESTAMP = "mapping.timestamp";
    public static String MAPPING_NAME = "mapping.name";
    public static String MAPPING_VALUE = "mapping.value";
    public static String MAPPING_QUALITY = "mapping.quality";
    public static String MAPPING_TAGS = "mapping.tags";
    public static String FORMAT_DATE = "format_date";
    public static String GROUP_BY = "group_by";
    public static String TIMEZONE_DATE = "timezone_date";
}


