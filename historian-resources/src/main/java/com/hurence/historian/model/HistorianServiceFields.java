package com.hurence.historian.model;

/**
 * This represent the fields used in input and ouput of methods in HistorianService.
 * This should be independant of other things like http requests or solr schema.
 *
 */
public class HistorianServiceFields {
    public static String TEXT = "text";
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
    public static String TAGS = "tags";
    public static String LIMIT = "limit";
    public static String MATCH_ANY = "matchAny";
    public static String TYPE = "type";
    public static String METRIC = "metric";
    public static String AGGREGATION = "aggregation";
    public static String QUALITY_VALUE = "quality_value";
    public static String QUALITY_AGG  = "quality_agg";
    public static String QUALITY_RETURN  = "quality_return";
    public static String USE_QUALITY  = "use_quality";
    public static String QUALITY_CHECK  = "is_quality_ok";
    public static String VALUE = "value";
    public static String DATE = "date";
    public static String ORIGIN = HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN;
    public static String START = "start";
    public static String END = "end";
    public static String DATA = "data";
    public static String STATUS = "status";
    public static String SUCCESS = "success";
    public static String TIMEOUT = "timeout";
    public static String STEP = "step";
    public static String MATCH = "match[]";

    //Response fields
    public static String TOTAL_POINTS = "total_points";
    public static String TIMESERIES = "timeseries";
    public static String CHUNKS = "chunks";
    public static String METRICS = "metrics";
    public static String ANNOTATIONS = "annotations";
    public static String TOTAL = "total";
    public static String TOTAL_HIT = "total_hit";
    public static String FIELD = "field";
    public static String QUERY = "query";
    public static String TIME_END_REQUEST_FIELD = "timeEnd";
    public static String TOTAL_ADDED_POINTS = "total_added_points";
    public static String TOTAL_ADDED_CHUNKS = "total_added_chunks";

    public static String CHUNK_ORIGIN = HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN;

    public static String RESPONSE_VALUES = "values";
    public static String NAME = HistorianChunkCollectionFieldsVersion0.NAME;
    /*
            Below this comment should be put fields that should be moved outside this class.
         */
    //Csv fields
    public static int MAX_LINES_FOR_CSV_FILE = 5000;
    public static String ERRORS_RESPONSE_FIELD = "error";
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
    public static String CUSTOM_NAME = "custom.name";
    public static String FORMAT_DATE = "format_date";
    public static String GROUP_BY = "group_by";
    public static String TIMEZONE_DATE = "timezone_date";
    public static String MAX_NUMBER_OF_LIGNES = "max_number_of_lignes";
}
