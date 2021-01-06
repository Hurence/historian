package com.hurence.historian.model;


/**
 * Static class to put field names used by HistorianService.
 */
/*
 Does not move those fields inside HistorianService for any reason !
 Indeed it is hard to refactor HistorianService as there is many other generated classes from HistorianService.
 THe source code generated from HistorianService copy paste static variables... So When you refactor them it is not
 taking in account by your code referencing auto generated source code.
 */
public class HistorianReportCollectionFieldsVersion0 {
    private HistorianReportCollectionFieldsVersion0() {}

    public static String TYPE = "type";
    public static String ID = "id";
    public static String START = "start";
    public static String END = "end";
    public static String JOB_DURATION_IN_MILLI = "job_duration_in_milli";
    public static String STATUS = "status";
    public static String NUMBER_OF_CHUNKS_IN_INPUT = "number_of_chunks_in_input";
    public static String NUMBER_OF_CHUNKS_IN_OUTPUT = "number_of_chunks_in_output";
    public static String TOTAL_METRICS_RECHUNKED = "total_metrics_rechunked";
    public static String JOB_CONF = "job_conf";
    public static String ERROR = "error";
    public static String EXCEPTION_MSG = "exception_msg";
}


