package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionApiUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiUtil.class);

    /**
     * @param allFilesReport        AllFilesReport
     * @param csvFilesConvertorConf    CsvFilesConvertorConf
     *
     * construct the final response to return, which should be grouped by.
     *
     * @return JsonObject : the response to return
     */
    public static JsonObject constructFinalResponseCsv(AllFilesReport allFilesReport,
                                                       CsvFilesConvertorConf csvFilesConvertorConf) {
        //TODO
        return null;
    }
}
