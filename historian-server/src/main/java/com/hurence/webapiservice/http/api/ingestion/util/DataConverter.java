package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.ingestion.util.TimeStampUnit.*;

public class DataConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataConverter.class);
    public CsvFilesConvertorConf csvFilesConvertorConf;



    public DataConverter (CsvFilesConvertorConf csvFilesConvertorConf) {
        this.csvFilesConvertorConf = csvFilesConvertorConf;
    }

    /**
     * @param LinesWithDateInfo        List<LineWithDateInfo>
     *
     * group by the LinesWithDateInfo into a json array
     *
     * @return JsonArray
     */
    public JsonArray toGroupedByMetricDataPoints(List<LineWithDateInfo> LinesWithDateInfo) {
        // TODO
        return null;
    }


}
