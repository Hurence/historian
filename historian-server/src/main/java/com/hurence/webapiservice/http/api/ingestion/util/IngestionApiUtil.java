package com.hurence.webapiservice.http.api.ingestion.util;

import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class IngestionApiUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiUtil.class);

    /**
     * @param allFilesReport        AllFilesReport
     * @param multiMap              MultiMap
     *
     * construct the final response to return, which should be grouped by.
     *
     * @return JsonObject : the response to return
     */
    public static JsonObject constructFinalResponseCsv(AllFilesReport allFilesReport,
                                                       MultiMap multiMap) {
        //TODO
        return null;
    }

    public static JsonObject constructFinalResponseJson(JsonObject response, ImportRequestParser.CorrectPointsAndErrorMessages responseAndErrorHolder) {
        StringBuilder message = new StringBuilder();
        message.append("Injected ").append(response.getInteger(RESPONSE_TOTAL_ADDED_POINTS)).append(" points of ")
                .append(response.getInteger(RESPONSE_TOTAL_ADDED_CHUNKS))
                .append(" metrics in ").append(response.getInteger(RESPONSE_TOTAL_ADDED_CHUNKS))
                .append(" chunks");
        JsonObject finalResponse = new JsonObject();
        if (!responseAndErrorHolder.errorMessages.isEmpty()) {
            message.append(extractFinalErrorMessage(responseAndErrorHolder).toString());
            finalResponse.put("status", "Done but got some errors").put("message", message.toString());
        }else
            finalResponse.put("status", "OK").put("message", message.toString());
        return finalResponse;
    }
    public static StringBuilder extractFinalErrorMessage(ImportRequestParser.CorrectPointsAndErrorMessages responseAndErrorHolder) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(". ").append(responseAndErrorHolder.errorMessages.get(0));
        if (responseAndErrorHolder.errorMessages.size() > 1)
            for (int i = 1; i < responseAndErrorHolder.errorMessages.size()-1; i++) {
                errorMessage.append("\n").append(responseAndErrorHolder.errorMessages.get(i));
            }
        return errorMessage;
    }

}
