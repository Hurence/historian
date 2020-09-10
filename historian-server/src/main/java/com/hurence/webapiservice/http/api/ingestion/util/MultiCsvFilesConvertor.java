package com.hurence.webapiservice.http.api.ingestion.util;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.FileUpload;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class MultiCsvFilesConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiCsvFilesConvertor.class);

    public MultiMap multiMap;
    Set<FileUpload> uploads;
    List<CsvFileConvertor> csvFileConvertors;
    public CorrectPointsAndFailedPointsOfAllFiles correctPointsAndFailedPointsOfAllFiles;

    public static class CorrectPointsAndFailedPointsOfAllFiles {

        public JsonArray correctPoints;
        public JsonArray filesThatFailedToBeImported;
        public LinkedHashMap<LinkedHashMap, Integer> numberOfFailedPointsPerMetric;

        public CorrectPointsAndFailedPointsOfAllFiles() {
            this.correctPoints = new JsonArray();
            this.filesThatFailedToBeImported = new JsonArray();
            this.numberOfFailedPointsPerMetric = new LinkedHashMap<>();
        }
    }

    public MultiCsvFilesConvertor(RoutingContext context) {
        this.multiMap = context.request().formAttributes();
        this.uploads = context.fileUploads();
        this.csvFileConvertors = new ArrayList<>();
        this.correctPointsAndFailedPointsOfAllFiles = new CorrectPointsAndFailedPointsOfAllFiles();
    }

    public void parseFiles() {
        for (CsvFileConvertor convertor : csvFileConvertors) {
            LOGGER.info("uploaded currentFileUpload : {} of size : {}", convertor.file.fileName(), convertor.file.size());
            LOGGER.info("contentType currentFileUpload : {} of contentTransferEncoding : {}", convertor.file.contentType(), convertor.file.contentTransferEncoding());
            LOGGER.info("uploaded uploadedFileName : {} ", convertor.file.uploadedFileName());
            LOGGER.info("uploaded charSet : {} ", convertor.file.charSet());
            try {
                convertor.ConvertCsvFileToJson();
            } catch (NoSuchElementException e) {
                String errorMessage = "The csv mappings don't match the mappings in the attributes. this file will be skipped";
                JsonObject errorObject = new JsonObject().put(HistorianServiceFields.FILE, convertor.file.fileName()).put(HistorianServiceFields.CAUSE, errorMessage);
                correctPointsAndFailedPointsOfAllFiles.filesThatFailedToBeImported.add(errorObject);
                continue;
            } catch (IOException e) {
                String errorMessage = "The csv contains " + e.getMessage() + " lines which is more than the max number of line of "+HistorianServiceFields.MAX_LINES_FOR_CSV_FILE;
                JsonObject errorObject = new JsonObject().put(HistorianServiceFields.FILE, convertor.file.fileName()).put(HistorianServiceFields.CAUSE, errorMessage);
                correctPointsAndFailedPointsOfAllFiles.filesThatFailedToBeImported.add(errorObject);
                continue;
            }
            try {
                convertor.correctPointsAndFailedPoints = new ImportRequestParser().parseCsvImportRequest(convertor.fileInArray, multiMap);
            } catch (IllegalArgumentException e) {
                JsonObject errorObject = new JsonObject().put(HistorianServiceFields.FILE, convertor.file.fileName()).put(HistorianServiceFields.CAUSE, e.getMessage());
                correctPointsAndFailedPointsOfAllFiles.filesThatFailedToBeImported.add(errorObject);
            }
        }
    }

    public void fillingAllFilesConvertor() {
        csvFileConvertors.forEach(convertor -> {
            if (!convertor.correctPointsAndFailedPoints.correctPoints.isEmpty())
                correctPointsAndFailedPointsOfAllFiles.correctPoints.addAll(convertor.correctPointsAndFailedPoints.correctPoints);
            if (!convertor.correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.isEmpty()) {
                convertor.correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.forEach((i,j) -> {
                    if (correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric.containsKey(i)) {
                        int currentNumberOfFailedPoints = correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric.get(i);
                        correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric.put(i, Integer.parseInt(j.toString())+currentNumberOfFailedPoints);
                    } else
                        correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric.put(i, Integer.valueOf(j.toString()));
                });
            }
        });
    }

}