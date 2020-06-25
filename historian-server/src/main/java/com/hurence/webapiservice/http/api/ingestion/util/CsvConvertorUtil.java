package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.*;

public class CsvConvertorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvConvertorUtil.class);


    public static void parseFiles(List<CsvFileConvertor> csvFileConvertors, AllFilesReport allFilesReport) {
        for (CsvFileConvertor convertor : csvFileConvertors) {
            LOGGER.info("uploaded currentFileUpload : {} of size : {}", convertor.file.fileName(), convertor.file.size());
            LOGGER.info("contentType currentFileUpload : {} of contentTransferEncoding : {}", convertor.file.contentType(), convertor.file.contentTransferEncoding());
            LOGGER.info("uploaded uploadedFileName : {} ", convertor.file.uploadedFileName());
            LOGGER.info("uploaded charSet : {} ", convertor.file.charSet());
            try {
                ConvertCsvFileToJson(convertor);
            } catch (NoSuchElementException e) {
                String errorMessage = "The csv mappings don't match the mappings in the attributes. this file will be skipped";
                JsonObject errorObject = new JsonObject().put(FILE, convertor.file.fileName()).put(CAUSE, errorMessage);
                allFilesReport.namesOfTooBigFiles.add(errorObject);
                continue;
            } catch (IOException e) {
                String errorMessage = "The csv contains " + e.getMessage() + " lines which is more than the max number of line of "+MAX_LINES_FOR_CSV_FILE;
                JsonObject errorObject = new JsonObject().put(FILE, convertor.file.fileName()).put(CAUSE, errorMessage);
                allFilesReport.namesOfTooBigFiles.add(errorObject);
                continue;
            }

            parseCsvImportRequest(convertor.fileInArray, convertor.csvFilesConvertorConf, convertor.fileReport);
        }
    }


}
