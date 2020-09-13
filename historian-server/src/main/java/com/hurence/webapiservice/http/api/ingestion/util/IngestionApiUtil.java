package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.ingestion.ImportRequestParser.parseCsvImportRequest;
import static com.hurence.webapiservice.http.api.ingestion.util.CsvConvertorUtil.ConvertCsvFileToJson;

public class IngestionApiUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiUtil.class);

    /**
     * @param csvFileConvertors        List<CsvFileConvertor>
     *
     * parse each csv file : for each file :
     *                       1- convert it to json then add it to CsvFileConvertor.fileInArray.
     *                       2- parse the fileInArray to only get the correct points and store them in CsvFileConvertor.fileReport.correctPoints.
     *                       3- fill the fileReport.numberOfFailedPointsPerMetric.
     *
     * @return void
     */
    public static void parseFiles(List<CsvFileConvertor> csvFileConvertors, AllFilesReport allFilesReport,
                                  CsvFilesConvertorConf csvFilesConvertorConf) {
        for (CsvFileConvertor convertor : csvFileConvertors) {
            try {
                // convert the file to json
                ConvertCsvFileToJson(convertor, csvFilesConvertorConf);
            } catch (NoSuchElementException e) {
                // the file will be skipped because it's mapping aren't correct.
                String errorMessage = "The csv mappings don't match the mappings in the attributes. this file will be skipped";
                fillingTheNamesOfFailedFiles(errorMessage, convertor, allFilesReport);
                continue;
            } catch (IOException e) {
                // the file will be skipped because it is too big (> 5000 lines)
                String errorMessage = "The csv contains " + e.getMessage() + " lines which is more than the max number of line of "+ MAX_LINES_FOR_CSV_FILE;
                fillingTheNamesOfFailedFiles(errorMessage, convertor, allFilesReport);
                continue;
            }
            // parse the jsonArray fileInArray
            parseCsvImportRequest(convertor.fileInArray,csvFilesConvertorConf, convertor.fileReport);
        }
    }

    private static void fillingTheNamesOfFailedFiles(String errorMessage, CsvFileConvertor convertor,
                                                     AllFilesReport allFilesReport) {
        JsonObject errorObject = new JsonObject().put(FILE, convertor.file.fileName()).put(CAUSE, errorMessage);
        allFilesReport.failedFiles.add(errorObject);
    }

    /**
     * @param csvFileConvertors        List<CsvFileConvertor>
     * @param allFilesReport           AllFilesReport
     *
     * filling the allFilesReport
     *
     * @return void
     */
    public static void fillingAllFilesReport(List<CsvFileConvertor> csvFileConvertors, AllFilesReport allFilesReport) {
        csvFileConvertors.forEach(convertor -> {
            if (!convertor.fileReport.correctPoints.isEmpty())
                allFilesReport.correctPoints.addAll(convertor.fileReport.correctPoints);
            if (!convertor.fileReport.numberOfFailedPointsPerMetric.isEmpty()) {
                convertor.fileReport.numberOfFailedPointsPerMetric.forEach((i,j) -> {
                    if (allFilesReport.numberOfFailedPointsPerMetric.containsKey(i)) {
                        int currentNumberOfFailedPoints = allFilesReport.numberOfFailedPointsPerMetric.get(i);
                        allFilesReport.numberOfFailedPointsPerMetric.put(i, Integer.parseInt(j.toString())+currentNumberOfFailedPoints);
                    } else
                        allFilesReport.numberOfFailedPointsPerMetric.put(i, Integer.valueOf(j.toString()));
                });
            }
        });
    }

}
