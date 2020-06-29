package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.MAX_LINES_FOR_CSV_FILE;
import static com.hurence.webapiservice.http.api.ingestion.ImportRequestParser.parseCsvImportRequest;

public class CsvConvertorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvConvertorUtil.class);


    /**
     * @param csvFileConvertors        List<CsvFileConvertor>
     *
     * parse each csv file and convert it to json then add it to CsvFileConvertor.fileReport
     *
     * @return void
     */
    public static void parseFiles(List<CsvFileConvertor> csvFileConvertors, AllFilesReport allFilesReport, CsvFilesConvertorConf csvFilesConvertorConf) {
        for (CsvFileConvertor convertor : csvFileConvertors) {
            try {
                ConvertCsvFileToJson(convertor, csvFilesConvertorConf);
            } catch (Exception e) {
                //TODO here skip the csv file if it is too big or it's mapping don't match the mapping in the attributes
                // here i use allFilesReport to save that a certin file is too big or it's mapping don't match.
            }
            parseCsvImportRequest(convertor.fileInArray,csvFilesConvertorConf, convertor.fileReport);
        }
    }

    /**
     * @param convertor        CsvFileConvertor
     *
     * convert the csv file to json , and putting the json in convertor.fileInArray
     * @return void
     */
    public static void ConvertCsvFileToJson(CsvFileConvertor convertor, CsvFilesConvertorConf csvFilesConvertorConf) throws IOException {
        String fileName = convertor.file.uploadedFileName();
        MappingIterator<Map> rows = getMappingIteratorFromFile(fileName);
        List<LineWithDateInfo> linesWithDateInfo = addDateInfoToEachLine(rows, csvFilesConvertorConf);

        DataConverter dataConverter = new DataConverter(csvFilesConvertorConf);
        JsonArray groupedByMetricDataPoints = dataConverter.toGroupedByMetricDataPoints(linesWithDateInfo);
        if (groupedByMetricDataPoints.size() > MAX_LINES_FOR_CSV_FILE) {
            throw new IOException(String.valueOf(groupedByMetricDataPoints.size()));
        }
        convertor.fileInArray = groupedByMetricDataPoints;
    }

    /**
     * @param fileName        String
     *
     * use fasterxml.jackson.dataformat here to convert the csv file to a mapping iterator.
     *
     * @return MappingIterator<Map>
     */
    private static MappingIterator<Map> getMappingIteratorFromFile(String fileName) throws IOException {
        // TODO
        return  null;
    }

    /**
     * @param rows                   MappingIterator<Map>
     * @param csvFilesConvertorConf  CsvFilesConvertorConf
     *
     * for each row construct a LineWithDateInfo object, if there is no date use null.
     *
     * @return List<LineWithDateInfo>
     */
    private static List<LineWithDateInfo> addDateInfoToEachLine(MappingIterator<Map> rows, CsvFilesConvertorConf csvFilesConvertorConf) throws IOException {
        // TODO
        return  null;
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
