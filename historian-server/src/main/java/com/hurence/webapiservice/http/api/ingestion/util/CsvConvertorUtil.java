package com.hurence.webapiservice.http.api.ingestion.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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
    public static void parseFiles(List<CsvFileConvertor> csvFileConvertors, AllFilesReport allFilesReport) {
        for (CsvFileConvertor convertor : csvFileConvertors) {
            try {
                ConvertCsvFileToJson(convertor);
            } catch (Exception e) {
                //TODO here skip the csv file if it is too big or it's mapping don't match the mapping in the attributes
                // here i use allFilesReport to save that a certin file is too big or it's mapping don't match.
            }
            parseCsvImportRequest(convertor.fileInArray, convertor.csvFilesConvertorConf, convertor.fileReport);
        }
    }

    /**
     * @param convertor        CsvFileConvertor
     *
     * convert the csv file to json , and putting the json in convertor.fileInArray
     * @return void
     */
    public static void ConvertCsvFileToJson(CsvFileConvertor convertor) throws IOException {
        //TODO
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
        //TODO
    }
}
