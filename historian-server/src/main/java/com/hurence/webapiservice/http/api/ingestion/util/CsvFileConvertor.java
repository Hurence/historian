package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.FileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.*;

public class CsvFileConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvFileConvertor.class);

    public CsvFilesConvertorConf csvFilesConvertorConf;
    public FileUpload file;
    public JsonArray fileInArray;
    public FileReport fileReport;

    CsvFileConvertor (MultiMap multiMap, FileUpload file) {
        this.csvFilesConvertorConf = new CsvFilesConvertorConf(multiMap);
        this.file = file;
        this.fileReport = new FileReport();
    }

    public CsvFilesConvertorConf getCsvFilesConvertorConf() {
        return csvFilesConvertorConf;
    }

    public FileUpload getFile() {
        return file;
    }

    public JsonArray getFileInArray() {
        return fileInArray;
    }

    public FileReport getFileReport() {
        return fileReport;
    }

}