package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.FileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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