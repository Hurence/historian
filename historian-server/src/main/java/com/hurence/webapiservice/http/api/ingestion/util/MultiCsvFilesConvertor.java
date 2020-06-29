package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.FileUpload;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class MultiCsvFilesConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiCsvFilesConvertor.class);

    public Set<FileUpload> uploads;
    public List<CsvFileConvertor> csvFileConvertors;
    public AllFilesReport allFilesReport;

    public MultiCsvFilesConvertor(RoutingContext context) {
        this.uploads = context.fileUploads();
        this.allFilesReport = new AllFilesReport();
        fillingCsvFileConverotrs();
    }

    private void fillingCsvFileConverotrs() {
        uploads.forEach(file -> {
            CsvFileConvertor csvFileConvertor = new CsvFileConvertor(file);
            csvFileConvertors.add(csvFileConvertor);
        });
    }

}