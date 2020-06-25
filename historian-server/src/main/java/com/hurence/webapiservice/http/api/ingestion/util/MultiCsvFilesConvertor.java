package com.hurence.webapiservice.http.api.ingestion.util;

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

import static com.hurence.historian.modele.HistorianFields.*;

public class MultiCsvFilesConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiCsvFilesConvertor.class);

    public Set<FileUpload> uploads;
    public List<CsvFileConvertor> csvFileConvertors;
    public AllFilesReport allFilesReport;

    public MultiCsvFilesConvertor(RoutingContext context) {
        this.uploads = context.fileUploads();
        this.allFilesReport = new AllFilesReport();
        fillingCsvFileConverotrs(context.request().formAttributes());
    }

    private void fillingCsvFileConverotrs(MultiMap multiMap) {
        uploads.forEach(file -> {
            CsvFileConvertor csvFileConvertor = new CsvFileConvertor(multiMap, file);
            csvFileConvertors.add(csvFileConvertor);
        });
    }


}