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
import static com.hurence.webapiservice.http.api.ingestion.util.DataConverter.DEFAULT_TIMESTAMP_COLUMN_MAPPING;

public class CsvFileConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvFileConvertor.class);

    MultiMap multiMap;
    FileUpload file;
    JsonArray fileInArray;
    CorrectPointsAndFailedPoints correctPointsAndFailedPoints;

    public static class CorrectPointsAndFailedPoints {

        public JsonArray correctPoints;
        public LinkedHashMap<LinkedHashMap, Integer> numberOfFailedPointsPerMetric;

        public CorrectPointsAndFailedPoints() {
            this.correctPoints = new JsonArray();
            this.numberOfFailedPointsPerMetric = new LinkedHashMap<>();
        }
    }

    CsvFileConvertor (MultiMap multiMap, FileUpload file) {
        this.multiMap = multiMap;
        this.file = file;
        this.correctPointsAndFailedPoints = new CorrectPointsAndFailedPoints();
    }

    public void ConvertCsvFileToJson() throws IOException {
        String fileName = file.uploadedFileName();
        File uploadedFile = new File(fileName);
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map> rows = csvMapper
                .readerWithSchemaFor(Map.class)
                .with(CsvSchema.emptySchema().withHeader())
                .readValues(uploadedFile);
        List<IngestionApiUtil.LineWithDateInfo> linesWithDateInfo = addDateInfoToEachLine(rows);

        DataConverter converter = new DataConverter(multiMap);
        JsonArray result = converter.toGroupedByMetricDataPoints(linesWithDateInfo);
        if (result.size() > MAX_LINES_FOR_CSV_FILE) {
            throw new IOException(String.valueOf(result.size()));
        }
        fileInArray = result;
    }

    private List<IngestionApiUtil.LineWithDateInfo> addDateInfoToEachLine(MappingIterator<Map> rows) throws IOException {
        List<Map> listFromRows = rows.readAll();
        verifyMultiMap(listFromRows, multiMap);
        List<IngestionApiUtil.LineWithDateInfo> linesWithDateInfos = new LinkedList<>();
        listFromRows.forEach(i -> {
            try {
                String date = generateDateFromTime(i, multiMap);
                linesWithDateInfos.add(new IngestionApiUtil.LineWithDateInfo(i,date));
            }catch (Exception e) {
                LOGGER.debug("error in parsing date", e);
                linesWithDateInfos.add(new IngestionApiUtil.LineWithDateInfo(i,null));
            }
        });
        return linesWithDateInfos;
    }

    private static void verifyMultiMap(List<Map> listFromRows, MultiMap multiMap) {
        listFromRows.forEach(i ->  {
            Set<String> keySet = i.keySet();
            if ((multiMap.get(MAPPING_NAME) != null) && (!keySet.contains(multiMap.get(MAPPING_NAME))))
                throw new NoSuchElementException("error in the attributes");
            if ((multiMap.get(MAPPING_TIMESTAMP) != null) && (!keySet.contains(multiMap.get(MAPPING_TIMESTAMP))))
                throw new NoSuchElementException("error in the attributes");
            if ((multiMap.get(MAPPING_VALUE) != null) && (!keySet.contains(multiMap.get(MAPPING_VALUE))))
                throw new NoSuchElementException("error in the attributes");
            if ((multiMap.get(MAPPING_QUALITY) != null) && (!keySet.contains(multiMap.get(MAPPING_QUALITY))))
                throw new NoSuchElementException("error in the attributes");
        });
    }

    private static String generateDateFromTime (Map map, MultiMap multiMap) {
        if (multiMap.get(MAPPING_TIMESTAMP) == null)
            multiMap.add(MAPPING_TIMESTAMP, DEFAULT_TIMESTAMP_COLUMN_MAPPING);
        Object date1 = map.get(multiMap.get(MAPPING_TIMESTAMP));
        long date = (long) DataConverter.toNumber(date1, multiMap);
        Date d = new Date(date);
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        return f.format(d);
    }

}