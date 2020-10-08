package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.hurence.historian.model.HistorianServiceFields.CUSTOM_NAME;
import static com.hurence.webapiservice.http.api.ingestion.util.DataConverter.toNumber;

public class CsvConvertorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvConvertorUtil.class);

    /**
     * @param convertor        CsvFileConvertor
     *
     * convert the csv file to json , and putting the json in convertor.fileInArray
     *
     * @return void
     */
    public static void ConvertCsvFileToJson(CsvFileConvertor convertor, CsvFilesConvertorConf csvFilesConvertorConf) throws IOException {
        String fileName = convertor.file.uploadedFileName();
        MappingIterator<Map> rows = getMappingIteratorFromFile(fileName);
        List<LineWithDateInfo> linesWithDateInfo = ReturnLineWithDateInfoList(rows, csvFilesConvertorConf);
        if (linesWithDateInfo.size() > csvFilesConvertorConf.getMaxNumberOfLignes()) {
            throw new IOException(String.valueOf(linesWithDateInfo.size()));
        }
        DataConverter dataConverter = new DataConverter(csvFilesConvertorConf);
        convertor.fileInArray = dataConverter.toGroupedByMetricDataPoints(linesWithDateInfo);
    }

    /**
     * @param fileName        String
     *
     * use fasterxml.jackson.dataformat here to convert the csv file to a mapping iterator.
     *
     * @return MappingIterator<Map>
     */
    private static MappingIterator<Map> getMappingIteratorFromFile(String fileName) throws IOException {
        File uploadedFile = new File(fileName);
        CsvMapper csvMapper = new CsvMapper();
        return  csvMapper
                .readerWithSchemaFor(Map.class)
                .with(CsvSchema.emptySchema().withHeader())
                .readValues(uploadedFile);
    }

    /**
     * @param rows                   MappingIterator<Map>
     * @param csvFilesConvertorConf  CsvFilesConvertorConf
     *
     * return a list of  LineWithDateInfo
     * for each row construct a LineWithDateInfo object, if there is no date use null.
     *
     * @return List<LineWithDateInfo>
     */
    private static List<LineWithDateInfo> ReturnLineWithDateInfoList(MappingIterator<Map> rows,
                                                                     CsvFilesConvertorConf csvFilesConvertorConf) throws IOException {
        List<Map> listFromRows = rows.readAll();

        // here just checking the attributes :
        verifyAttributes(listFromRows, csvFilesConvertorConf);

        List<LineWithDateInfo> linesWithDateInfos = new LinkedList<>();
        listFromRows.forEach(i -> {
            try {
                // if date is generated from timestamp successfully
                String date = generateDateFromTime(i.get(csvFilesConvertorConf.getTimestamp()), csvFilesConvertorConf);
                linesWithDateInfos.add(new LineWithDateInfo(i,date));
            }catch (Exception e) {
                // otherwise put null as date
                LOGGER.debug("error in parsing date", e);
                linesWithDateInfos.add(new LineWithDateInfo(i,null));
            }
        });
        return  linesWithDateInfos;
    }

    /**
     * @param listFromRows           List<Map>
     * @param csvFilesConvertorConf  CsvFilesConvertorConf
     *
     * verify if this list from the csv file have the same column as the attributes in input :
     *                               name, timestamp, value, and all the tags.
     *
     */
    private static void verifyAttributes(List<Map> listFromRows, CsvFilesConvertorConf csvFilesConvertorConf) {
        listFromRows.forEach(i ->  {
            Set<String> keySet = i.keySet();
            if ( (csvFilesConvertorConf.getName() != null) && (!keySet.contains(csvFilesConvertorConf.getName())))
                throw new NoSuchElementException("error in the attributes : the attribute name :"+ csvFilesConvertorConf.getName()
                                                + " don't exist in the csv, you acn use the attribute " +CUSTOM_NAME);
            if (!keySet.contains(csvFilesConvertorConf.getTimestamp()))
                throw new NoSuchElementException("error in the attributes");
            if (!keySet.contains(csvFilesConvertorConf.getValue()))
                throw new NoSuchElementException("error in the attributes");
        });
    }


    /**
     * @param timestamp              Object
     * @param csvFilesConvertorConf  CsvFilesConvertorConf
     *
     * generate the date in format yyyy-MM-dd from timestamp.
     *
     * @return List<LineWithDateInfo>
     */
    private static String generateDateFromTime (Object timestamp, CsvFilesConvertorConf csvFilesConvertorConf) {
        long date = (long) toNumber(timestamp, csvFilesConvertorConf);
        Date d = new Date(date);
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        return f.format(d);
    }
}
