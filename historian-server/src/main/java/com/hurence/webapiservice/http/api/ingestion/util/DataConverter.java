package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.ingestion.util.TimeStampUnit.*;

public class DataConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataConverter.class);
    public CsvFilesConvertorConf csvFilesConvertorConf;



    public DataConverter (CsvFilesConvertorConf csvFilesConvertorConf) {
        this.csvFilesConvertorConf = csvFilesConvertorConf;
    }

    public JsonArray toGroupedByMetricDataPoints(List<LineWithDateInfo> LinesWithDateInfo) {

        List<String> groupByList = getGroupByList();

        List<Map<String, Object>> finalGroupedPoints = LinesWithDateInfo.stream()
            //group by metric,tag,date -> [value, date] ( in case where group by just metric : metric,date -> [value, date])
            .collect(Collectors.groupingBy(map -> getCollectorGroupingBy(map,groupByList),
                LinkedHashMap::new,
                Collectors.mapping(this::getCollectorMapping,
                Collectors.toList())))
            .entrySet().stream()
            // convert to Map: metric + tag's values + points
            .map(entry -> {
                Map<String, Object> fieldsAndThereValues = new LinkedHashMap<>();
                groupByList.forEach(i -> {
                    if (i.equals(csvFilesConvertorConf.getName())) {
                        fieldsAndThereValues.put(NAME, entry.getKey().get(groupByList.indexOf(i)));
                        Map<String, Object> tags = ((JsonObject) entry.getValue().get(0).get(1)).getMap();
                        while (tags.values().remove(null));
                        fieldsAndThereValues.put(TAGS, tags);
                    }
                });
                List pointsList = new LinkedList();
                entry.getValue().forEach(i -> pointsList.add(i.get(0)));
                fieldsAndThereValues.put(POINTS_REQUEST_FIELD, pointsList);
                return fieldsAndThereValues;
            }).collect(Collectors.toList());
        return new JsonArray(finalGroupedPoints);
    }

    private List<Object> getCollectorGroupingBy(LineWithDateInfo lineWithDateInfo, List<String> groupByList) {
        List<Object> groupByListForThisMap = new ArrayList<>();
        groupByList.forEach(i -> groupByListForThisMap.add(lineWithDateInfo.mapFromOneCsvLine.get(i)));
        groupByListForThisMap.add(lineWithDateInfo.date);
        return groupByListForThisMap;
    }

    private List getCollectorMapping (LineWithDateInfo lineWithDateInfo) {
        JsonObject tagsList = new JsonObject();
        csvFilesConvertorConf.getTags().forEach(t -> tagsList.put(t, lineWithDateInfo.mapFromOneCsvLine.get(t)));
        return Arrays.asList(Arrays.asList(toNumber(lineWithDateInfo.mapFromOneCsvLine.get(csvFilesConvertorConf.getTimestamp()), csvFilesConvertorConf),
                toDouble(lineWithDateInfo.mapFromOneCsvLine.get(csvFilesConvertorConf.getValue()))),
                tagsList);
    }

    private List<String> getGroupByList() {
        return csvFilesConvertorConf.getGroup_by().stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(NAME))
                return csvFilesConvertorConf.getName();
            else
                throw new IllegalArgumentException("You can not group by a column that is not a tag or the name of the metric");
        }).collect(Collectors.toList());
    }

    public static Object toNumber(Object value, CsvFilesConvertorConf csvFilesConvertorConf) {
        // here you should take timestamps in diff timezones and store only in utc.
        try {
            long longValue = Long.parseLong(Objects.toString(value, "0").replaceAll("\\s+", ""));
            String format = csvFilesConvertorConf.getFormatDate();
            if (format != null)
                switch (format) {
                    case SECONDS_EPOCH:
                        longValue = longValue*1000;
                        break;
                    case MICROSECONDS_EPOCH:
                        longValue = longValue/1000;
                        break;
                    case NANOSECONDS_EPOCH:
                        longValue = longValue/1000000;
                    case MILLISECONDS_EPOCH:
                        break;
                    default:
                        throw  new IllegalArgumentException("TIMESTAMP_UNIT is not correct.");
                }
            return longValue;
        } catch (Exception e) {

            LOGGER.debug("error in parsing date", e);
            long date = 0;
            try {
                date = createDateFormat(csvFilesConvertorConf.getFormatDate(),csvFilesConvertorConf.getTimezoneDate()).parse(value.toString()).getTime();
                return date;
            } catch (ParseException ex) {
                LOGGER.trace("error in parsing date", ex);
                return value;
            }
        }
    }

    private Object toDouble(Object value) {
        try {
            return Double.parseDouble(Objects.toString(value, "0").replaceAll("\\s+",""));
        }catch (Exception e) {
            return value;
        }
    }

    public static SimpleDateFormat createDateFormat(String dataFormat, String timezone) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
        TimeZone myTimeZone = TimeZone.getTimeZone(timezone);
        dateFormat.setTimeZone(myTimeZone);
        dateFormat.setLenient(false);
        return dateFormat;
    }

}
