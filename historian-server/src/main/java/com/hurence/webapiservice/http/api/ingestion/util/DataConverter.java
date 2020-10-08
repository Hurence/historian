package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.model.HistorianServiceFields.POINTS;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
import static com.hurence.webapiservice.http.api.ingestion.util.TimestampUnit.*;

public class DataConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataConverter.class);
    public CsvFilesConvertorConf csvFilesConvertorConf;

    public DataConverter (CsvFilesConvertorConf csvFilesConvertorConf) {
        this.csvFilesConvertorConf = csvFilesConvertorConf;
    }

    /**
     * @param lineWithDateInfos        List<LineWithDateInfo>
     *
     * group by the lineWithDateInfos into a json array :
     *                                 group by : metric,tag,date
     *
     * @return JsonArray
     */
    public JsonArray toGroupedByMetricDataPoints(List<LineWithDateInfo> lineWithDateInfos) {

        List<Map<String, Object>> finalGroupedPoints = lineWithDateInfos.stream()
                //group by metric,tag,date -> [value, date] ( in case where group by just metric : metric,date -> [value, date])
                .collect(Collectors.groupingBy(this::customGroupingBy,
                        LinkedHashMap::new,
                        Collectors.mapping(this::customMapping, Collectors.toList())))
                .entrySet().stream()
                // convert to Map: metric + tag's values + points
                .map(this::customMap)
                .collect(Collectors.toList());
        return new JsonArray(finalGroupedPoints);
    }

    private LinkedList<Object> customGroupingBy(LineWithDateInfo map) {
        LinkedList<Object> groupByListForThisMap = new LinkedList<>();
        csvFilesConvertorConf.getGroupByList().forEach(i -> groupByListForThisMap.add(map.mapFromOneCsvLine.get(i)));
        groupByListForThisMap.add(map.date);
        return groupByListForThisMap;
    }

    private List<Iterable<? extends Object>> customMapping (LineWithDateInfo map) {
        JsonObject tagsList = new JsonObject();
        csvFilesConvertorConf.getTags().forEach(t -> tagsList.put(t, map.mapFromOneCsvLine.get(t)));
        return Arrays.asList(Arrays.asList(toNumber(map.mapFromOneCsvLine.get(csvFilesConvertorConf.getTimestamp()), csvFilesConvertorConf),
                toDouble(map.mapFromOneCsvLine.get(csvFilesConvertorConf.getValue()))),
                tagsList);
    }

    private Map<String, Object> customMap(Map.Entry<LinkedList<Object>, List<List<Iterable<? extends Object>>>> entry) {
        Map<String, Object> fieldsAndThereValues = new LinkedHashMap<>();
        putNameFieldAndTagsFields(fieldsAndThereValues,entry);
        putPointsFields(fieldsAndThereValues, entry);
        return fieldsAndThereValues;
    }

    /**
     * @param fieldsAndThereValues        Map<String, Object>
     * @param entry        Map.Entry<List<Object>, List<List<Iterable<? extends Object>>>>
     *
     * put the field name and the tags fields into the fieldsAndThereValues Map.
     *
     */
    private void putNameFieldAndTagsFields(Map<String, Object> fieldsAndThereValues,
                                           Map.Entry<LinkedList<Object>, List<List<Iterable<? extends Object>>>> entry) {
        String name = nameToReturn(entry);
        fieldsAndThereValues.put(FIELD_NAME, name);
        Map<String, Object> tags = ((JsonObject) entry.getValue().get(0).get(1)).getMap();
        tags.values().remove(null);
        fieldsAndThereValues.put(FIELD_TAGS, tags);

    }

    private String nameToReturn(Map.Entry<LinkedList<Object>, List<List<Iterable<? extends Object>>>> entry) {

        if (csvFilesConvertorConf.getCustomName() != null)
            return csvFilesConvertorConf.getCustomName();
        else
            return entry.getKey().get(0).toString();
    }

    /**
     * @param fieldsAndThereValues        Map<String, Object>
     * @param entry        Map.Entry<List<Object>, List<List<Iterable<? extends Object>>>>
     *
     * put the field points into the fieldsAndThereValues Map.
     *
     */
    private void putPointsFields(Map<String, Object> fieldsAndThereValues,
                                           Map.Entry<LinkedList<Object>, List<List<Iterable<? extends Object>>>> entry) {
        List pointsList = new LinkedList();
        entry.getValue().forEach(i -> pointsList.add(i.get(0)));
        fieldsAndThereValues.put(POINTS, pointsList);

    }

    /**
     * @param value                    Object
     * @param csvFilesConvertorConf    CsvFilesConvertorConf
     *
     * get the timestamp as long if possible.
     *
     */
    public static Object toNumber(Object value, CsvFilesConvertorConf csvFilesConvertorConf) {
        try {
            return getNumberIfLong(value, csvFilesConvertorConf);
        } catch (Exception e) {
            return getNumberIfNotLong(value, csvFilesConvertorConf);
        }
    }

    /**
     * @param value                    Object
     * @param csvFilesConvertorConf    CsvFilesConvertorConf
     *
     * get the timestamp as long and convert it to milliseconds.
     *
     */
    private static long getNumberIfLong(Object value, CsvFilesConvertorConf csvFilesConvertorConf) {
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
    }

    /**
     * @param value                    Object
     * @param csvFilesConvertorConf    CsvFilesConvertorConf
     *
     * if timestamp is not a long, check the date format and the time zone in input, and get the time, as long
     *                                 if possible.
     *
     */
    private static Object getNumberIfNotLong(Object value, CsvFilesConvertorConf csvFilesConvertorConf) {
        long date;
        try {
            date = createDateFormat(csvFilesConvertorConf.getFormatDate(),csvFilesConvertorConf.getTimezoneDate())
                    .parse(value.toString()).getTime();
            return date;
        } catch (ParseException ex) {
            LOGGER.trace("error in parsing date", ex);
            return value;
        }
    }

    /**
     * @param value      Object
     *
     * get the value as double if possible.
     *
     */
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
