package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.joda.time.IllegalFieldValueException;
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
    public MultiMap multiMap;

    public static final String DEFAULT_NAME_COLUMN_MAPPING = "metric";
    public static final String DEFAULT_VALUE_COLUMN_MAPPING = "value";
    public static final String DEFAULT_TIMESTAMP_COLUMN_MAPPING = "timestamp";

    public DataConverter (MultiMap multiMap) {
        this.multiMap = multiMap;
    }

    public JsonArray toGroupedByMetricDataPoints(List<IngestionApiUtil.LineWithDateInfo> LinesWithDateInfo) {

        if (multiMap.get(MAPPING_NAME) == null)
            multiMap.add(MAPPING_NAME, DEFAULT_NAME_COLUMN_MAPPING);
        if (multiMap.get(MAPPING_VALUE) == null)
            multiMap.add(MAPPING_VALUE, DEFAULT_VALUE_COLUMN_MAPPING);
        if (multiMap.get(MAPPING_TIMESTAMP) == null)
            multiMap.add(MAPPING_TIMESTAMP, DEFAULT_TIMESTAMP_COLUMN_MAPPING);
        if (multiMap.getAll(GROUP_BY).isEmpty())
            multiMap.add(GROUP_BY, DEFAULT_NAME_FIELD);

        List<String> groupByList = getGroupByList();

        List<Map<String, Object>> finalGroupedPoints = LinesWithDateInfo.stream()
            //group by metric,tag,date -> [value, date, quality] ( in case where group by just metric : metric,date -> [value, date, quality])
            .collect(Collectors.groupingBy(map -> {
                List<Object> groupByListForThisMap = new ArrayList<>();
                groupByList.forEach(i -> groupByListForThisMap.add(map.mapFromOneCsvLine.get(i)));
                groupByListForThisMap.add(map.date);
                return groupByListForThisMap;
                },
                LinkedHashMap::new,
                Collectors.mapping(map -> {
                    JsonObject tagsList = new JsonObject();
                    multiMap.getAll(MAPPING_TAGS).forEach(t -> tagsList.put(t, map.mapFromOneCsvLine.get(t)));
                    return Arrays.asList(Arrays.asList(toNumber(map.mapFromOneCsvLine.get(multiMap.get(MAPPING_TIMESTAMP)), multiMap),
                            toDouble(map.mapFromOneCsvLine.get(multiMap.get(MAPPING_VALUE))),
                            toDouble(map.mapFromOneCsvLine.get(multiMap.get(MAPPING_QUALITY)))),
                            tagsList);
                },
            Collectors.toList())))
            .entrySet().stream()
            // convert to Map: metric + tag's values + points
            .map(entry -> {
                Map<String, Object> fieldsAndThereValues = new LinkedHashMap<>();
                groupByList.forEach(i -> {
                    if (i.equals(multiMap.get(MAPPING_NAME))) {
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

    private List<String> getGroupByList() {
        return multiMap.getAll(GROUP_BY).stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(NAME))
                return multiMap.get(MAPPING_NAME);
            else
                throw new IllegalArgumentException("You can not group by a column that is not a tag or the name of the metric");
        }).collect(Collectors.toList());
    }

    public static Object toNumber(Object value, MultiMap multiMap) {
        // here you should take timestamps in diff timezones and store only in utc.
        try {
            long longValue = Long.parseLong(Objects.toString(value, "0").replaceAll("\\s+", ""));
            String format = multiMap.get(FORMAT_DATE);
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
            /*if ((multiMap.get(FORMAT_DATE) == null) || (multiMap.get(FORMAT_DATE).equals(TimestampUnit.MILLISECONDS_EPOCH.toString())))
                return longValue;
            else if (multiMap.get(FORMAT_DATE).equals(TimestampUnit.SECONDS_EPOCH.toString()))
                return longValue*1000;
            else if (multiMap.get(FORMAT_DATE).equals(TimestampUnit.MICROSECONDS_EPOCH.toString()))
                return longValue/1000;
            else if (multiMap.get(FORMAT_DATE).equals(TimestampUnit.NANOSECONDS_EPOCH.toString()))
                return longValue/1000000;*/
        } catch (Exception e) {
            LOGGER.trace("error in parsing date", e);
            if (multiMap.get(TIMEZONE_DATE) == null)
                multiMap.add(TIMEZONE_DATE, "UTC");
            long date = 0;
            try {
                date = createDateFormat(multiMap.get(FORMAT_DATE),multiMap.get(TIMEZONE_DATE)).parse(value.toString()).getTime();
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
