package com.hurence.webapiservice.http.api.ingestion.util;

import com.hurence.historian.modele.HistorianServiceFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.api.ingestion.util.TimestampUnit.*;


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

        if (multiMap.get(HistorianServiceFields.MAPPING_NAME) == null)
            multiMap.add(HistorianServiceFields.MAPPING_NAME, DEFAULT_NAME_COLUMN_MAPPING);
        if (multiMap.get(HistorianServiceFields.MAPPING_VALUE) == null)
            multiMap.add(HistorianServiceFields.MAPPING_VALUE, DEFAULT_VALUE_COLUMN_MAPPING);
        if (multiMap.get(HistorianServiceFields.MAPPING_TIMESTAMP) == null)
            multiMap.add(HistorianServiceFields.MAPPING_TIMESTAMP, DEFAULT_TIMESTAMP_COLUMN_MAPPING);
        if (multiMap.getAll(HistorianServiceFields.GROUP_BY).isEmpty())
            multiMap.add(HistorianServiceFields.GROUP_BY, HistorianServiceFields.DEFAULT_NAME_FIELD);

        List<String> groupByList = getGroupByList();

        List<Map<String, Object>> finalGroupedPoints = LinesWithDateInfo.stream()
            //group by metric,tag,date -> [value, date] ( in case where group by just metric : metric,date -> [value, date])
            .collect(Collectors.groupingBy(map -> {
                List<Object> groupByListForThisMap = new ArrayList<>();
                groupByList.forEach(i -> groupByListForThisMap.add(map.mapFromOneCsvLine.get(i)));
                groupByListForThisMap.add(map.date);
                return groupByListForThisMap;
                },
                LinkedHashMap::new,
                Collectors.mapping(map -> {
                    JsonObject tagsList = new JsonObject();
                    multiMap.getAll(HistorianServiceFields.MAPPING_TAGS).forEach(t -> tagsList.put(t, map.mapFromOneCsvLine.get(t)));
                    return Arrays.asList(Arrays.asList(toNumber(map.mapFromOneCsvLine.get(multiMap.get(HistorianServiceFields.MAPPING_TIMESTAMP)), multiMap),
                            toDouble(map.mapFromOneCsvLine.get(multiMap.get(HistorianServiceFields.MAPPING_VALUE)))),
                            tagsList);
                },
            Collectors.toList())))
            .entrySet().stream()
            // convert to Map: metric + tag's values + points
            .map(entry -> {
                Map<String, Object> fieldsAndThereValues = new LinkedHashMap<>();
                groupByList.forEach(i -> {
                    if (i.equals(multiMap.get(HistorianServiceFields.MAPPING_NAME))) {
                        fieldsAndThereValues.put(HistorianServiceFields.NAME, entry.getKey().get(groupByList.indexOf(i)));
                        Map<String, Object> tags = ((JsonObject) entry.getValue().get(0).get(1)).getMap();
                        while (tags.values().remove(null));
                        fieldsAndThereValues.put(HistorianServiceFields.TAGS, tags);
                    }
                });
                List pointsList = new LinkedList();
                entry.getValue().forEach(i -> pointsList.add(i.get(0)));
                fieldsAndThereValues.put(HistorianServiceFields.POINTS, pointsList);
                return fieldsAndThereValues;
            }).collect(Collectors.toList());
        return new JsonArray(finalGroupedPoints);
    }

    private List<String> getGroupByList() {
        return multiMap.getAll(HistorianServiceFields.GROUP_BY).stream().map(s -> {
            if (s.startsWith(HistorianServiceFields.TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(HistorianServiceFields.NAME))
                return multiMap.get(HistorianServiceFields.MAPPING_NAME);
            else
                throw new IllegalArgumentException("You can not group by a column that is not a tag or the name of the metric");
        }).collect(Collectors.toList());
    }

    public static Object toNumber(Object value, MultiMap multiMap) {
        // here you should take timestamps in diff timezones and store only in utc.
        try {
            long longValue = Long.parseLong(Objects.toString(value, "0").replaceAll("\\s+", ""));
            String format = multiMap.get(HistorianServiceFields.FORMAT_DATE);
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
            if (multiMap.get(HistorianServiceFields.TIMEZONE_DATE) == null)
                multiMap.add(HistorianServiceFields.TIMEZONE_DATE, "UTC");
            long date = 0;
            try {
                date = createDateFormat(multiMap.get(HistorianServiceFields.FORMAT_DATE),multiMap.get(HistorianServiceFields.TIMEZONE_DATE)).parse(value.toString()).getTime();
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
