package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class DataConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataConverter.class);
    public MultiMap multiMap;

    public DataConverter (MultiMap multiMap) {
        this.multiMap = multiMap;
    }

    public JsonArray toGroupedByMetricDataPoints(List<Map> rows) {

        if (multiMap.get(MAPPING_NAME) == null)
            multiMap.add(MAPPING_NAME, "metric");
        if (multiMap.get(MAPPING_VALUE) == null)
            multiMap.add(MAPPING_VALUE, "value");
        if (multiMap.get(MAPPING_TIMESTAMP) == null)
            multiMap.add(MAPPING_TIMESTAMP, "timestamp");
        if (multiMap.getAll(GROUP_BY).isEmpty())
            multiMap.add(GROUP_BY, DEFAULT_NAME_FIELD);

        List<String> groupByList = multiMap.getAll(GROUP_BY).stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                 return s.substring(5);
            }else if (s.equals(NAME))
                return multiMap.get(MAPPING_NAME);
            else
                throw new IllegalArgumentException("You can not group by a column that is not a tag or the name of the metric");
        }).collect(Collectors.toList());

        List<Map<String, Object>> finalGroupedPoints = rows.stream()
            //group by metric,tag,date -> [value, date] ( in case where group by just metric : metric,date -> [value, date])
            .collect(Collectors.groupingBy(map -> {
                List<Object> groupByListForThisMap = new ArrayList<>();
                groupByList.forEach(i -> groupByListForThisMap.add(map.get(i)));
                groupByListForThisMap.add(map.get(DATE));
                return groupByListForThisMap;
                },
                LinkedHashMap::new,
                Collectors.mapping(map -> {
                    JsonObject tagsList = new JsonObject();
                    multiMap.getAll(MAPPING_TAGS).forEach(t -> tagsList.put(t, map.get(t)));
                    return Arrays.asList(Arrays.asList(toNumber(map.get(multiMap.get(MAPPING_TIMESTAMP)), multiMap),
                            toDouble(map.get(multiMap.get(MAPPING_VALUE)))),
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
                        fieldsAndThereValues.put(TAGS, entry.getValue().get(0).get(1));
                    }
                });
                List pointsList = new LinkedList();
                entry.getValue().forEach(i -> pointsList.add(i.get(0)));
                fieldsAndThereValues.put(POINTS_REQUEST_FIELD, pointsList);
                return fieldsAndThereValues;
            }).collect(Collectors.toList());
        return new JsonArray(finalGroupedPoints);
    }

    public static Object toNumber(Object value, MultiMap multiMap) {
        if (multiMap.get(FORMAT_DATE) == null || multiMap.get(TIMEZONE_DATE) == null)
            try {
                return Long.parseLong(Objects.toString(value, "0").replaceAll("\\s+",""));
            } catch (Exception e) {
                LOGGER.debug("error in parsing date", e);
                return value;
            }
        long date = 0;
        try {
            date = createDateFormat(multiMap.get(FORMAT_DATE),multiMap.get(TIMEZONE_DATE)).parse(value.toString()).getTime();
        } catch (ParseException e) {
            LOGGER.debug("error in parsing date", e);
            return value;
        }
        return date;
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
