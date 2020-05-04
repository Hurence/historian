package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.RoutingContext;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hurence.historian.modele.HistorianFields.*;

public class DataConverter {
    RoutingContext context;

    public MultiMap multiMap;

    public DataConverter (RoutingContext context) {
        this.context = context;
        multiMap = context.request().formAttributes();
    }

    public List<Map<String, Object>> toGroupedByMetricDataPoints(MappingIterator<Map> rows) {

        if (multiMap.get(MAPPING_NAME) == null)   // what to do in these cases
            multiMap.add(MAPPING_NAME, "metric");
        if (multiMap.get(MAPPING_VALUE) == null)
            multiMap.add(MAPPING_VALUE, "value");
        if (multiMap.get(MAPPING_TIMESTAMP) == null)
            multiMap.add(MAPPING_TIMESTAMP, "timestamp");
        if (multiMap.getAll(GROUP_BY).isEmpty())
            multiMap.add(GROUP_BY, "metric");

        List<String> groupByList = multiMap.getAll(GROUP_BY).stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                 return s.substring(5);
            }else if (s.equals(multiMap.get(MAPPING_NAME)))
                return s;
            else
                throw new IllegalArgumentException("You can not group by a column that is not a tag or the name of the metric");
        }).collect(Collectors.toList());

        List<Map<String, Object>> finalGroupedPoints = toStream(rows)
                //group by metric,tag -> [value, date] ( in case where group by just metric : metric -> [value, date])
                .collect(Collectors.groupingBy(map -> {
                            List<Object> gruopByListForThisMap = new ArrayList<>();
                            groupByList.forEach(i -> {
                                gruopByListForThisMap.add(map.get(i));
                            });
                            return gruopByListForThisMap;
                        },
                        LinkedHashMap::new,
                        Collectors.mapping(map -> Arrays.asList(toDouble(map.get(multiMap.get(MAPPING_VALUE))), toNumber(map.get(multiMap.get(MAPPING_TIMESTAMP)))),
                                Collectors.toList())))
                .entrySet().stream()
                // convert to Map: metric + points
                .map(entry -> {
                    Map<String, Object> fieldsAndThereValues = new LinkedHashMap<>();
                    groupByList.forEach(i -> {
                        if (i.equals(multiMap.get(MAPPING_NAME))) {
                            fieldsAndThereValues.put(NAME, entry.getKey().get(groupByList.indexOf(i))); // here i change the metric (MAPPING_NAME) to name
                            fieldsAndThereValues.put(TAGS, multiMap.getAll(MAPPING_TAGS));  // here for every chunk i put the same field all tags :: tags : [...]
                        }else {
                            fieldsAndThereValues.put(i, entry.getKey().get(groupByList.indexOf(i)));
                        }
                    });
                    Map<String, Object> oneGroupOfPoints = new LinkedHashMap<>(fieldsAndThereValues); // i made the groupByFields so that if there is more than one tag to group by to this will work ... test this
                    oneGroupOfPoints.put(POINTS_REQUEST_FIELD, entry.getValue());
                    return oneGroupOfPoints;
                }).collect(Collectors.toList());
        Map<String,Object> groupByMapAndTags = new HashMap<String, Object>();
        groupByList.set(groupByList.indexOf(multiMap.get(MAPPING_NAME)), NAME);
        groupByMapAndTags.put(GROUPED_BY,groupByList); // here i add the group by and the tags and change the MAPPING_NAME to name
        finalGroupedPoints.add(groupByMapAndTags);                                  // i put this so it will be used in the addTimeserie method
        return finalGroupedPoints;

    }

    private Stream<Map> toStream(MappingIterator<Map> rowIterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(rowIterator, Spliterator.ORDERED), false);
    }

    private long toNumber(Object value) {
        if (multiMap.get(FORMAT_DATE) == null || multiMap.get(TIMEZONE_DATE) == null)
            return Long.parseLong(Objects.toString(value, "0").replaceAll("\\s+",""));
        long date = 0;
        try {
            date = createDateFormat(multiMap.get(FORMAT_DATE),multiMap.get(TIMEZONE_DATE)).parse(value.toString()).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    private double toDouble(Object value) {
        return Double.parseDouble(Objects.toString(value, "0").replaceAll("\\s+",""));
    }

    private static SimpleDateFormat createDateFormat(String dataFormat, String timezone) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
        TimeZone myTimeZone = TimeZone.getTimeZone(timezone);
        dateFormat.setTimeZone(myTimeZone);
        dateFormat.setLenient(false);
        return dateFormat;
    }

}
