package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.reactivex.core.MultiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class CsvFilesConvertorConf {

    private final String timestamp;
    private final String name;
    private final String value;
    private final String quality;
    private String formatDate;
    private String timezoneDate;
    private List<String> group_by = new ArrayList<>();
    private List<String> tags = new ArrayList<>();

    public static final String DEFAULT_QUALITY_COLUMN_MAPPING = "quality";
    public static final String DEFAULT_NAME_COLUMN_MAPPING = "metric";
    public static final String DEFAULT_VALUE_COLUMN_MAPPING = "value";
    public static final String DEFAULT_TIMESTAMP_COLUMN_MAPPING = "timestamp";

    public CsvFilesConvertorConf(MultiMap multiMap) {
        if (multiMap.get(MAPPING_TIMESTAMP) == null)
            this.timestamp = DEFAULT_TIMESTAMP_COLUMN_MAPPING; // change the variable place
        else
            this.timestamp = multiMap.get(MAPPING_TIMESTAMP);
        if (multiMap.get(MAPPING_NAME) == null)
            this.name = DEFAULT_NAME_COLUMN_MAPPING;
        else
            this.name = multiMap.get(MAPPING_NAME);
        if (multiMap.get(MAPPING_VALUE) == null)
            this.value = DEFAULT_VALUE_COLUMN_MAPPING;
        else
            this.value = multiMap.get(MAPPING_VALUE);
        if (multiMap.get(MAPPING_QUALITY) == null)   // TODO here if we take csv without quality i should let quality null
            this.quality = DEFAULT_QUALITY_COLUMN_MAPPING;
        else
            this.quality = multiMap.get(MAPPING_QUALITY);
        if (multiMap.get(FORMAT_DATE) != null)
            this.formatDate = multiMap.get(FORMAT_DATE);
        if (multiMap.getAll(GROUP_BY) != null)
            this.group_by = multiMap.getAll(GROUP_BY);
        else
            this.group_by.add(this.name);
        if (multiMap.getAll(MAPPING_TAGS) != null)
            this.tags = multiMap.getAll(MAPPING_TAGS);
        if (multiMap.get(TIMEZONE_DATE) == null)
            this.timezoneDate = "UTC";
        else
            this.timezoneDate = multiMap.get(TIMEZONE_DATE);
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public String getQuality() {
        return quality;
    }

    public String getFormatDate() {
        return formatDate;
    }

    public List<String> getGroup_by() {
        return group_by;
    }

    public List<String> getTags() {
        return tags;
    }

    public String getTimezoneDate() {
        return timezoneDate;
    }

    public List<String> getGroupByList() {
        return group_by.stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(NAME))
                return name;
            else
                throw new IllegalArgumentException("You can not group by a column that is not a tag or the name of the metric");
        }).collect(Collectors.toList());
    }

}