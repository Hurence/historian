package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.reactivex.core.MultiMap;
import net.sf.cglib.asm.$MethodTooLargeException;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.ingestion.util.DataConverter.*;

public class CsvFilesConvertorConf {

    String timestamp;
    String name;
    String value;
    String quality;
    String formatDate;
    String timezoneDate;
    List<String> group_by = new ArrayList<>();
    List<String> tags = new ArrayList<>();

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
        if (multiMap.get(MAPPING_QUALITY) == null)
            this.quality = DEFAULT_QUALITY_COLUMN_MAPPING;
        else
            this.quality = multiMap.get(MAPPING_QUALITY);
        if (multiMap.get(FORMAT_DATE) != null)
            this.formatDate = multiMap.get(FORMAT_DATE);
        if (multiMap.getAll(GROUP_BY) != null)
            this.group_by = multiMap.getAll(GROUP_BY);
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

}