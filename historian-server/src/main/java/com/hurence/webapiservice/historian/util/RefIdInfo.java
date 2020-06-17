package com.hurence.webapiservice.historian.util;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracter.TIMESERIE_NAME;
import static com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracter.TIMESERIE_TAGS;

public class RefIdInfo {
    private final String name;
    private final String refId;
    private final Map<String, String> tags;

    public RefIdInfo(String name, String refId, Map<String, String> tags) {
        this.name = name;
        this.refId =refId;
        this.tags = tags;

    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getRefId() {
        return refId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RefIdInfo that = (RefIdInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(refId, that.refId) &&
                Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags, refId);
    }

    public boolean isMetricMatching(JsonObject timeserieWithoutRefIdObject) {
        String metricName = timeserieWithoutRefIdObject.getString(TIMESERIE_NAME);
        JsonObject metricTags = timeserieWithoutRefIdObject.getJsonObject(TIMESERIE_TAGS);
        Map<String,String> metricTagsMap = new HashMap<>();
        for (Map.Entry<String, Object> metricTagsEntry : metricTags.getMap().entrySet()){
            metricTagsMap.put(metricTagsEntry.getKey() ,metricTagsEntry.getValue().toString());
        }
        return name.equals(metricName) && tags.equals(metricTagsMap);
    }
}
