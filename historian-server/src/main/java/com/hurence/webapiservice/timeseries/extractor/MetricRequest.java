package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.model.Chunk;
import com.hurence.webapiservice.http.api.grafana.util.QualityConfig;

import java.util.Map;
import java.util.Objects;

public class MetricRequest {
    private final String name;
    private final Map<String, String> tags;
    private final QualityConfig quality;

    public MetricRequest(String name, Map<String, String> tags, QualityConfig quality) {
        this.name = name;
        this.tags = tags;
        this.quality = quality;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getTagsAsString() {
        if (!tags.isEmpty())
            return tags.toString();
        else
            return "";
    }

    public QualityConfig getQuality() { return quality; }

    @Override
    public String toString() {
        return "MetricRequest{" +
                "name='" + name + '\'' +
                ", tags=" + tags +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricRequest that = (MetricRequest) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(quality, that.quality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags, quality);
    }

    /**
     *  return true if chunk is corresponding to metric query. This means
     *  that the chunk got expected metric name and tags for this MetricRequest
     * @param chunk
     * @return true if chunk is corresponding to metric query.
     */
    public boolean isChunkMatching(Chunk chunk) {
        final String chunkName = chunk.getName();
        if (!getName().equals(chunkName)) {
            return false;
        }
        if (getQuality() != null && !getQuality().matchChunk(chunk)) {
            return false;
        }
        boolean isChunkMatching = true;
        for(Map.Entry<String, String> entry : getTags().entrySet()) {
            if (!chunk.containsTag(entry.getKey())) {
                return false;
            } else {
                if (!chunk.getTag(entry.getKey()).equals(entry.getValue())) {
                    isChunkMatching = false;
                    break;
                }
            }
        }
        return isChunkMatching;
    }
    
}
