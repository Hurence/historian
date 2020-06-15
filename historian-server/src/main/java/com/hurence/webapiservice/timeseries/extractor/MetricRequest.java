package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.historian.modele.HistorianFields;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.Objects;

import static com.hurence.historian.modele.HistorianFields.NAME;

public class MetricRequest {
    private final String name;
    private final Map<String, String> tags;

    public MetricRequest(String name, Map<String, String> tags) {
        this.name = name;
        this.tags = tags;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

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
                Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags);
    }

    /**
     *  return true if chunk is corresponding to metric query. This means
     *  that the chunk got expected metric name and tags for this MetricRequest
     * @param chunk
     * @return true if chunk is corresponding to metric query.
     */
    public boolean isChunkMatching(JsonObject chunk) {
        final String chunkName = chunk.getString(HistorianFields.NAME);
        if (!getName().equals(chunkName)) {
            return false;
        }
        boolean isChunkMatching = true;
        for(Map.Entry<String, String> entry : getTags().entrySet()) {
            if (!chunk.containsKey(entry.getKey())) {
                return false;
            } else {
                if (!chunk.getString(entry.getKey()).equals(entry.getValue())) {
                    isChunkMatching = false;
                    break;
                }
            }
        }
        return isChunkMatching;
        //TODO feiz, need to adjust this method to be sure to keep chunk matching all tags !
        // Attention aux NPE ! Bien réfléchir a ce qui peut être null ou non a ce stade du code.
    }
    
}
