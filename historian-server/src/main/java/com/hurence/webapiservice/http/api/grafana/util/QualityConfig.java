package com.hurence.webapiservice.http.api.grafana.util;

import com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler;
import com.hurence.webapiservice.timeseries.extractor.MetricRequest;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


public class QualityConfig {

    private static Logger LOGGER = LoggerFactory.getLogger(QualityConfig.class);
    private Float quality;
    private QualityAgg qualityAgg;


    public QualityConfig(Float quality, String qualityAgg) {
        this.quality = quality;
        this.qualityAgg = QualityAgg.valueOf(qualityAgg);
    }

    public Float getQuality() {
        return quality;
    }

    public void setQuality(Float quality) {
        this.quality = quality;
    }

    public QualityAgg getQualityAgg() {
        return qualityAgg;
    }

    public void setQualityAgg(QualityAgg qualityAgg) {
        this.qualityAgg = qualityAgg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QualityConfig that = (QualityConfig) o;
        return Objects.equals(quality, that.quality) &&
                Objects.equals(qualityAgg, that.qualityAgg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quality, qualityAgg);
    }

    public boolean matchChunk(JsonObject chunk) {
        Float qualityChunk;
        String chunkQualityField = getChunkQualityField();
        try {
            qualityChunk = chunk.getFloat(chunkQualityField);
        } catch (Exception e) {
            LOGGER.debug("chunk don't have field "+chunkQualityField);
            return false;
        }
        return (this.quality <= qualityChunk);
    }

    public String getChunkQualityField() {
        String qualityAggName;
        switch (qualityAgg) {
            case AVG:
                qualityAggName = "chunk_quality_avg"; //TODO
                break;
            case MIN:
                qualityAggName = "chunk_quality_min"; //TODO
                break;
            case MAX:
                qualityAggName = "chunk_quality_max"; //TODO
                break;
            default:
                throw new IllegalStateException("Unsupported quality aggregation: " + qualityAgg);
        }
        return qualityAggName;
    }
}
