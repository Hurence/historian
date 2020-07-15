package com.hurence.webapiservice.http.api.grafana.util;

import com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    public boolean matchChunk(JsonObject chunk) {
        Float qualityChunk;
        String chunkQualityField = getChunkQualityField(qualityAgg);
        try {
            qualityChunk = chunk.getFloat(chunkQualityField);
        } catch (Exception e) {
            LOGGER.debug("chunk don't have field "+chunkQualityField);
            return false;
        }
        return this.quality.equals(qualityChunk);
    }

    public static String getChunkQualityField(QualityAgg qualityAgg) {
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
