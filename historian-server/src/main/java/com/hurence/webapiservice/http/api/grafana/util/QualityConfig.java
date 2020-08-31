package com.hurence.webapiservice.http.api.grafana.util;

import com.hurence.historian.modele.FieldNamesInsideHistorianService;
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

    public Float getQualityValue() {
        return quality;
    }

    public void setQuality(Float quality) {
        this.quality = quality;
    }

    public QualityAgg getQualityAgg() {
        return qualityAgg;
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
        String chunkQualityField = getChunkQualityFieldForSampling();
        if(chunkQualityField == null)
            return true;
        try {
            qualityChunk = chunk.getFloat(chunkQualityField);
        } catch (Exception e) {
            LOGGER.trace("chunk don't have field "+chunkQualityField);
            return false;
        }
        return (this.quality <= qualityChunk);
    }

    public String getChunkQualityFieldForSampling() {
        String qualityAggName;
        switch (qualityAgg) {
            case AVG:
                qualityAggName = FieldNamesInsideHistorianService.CHUNK_QUALITY_AVG_FIELD;
                break;
            case MIN:
                qualityAggName = FieldNamesInsideHistorianService.CHUNK_QUALITY_MIN_FIELD;
                break;
            case MAX:
                qualityAggName = FieldNamesInsideHistorianService.CHUNK_QUALITY_MAX_FIELD;
                break;
            case FIRST:
                qualityAggName = FieldNamesInsideHistorianService.CHUNK_QUALITY_FIRST_FIELD;
                break;
            case NONE:
                qualityAggName = null;
                break;
            default:
                throw new IllegalStateException("Unsupported quality aggregation: " + qualityAgg);
        }
        return qualityAggName;
    }
}
