package com.hurence.webapiservice.http.api.grafana.util;

import com.hurence.webapiservice.historian.handler.GetTimeSeriesHandler;
import com.hurence.webapiservice.timeseries.extractor.MetricRequest;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.hurence.historian.modele.HistorianFields.*;


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

    public String getChunkQualityField() {
        String qualityAggName;
        switch (qualityAgg) {
            case AVG:
                qualityAggName = CHUNK_QUALITY_AVG_FIELD; //TODO
                break;
            case MIN:
                qualityAggName = CHUNK_QUALITY_MIN_FIELD; //TODO
                break;
            case MAX:
                qualityAggName = CHUNK_QUALITY_MAX_FIELD; //TODO
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
