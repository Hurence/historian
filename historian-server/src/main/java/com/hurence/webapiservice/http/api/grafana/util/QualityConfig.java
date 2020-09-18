package com.hurence.webapiservice.http.api.grafana.util;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionCurrent;
import com.hurence.timeseries.model.Chunk;
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

    public boolean matchChunk(Chunk chunk) {
        Float qualityChunk = getQualityOfChunk(chunk);
        if(qualityChunk == null)
            return true;
        return (this.quality <= qualityChunk);
    }

    private Float getQualityOfChunk(Chunk chunk) {
        switch (qualityAgg) {
            case AVG:
                return Double.valueOf(chunk.getQualityAvg()).floatValue();
            case MIN:
                return Double.valueOf(chunk.getQualityMin()).floatValue();
            case MAX:
                return Double.valueOf(chunk.getQualityMax()).floatValue();
            case FIRST:
                return Double.valueOf(chunk.getQualityFirst()).floatValue();
            case NONE:
                return null;
            default:
                throw new IllegalStateException("Unsupported quality aggregation: " + qualityAgg);
        }
    }

    public String getChunkQualityFieldForSampling() {
        switch (qualityAgg) {
            case AVG:
                return HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_AVG;
            case MIN:
                return HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MIN;
            case MAX:
                return HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MAX;
            case FIRST:
                return HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_FIRST;
            case NONE:
                return null;
            default:
                throw new IllegalStateException("Unsupported quality aggregation: " + qualityAgg);
        }
    }
}
