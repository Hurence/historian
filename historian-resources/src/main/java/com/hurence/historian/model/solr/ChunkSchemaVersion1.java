package com.hurence.historian.model.solr;

import com.hurence.historian.model.HistorianChunkCollectionFieldsVersionCurrent;
import com.hurence.historian.model.SchemaVersion;
import java.util.Arrays;
import java.util.Collection;

public class ChunkSchemaVersion1 implements Schema{

    private static final Collection<SolrField> fields = Arrays.asList(
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.ID)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.NAME)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.COMPACTIONS_RUNNING)
                    .withType("string")
                    .withMultivalued(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_VALUE)
                    .withType("string")
                    .withIndexed(false).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_START)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_END)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_AVG)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_COUNT)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MIN)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MAX)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SAX)
                    .withType("ngramtext").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_TREND)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_ORIGIN)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_OUTLIER)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_FIRST)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_LAST)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_STDDEV)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SUM)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_YEAR)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MONTH)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_DAY)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_AVG)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MIN)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MAX)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_SUM)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_FIRST)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.METRIC_KEY)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_HOUR)
                    .withType("pint").build()
    );

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }

    @Override
    public Collection<SolrField> getFields() {
        return fields;
    }
}
