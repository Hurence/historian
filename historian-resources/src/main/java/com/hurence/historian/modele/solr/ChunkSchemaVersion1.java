package com.hurence.historian.modele.solr;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion1;
import com.hurence.historian.modele.SchemaVersion;

import java.util.Arrays;
import java.util.Collection;

public class ChunkSchemaVersion1 implements Schema{

    private static final Collection<SolrField> fields = Arrays.asList(
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.ID)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.NAME)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.COMPACTIONS_RUNNING)
                    .withType("string")
                    .withMultivalued(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_VALUE)
                    .withType("string")
                    .withIndexed(false).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_START)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_END)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_AVG)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_COUNT)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_MIN)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_MAX)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_SAX)
                    .withType("ngramtext").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_TREND)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_ORIGIN)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_OUTLIER)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_FIRST)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_LAST)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_STDDEV)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_SUM)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_YEAR)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_MONTH)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_DAY)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_HOUR)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_AVG)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MIN)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MAX)
                    .withType("pfloat").build()
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
