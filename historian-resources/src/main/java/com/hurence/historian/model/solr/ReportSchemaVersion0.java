package com.hurence.historian.model.solr;

import com.hurence.historian.model.HistorianReportCollectionFieldsVersion0;
import com.hurence.historian.model.SchemaVersion;

import java.util.Arrays;
import java.util.Collection;

public class ReportSchemaVersion0 implements Schema {

    private static final Collection<SolrField> fields = Arrays.asList(
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.ID)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.TYPE)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.START)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.END)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.JOB_DURATION_IN_MILLI)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.STATUS)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.NUMBER_OF_CHUNKS_IN_INPUT)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.NUMBER_OF_CHUNKS_IN_OUTPUT)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.TOTAL_METRICS_RECHUNKED)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.JOB_CONF)
                    .withType("string")
                    .withIndexed(false).build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.ERROR)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.EXCEPTION_MSG)
                    .withType("string").build()
    );

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    @Override
    public Collection<SolrField> getFields() {
        return fields;
    }
}
