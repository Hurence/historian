package com.hurence.historian.modele.solr;


import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionCurrent;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.historian.modele.SchemaVersion;

/**
 * This represent the mapping of fields in solr depending on the schema version.
 * This class enables us to adapt solr query depending on solr schema.
 * But it is really simple and naif, just a mapping between names fields.
 * So this can work only if we add or rename fields between schemas.
 * For supporting really big breaking changes we should implement different version
 * of a class building a request. Or a method building request depending on a version...
 *
 */
public class SolrFieldMapping {

    public String CHUNK_NAME = HistorianChunkCollectionFieldsVersion0.NAME;
    public String CHUNK_ID_FIELD = HistorianChunkCollectionFieldsVersion0.ID;
    public String CHUNK_VERSION_FIELD = "_version_";
    public String CHUNK_VALUE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE;
    public String CHUNK_MAX_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_MAX;
    public String CHUNK_MIN_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_MIN;
    public String CHUNK_START_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_START;
    public String CHUNK_END_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_END;
    public String CHUNK_FIRST_VALUE_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST;
    public String CHUNK_AVG_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_AVG;
    public String CHUNK_COUNT_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT;
    public String CHUNK_SUM_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SUM;
    public String CHUNK_SAX_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_SAX;
    public String CHUNK_TREND_FIELD = HistorianChunkCollectionFieldsVersion0.CHUNK_TREND;
    public String CHUNK_YEAR = HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR;
    public String CHUNK_MONTH = HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH;
    public String CHUNK_DAY = HistorianChunkCollectionFieldsVersion0.CHUNK_DAY;
    public String CHUNK_ORIGIN = HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN;
    //since VERSION 1
    public String CHUNK_QUALITY_FIRST_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_FIRST;
    public String CHUNK_QUALITY_AVG_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_AVG;
    public String CHUNK_QUALITY_MIN_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MIN;
    public String CHUNK_QUALITY_MAX_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MAX;
    public String CHUNK_QUALITY_SUM_FIELD = HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_SUM;

    public SolrFieldMapping() {}

    public static SolrFieldMapping fromVersion(SchemaVersion version) {
        SolrFieldMapping fields = new SolrFieldMapping();
        switch (version) {
            case EVOA0:
                fields.CHUNK_COUNT_FIELD = HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE;
                break;
            case VERSION_0:
                break;
            case VERSION_1:
                break;
            default:
                throw new IllegalArgumentException(String.format("version '%s' is not yet supported !", version.toString()));
        }
        return fields;
    }
}



