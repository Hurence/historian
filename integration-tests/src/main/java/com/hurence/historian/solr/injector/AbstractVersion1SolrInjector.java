package com.hurence.historian.solr.injector;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.spark.compactor.job.ChunkModeleVersion1;

public abstract class AbstractVersion1SolrInjector extends AbstractSolrInjector<ChunkModeleVersion1> implements SolrInjector{

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }
}
