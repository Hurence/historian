package com.hurence.historian.solr.injector;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.historian.spark.compactor.job.ChunkModeleVersionEVOA0;

public abstract class AbstractEVOA0SolrInjector extends AbstractSolrInjector<ChunkModeleVersionEVOA0> implements SolrInjector {

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.EVOA0;
    }
}
