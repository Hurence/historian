package com.hurence.historian.solr.injector;

import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;
import org.apache.solr.common.SolrInputDocument;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractSolrInjectorChunkCurrentVersion extends AbstractSolrInjector<ChunkVersionCurrent> {

    @Override
    protected SolrInputDocument buildSolrDocument(ChunkVersionCurrent chunk) {
        return SolrDocumentBuilder.fromChunk(chunk);
    }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }
}
