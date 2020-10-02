package com.hurence.historian.modele.stream;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.model.Chunk;
import com.hurence.historian.modele.stream.impl.ChunkSolrStreamVersionCurrent;
import com.hurence.historian.modele.stream.impl.JsonSolrStream;
import com.hurence.historian.modele.stream.impl.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;

public interface ChunkStream extends Stream<Chunk> {


    static ChunkStream fromVersionAndSolrStream(SchemaVersion version, TupleStream stream) {
        switch (version) {
            case VERSION_1:
                return new ChunkSolrStreamVersionCurrent(new JsonSolrStream(new SolrStream(stream)));
            default:
                throw new IllegalArgumentException(String.format(
                        "schema version %s for chunks is not yet supported or no longer supported",
                        version));
        }
    }
}