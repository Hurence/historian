package com.hurence.historian.model.stream;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.model.stream.impl.ChunkSolrStreamVersionCurrent;
import com.hurence.historian.model.stream.impl.JsonSolrStream;
import com.hurence.historian.model.stream.impl.SolrStream;
import com.hurence.timeseries.model.Chunk;
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
