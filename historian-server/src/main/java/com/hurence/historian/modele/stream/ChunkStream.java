package com.hurence.historian.modele.stream;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.mymodele.Chunk;
import com.hurence.historian.modele.stream.impl.ChunkSolrStreamVersion0;
import com.hurence.historian.modele.stream.impl.ChunkSolrStreamVersionEVOA0;
import com.hurence.historian.modele.stream.impl.JsonSolrStream;
import com.hurence.historian.modele.stream.impl.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;

public interface ChunkStream extends Stream<Chunk> {


    public static ChunkStream fromVersionAndSolrStream(SchemaVersion version, TupleStream stream) {
        switch (version) {
            case EVOA0:
                return new ChunkSolrStreamVersionEVOA0(new JsonSolrStream(new SolrStream(stream)));
            case VERSION_0:
                return new ChunkSolrStreamVersion0(new JsonSolrStream(new SolrStream(stream)));
            default:
                throw new IllegalArgumentException(String.format(
                        "schema version %s for chunks is not yet supported or no longer supported",
                        version));
        }
    }
}
