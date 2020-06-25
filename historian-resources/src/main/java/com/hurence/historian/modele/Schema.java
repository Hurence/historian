package com.hurence.historian.modele;

import java.util.Collection;

public interface Schema {

    static Schema getChunkSchema(SchemaVersion version) {
        switch (version) {
            case EVOA0:
                return new ChunkSchemaVersionEVOA0();
            case VERSION_0:
                return new ChunkSchemaVersion0();
            default:
                throw new IllegalArgumentException(String.format("version '%s' is not yet supported !", version.toString()));
        }
    }

//    static Schema getAnnotationSchema(SchemaVersion version) {
//        switch (version) {
//            case EVOA0:
//            case VERSION_0:
//                return new AnnotationSchemaVersion0();
//            default:
//                throw new IllegalArgumentException(String.format("version '%s' is not yet supported !", version.toString()));
//        }
//    }
//
    static Schema getReportSchema(SchemaVersion version) {
        switch (version) {
            case EVOA0:
            case VERSION_0:
                return new ReportSchemaVersion0();
            default:
                throw new IllegalArgumentException(String.format("version '%s' is not yet supported !", version.toString()));
        }
    }

    SchemaVersion getVersion();

    Collection<Field> getFields();
}
