package com.hurence.historian.modele;

import static com.hurence.historian.modele.SchemaVersion.VERSION_0;

public class SchemaVersions {

    public SchemaVersions() {}

    public static SchemaVersion DEFAULT_SCHEMA_VERSION = VERSION_0;

    public static SchemaVersion getDefaultSchemaVersion() {
        return DEFAULT_SCHEMA_VERSION;
    }
}
