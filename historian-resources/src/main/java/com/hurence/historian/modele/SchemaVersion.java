package com.hurence.historian.modele;

public enum SchemaVersion {
    EVOA0,
    VERSION_0,
    VERSION_1;

    public static SchemaVersion getCurrentVersion() {
        return VERSION_1;
    }
}
