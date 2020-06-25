package com.hurence.historian.modele;


/**
 * Static class to put default COLLECTION names used by Historian.
 */
public class HistorianCollections {
    private HistorianCollections() {}

    public static String DEFAULT_COLLECTION_HISTORIAN = "historian";
    public static String DEFAULT_COLLECTION_ANNOTATION = "annotation";
    public static String DEFAULT_COLLECTION_REPORT = "historian-reports";

    public static HistorianCollection[] getAllCollections() {
        return HistorianCollection.values();
    }
}


