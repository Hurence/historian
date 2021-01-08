package com.hurence.webapiservice.http.api.modele;

public enum ContentType {
    TEXT_PLAIN("text/plain"),
    APPLICATION_JSON("application/json"),
    TEXT_CSV("text/csv");

    public final String contentType;

    private ContentType(String contentType) {
        this.contentType = contentType;
    }

    public static ContentType valueOfContentType(String contentType) {
        for (ContentType e : values()) {
            if (e.contentType.equals(contentType)) {
                return e;
            }
        }
        return null;
    }
}
