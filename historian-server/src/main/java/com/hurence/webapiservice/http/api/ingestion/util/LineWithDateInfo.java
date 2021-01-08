package com.hurence.webapiservice.http.api.ingestion.util;

import java.util.Map;

public class LineWithDateInfo {
    public Map mapFromOneCsvLine;
    public String date;

    LineWithDateInfo(Map map, String date) {
        this.mapFromOneCsvLine = map;
        this.date = date;
    }
}