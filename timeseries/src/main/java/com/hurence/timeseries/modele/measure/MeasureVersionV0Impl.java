package com.hurence.timeseries.modele.measure;

import java.util.Map;

public class MeasureVersionV0Impl implements MeasureVersionV0 {

    private String name;
    private double value;
    private long timestamp;
    private int year;
    private int month;
    private String day;
    private Map<String, String> tags;

    public MeasureVersionV0Impl(String name, double value, long timestamp, int year, int month, String day, Map<String, String> tags) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
        this.year = year;
        this.month = month;
        this.day = day;
        this.tags = tags;
    }


    @Override
    public int getYear() {
        return year;
    }

    @Override
    public int getMonth() {
        return month;
    }

    @Override
    public String getDay() {
        return day;
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public double getValue() {
        return value;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
