package com.hurence.logisland.record;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.WeekFields;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EvoaUtils {

    static Pattern  csvRegexp = Pattern.compile("(\\w+)\\.?(\\w+-?\\w+-?\\w+)?\\.?(\\w+)?");


    public static Record setDateFields(final Record record){

        /**
         * set some data metas
         */
        if (record.hasField(TimeSeriesRecord.CHUNK_START)) {
            try {
                Instant instant = Instant.ofEpochMilli(record.getField(TimeSeriesRecord.CHUNK_START).asLong());
                LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
                int month = localDate.getMonthValue();
                int day = localDate.getDayOfMonth();
                int year = localDate.getYear();
                int week = localDate.get(WeekFields.ISO.weekOfWeekBasedYear());

                record.setIntField("month", month);
                record.setIntField("day", day);
                record.setIntField("year", year);
                record.setIntField("week", week);
            } catch (Exception ex) {
                //do nothing
            }
        }

        return record;
    }

    public static synchronized Record setBusinessFields(Record record) {
        /**
         * set some specific fields
         */
        if (record.hasField(TimeSeriesRecord.METRIC_NAME)) {

            try {
                Matcher m = csvRegexp.matcher(record.getField(TimeSeriesRecord.METRIC_NAME).asString());
                boolean b = m.matches();
                if (b) {
                    record.setStringField("code_install", m.group(1));
                    record.setStringField("sensor", m.group(2));
                }
            } catch (Exception ex) {
                //do nothing
            }
        }
        return record;
    }

    public static synchronized Record setChunkOrigin(Record record, String origin) {

        record.setStringField(TimeSeriesRecord.CHUNK_ORIGIN, origin);

        return record;
    }

    public static synchronized Record setHashId(Record record) {


        try {
            String sha256hex = Hashing.sha256()
                    .hashString(record.getField(TimeSeriesRecord.CHUNK_VALUE).asString(), StandardCharsets.UTF_8)
                    .toString();

            record.setId(sha256hex);
        }catch (Exception ex) {
// do nothing
        }




        return record;
    }




}
