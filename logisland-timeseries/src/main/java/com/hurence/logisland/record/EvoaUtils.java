package com.hurence.logisland.record;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.HistorianFields;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.WeekFields;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EvoaUtils {

    static Pattern csvRegexp = Pattern.compile("(\\w+)\\.?(\\w+-?\\w+-?\\w+)?\\.?(\\w+)?");


    /**
     * add month day, year and week fields
     *
     * @param record
     * @return
     */
    public static synchronized Record setDateFields(final Record record) {
        if (record.hasField(HistorianFields.CHUNK_START_FIELD)) {
            try {
                Instant instant = Instant.ofEpochMilli(record.getField(HistorianFields.CHUNK_START_FIELD).asLong());
                LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
                int month = localDate.getMonthValue();
                int day = localDate.getDayOfMonth();
                int year = localDate.getYear();
                int week = localDate.get(WeekFields.ISO.weekOfWeekBasedYear());

                record.setIntField("chunk_month", month);
                record.setIntField("chunk_day", day);
                record.setIntField("chunk_year", year);
                record.setIntField("chunk_week", week);
            } catch (Exception ex) {
                //do nothing
            }
        }

        return record;
    }

    /**
     * infer some fields from the name
     *
     * @param record
     * @return
     */
    public static synchronized Record setBusinessFields(Record record) {
        if (record.hasField(HistorianFields.NAME)) {

            try {
                Matcher m = csvRegexp.matcher(record.getField(HistorianFields.NAME).asString());
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


    /**
     * setup from where comes the record
     *
     * @param record
     * @param origin
     * @return
     */
    public static synchronized Record setChunkOrigin(Record record, String origin) {
        record.setStringField(HistorianFields.CHUNK_ORIGIN, origin);
        return record;
    }

    /**
     * modify Id with sha256 of value, start and name
     *
     * @param record
     * @return
     */
    public static synchronized Record setHashId(Record record) {
        try {
            String toHash = record.getField(HistorianFields.CHUNK_VALUE_FIELD).asString() +
                    record.getField(HistorianFields.NAME).asString() +
                    record.getField(HistorianFields.CHUNK_START_FIELD).asLong();

            String sha256hex = Hashing.sha256()
                    .hashString(toHash, StandardCharsets.UTF_8)
                    .toString();

            record.setId(sha256hex);
        } catch (Exception ex) {
            // do nothing
        }

        return record;
    }


}
