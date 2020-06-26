/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.timeseries;


import com.hurence.logisland.timeseries.dts.Pair;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.Iterator;

import static com.hurence.logisland.timeseries.iterators.FluentIterator.fluent;


/**
 * This class contains some utility functions for dealing with Iterables or Iterators of
 * (time, value)-pairs.
 *
 * @author johannes.siedersleben
 */
public final class TimeSeriesUtil {



    public static ZoneId DEFAULT_TIMEZONE = ZoneId.of("UTC");

    public static DateInfo calculDateFields(final long epochMilliUTC){
        return calculDateFields(epochMilliUTC, DEFAULT_TIMEZONE);
    }

    public static DateInfo calculDateFields(final long epochMilliUTC, ZoneId zoneId){
        Instant instant = Instant.ofEpochMilli(epochMilliUTC);
        ZonedDateTime date = instant.atZone(zoneId);
        DateInfo dateInfo = new DateInfo();
        dateInfo.month = date.getMonthValue();
        dateInfo.day = getDateAsFormat(epochMilliUTC, "yyyy-MM-dd", DEFAULT_TIMEZONE);
        dateInfo.year =date.getYear();
        dateInfo.week =  date.get(WeekFields.ISO.weekOfWeekBasedYear());
        return dateInfo;
    }

    public static String getDateAsFormat(final long epochMilliUTC, String dateFormat){
        return getDateAsFormat(epochMilliUTC, dateFormat, DEFAULT_TIMEZONE);
    }

    public static String getDateAsFormat(final long epochMilliUTC, String dateFormat, ZoneId zoneId){
        DateTimeFormatter dateFormatter = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
                .withZone(zoneId);
        return dateFormatter.format(java.time.Instant.ofEpochMilli(epochMilliUTC));
    }

    /**
     * Private utility class constructor.
     */
    private TimeSeriesUtil() {
    }

    /**
     * Cleansing means:
     * keep the last timeValuePair of identical timestamps and skip all others
     * keep the first timeValuePair of consecutive pairs with identical values
     * and skip all others
     *
     * @param input the time series to be cleansed
     * @param <T>   type of time
     * @param <V>   type of value
     * @return the cleansed time series
     */
    public static <T extends Comparable<T>, V> Iterator<Pair<T, V>> cleanse(Iterator<Pair<T, V>> input) {
        return fluent(input)
                .keepLast(Pair::getFirst)
                .keepFirst(Pair::getSecond);
    }

}
