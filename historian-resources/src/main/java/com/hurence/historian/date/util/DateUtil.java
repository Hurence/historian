package com.hurence.historian.date.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Useful Date utilities.
 *
 * @author BalusC
 * @see CalendarUtil
 * @link http://balusc.omnifaces.org/2007/09/dateutil.html
 */
public final class DateUtil {

    // Init ---------------------------------------------------------------------------------------

    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {{
        put("^\\d{8}$", "yyyyMMdd");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        put("^\\d{12}$", "yyyyMMddHHmm");
        put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        put("^\\d{14}$", "yyyyMMddHHmmss");
        put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "MM/dd/yyyy HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
    }};

    private DateUtil() {
        // Utility class, hide the constructor.
    }

    // Converters ---------------------------------------------------------------------------------

    /**
     * Convert the given date to a Calendar object. The TimeZone will be derived from the local
     * operating system's timezone.
     * @param date The date to be converted to Calendar.
     * @return The Calendar object set to the given date and using the local timezone.
     */
    public static Calendar toCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);
        return calendar;
    }

    /**
     * Convert the given date to a Calendar object with the given timezone.
     * @param date The date to be converted to Calendar.
     * @param timeZone The timezone to be set in the Calendar.
     * @return The Calendar object set to the given date and timezone.
     */
    public static Calendar toCalendar(Date date, TimeZone timeZone) {
        Calendar calendar = toCalendar(date);
        calendar.setTimeZone(timeZone);
        return calendar;
    }

    /**
     * Parse the given date string to date object and return a date instance based on the given
     * date string. This makes use of the {@link DateUtil#determineDateFormat(String)} to determine
     * the SimpleDateFormat pattern to be used for parsing.
     * @param dateString The date string to be parsed to date object.
     * @return The parsed date object.
     * @throws ParseException If the date format pattern of the given date string is unknown, or if
     * the given date string or its actual date is invalid based on the date format pattern.
     */
    public static Date parse(String dateString) throws ParseException {
        String dateFormat = determineDateFormat(dateString);
        if (dateFormat == null) {
            throw new ParseException("Unknown date format.", 0);
        }
        return parse(dateString, dateFormat);
    }

    /**
     * Validate the actual date of the given date string based on the given date format pattern and
     * return a date instance based on the given date string.
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return The parsed date object.
     * @throws ParseException If the given date string or its actual date is invalid based on the
     * given date format pattern.
     * @see SimpleDateFormat
     */
    public static Date parse(String dateString, String dateFormat) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
        return simpleDateFormat.parse(dateString);
    }

    // Validators ---------------------------------------------------------------------------------

    /**
     * Checks whether the actual date of the given date string is valid. This makes use of the
     * {@link DateUtil#determineDateFormat(String)} to determine the SimpleDateFormat pattern to be
     * used for parsing.
     * @param dateString The date string.
     * @return True if the actual date of the given date string is valid.
     */
    public static boolean isValidDate(String dateString) {
        try {
            parse(dateString);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * Checks whether the actual date of the given date string is valid based on the given date
     * format pattern.
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return True if the actual date of the given date string is valid based on the given date
     * format pattern.
     * @see SimpleDateFormat
     */
    public static boolean isValidDate(String dateString, String dateFormat) {
        try {
            parse(dateString, dateFormat);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    // Checkers -----------------------------------------------------------------------------------

    /**
     * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
     * format is unknown. You can simply extend DateUtil with more formats if needed.
     * @param dateString The date string to determine the SimpleDateFormat pattern for.
     * @return The matching SimpleDateFormat pattern, or null if format is unknown.
     * @see SimpleDateFormat
     */
    public static String determineDateFormat(String dateString) {
        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (dateString.toLowerCase().matches(regexp)) {
                return DATE_FORMAT_REGEXPS.get(regexp);
            }
        }
        return null; // Unknown format.
    }

    // Changers -----------------------------------------------------------------------------------

    /**
     * Add the given amount of years to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addYears(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of years to.
     * @param years The amount of years to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addYears(Date date, int years) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addYears(calendar, years);
        return calendar.getTime();
    }

    /**
     * Add the given amount of months to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addMonths(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of months to.
     * @param months The amount of months to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addMonths(Date date, int months) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addMonths(calendar, months);
        return calendar.getTime();
    }

    /**
     * Add the given amount of days to the given date. It actually converts the date to Calendar and
     * calls {@link CalendarUtil#addDays(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of days to.
     * @param days The amount of days to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addDays(Date date, int days) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addDays(calendar, days);
        return calendar.getTime();
    }

    /**
     * Add the given amount of hours to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addHours(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of hours to.
     * @param hours The amount of hours to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addHours(Date date, int hours) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addHours(calendar, hours);
        return calendar.getTime();
    }

    /**
     * Add the given amount of minutes to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addMinutes(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of minutes to.
     * @param minutes The amount of minutes to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addMinutes(Date date, int minutes) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addMinutes(calendar, minutes);
        return calendar.getTime();
    }

    /**
     * Add the given amount of seconds to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addSeconds(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of seconds to.
     * @param seconds The amount of seconds to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addSeconds(Date date, int seconds) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addSeconds(calendar, seconds);
        return calendar.getTime();
    }

    /**
     * Add the given amount of millis to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addMillis(Calendar, int)} and then converts back to date.
     * @param date The date to add the given amount of millis to.
     * @param millis The amount of millis to be added to the given date. Negative values are also
     * allowed, it will just go back in time.
     */
    public static Date addMillis(Date date, int millis) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addMillis(calendar, millis);
        return calendar.getTime();
    }

    // Comparators --------------------------------------------------------------------------------

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year. It actually
     * converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameYear(Calendar, Calendar)}.
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year.
     * @see CalendarUtil#sameYear(Calendar, Calendar)
     */
    public static boolean sameYear(Date one, Date two) {
        return CalendarUtil.sameYear(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year and month. It
     * actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameMonth(Calendar, Calendar)}.
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year and month.
     * @see CalendarUtil#sameMonth(Calendar, Calendar)
     */
    public static boolean sameMonth(Date one, Date two) {
        return CalendarUtil.sameMonth(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month and day. It
     * actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameDay(Calendar, Calendar)}.
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month and day.
     * @see CalendarUtil#sameDay(Calendar, Calendar)
     */
    public static boolean sameDay(Date one, Date two) {
        return CalendarUtil.sameDay(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day and
     * hour. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameHour(Calendar, Calendar)}.
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month, day and hour.
     * @see CalendarUtil#sameHour(Calendar, Calendar)
     */
    public static boolean sameHour(Date one, Date two) {
        return CalendarUtil.sameHour(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day, hour
     * and minute. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameMinute(Calendar, Calendar)}.
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month, day, hour and minute.
     * @see CalendarUtil#sameMinute(Calendar, Calendar)
     */
    public static boolean sameMinute(Date one, Date two) {
        return CalendarUtil.sameMinute(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day, hour,
     * minute and second. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameSecond(Calendar, Calendar)}.
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month, day, hour, minute and
     * second.
     * @see CalendarUtil#sameSecond(Calendar, Calendar)
     */
    public static boolean sameSecond(Date one, Date two) {
        return CalendarUtil.sameSecond(toCalendar(one), toCalendar(two));
    }

    // Calculators --------------------------------------------------------------------------------

    /**
     * Retrieve the amount of elapsed years between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedYears(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed years between the two given dates
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedYears(Calendar, Calendar)
     */
    public static int elapsedYears(Date before, Date after) {
        return CalendarUtil.elapsedYears(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed months between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedMonths(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed months between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedMonths(Calendar, Calendar)
     */
    public static int elapsedMonths(Date before, Date after) {
        return CalendarUtil.elapsedMonths(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed days between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedDays(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed days between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedDays(Calendar, Calendar)
     */
    public static int elapsedDays(Date before, Date after) {
        return CalendarUtil.elapsedDays(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed hours between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedHours(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed hours between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedHours(Calendar, Calendar)
     */
    public static int elapsedHours(Date before, Date after) {
        return CalendarUtil.elapsedHours(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed minutes between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedMinutes(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed minutes between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedMinutes(Calendar, Calendar)
     */
    public static int elapsedMinutes(Date before, Date after) {
        return CalendarUtil.elapsedMinutes(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed seconds between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedSeconds(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed seconds between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedSeconds(Calendar, Calendar)
     */
    public static int elapsedSeconds(Date before, Date after) {
        return CalendarUtil.elapsedSeconds(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed milliseconds between the two given dates. It actually converts
     * the both dates to Calendar and calls {@link CalendarUtil#elapsedMillis(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The amount of elapsed milliseconds between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedMillis(Calendar, Calendar)
     */
    public static long elapsedMillis(Date before, Date after) {
        return CalendarUtil.elapsedMillis(toCalendar(before), toCalendar(after));
    }

    /**
     * Calculate the total of elapsed time from years up to seconds between the two given dates. It
     * Returns an int array with the elapsed years, months, days, hours, minutes and seconds
     * respectively. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#elapsedTime(Calendar, Calendar)}.
     * @param before The first date with expected date before the second date.
     * @param after The second date with expected date after the first date.
     * @return The elapsed time between the two given dates in years, months, days, hours, minutes
     * and seconds.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedTime(Calendar, Calendar)
     */
    public static int[] elapsedTime(Date before, Date after) {
        return CalendarUtil.elapsedTime(toCalendar(before), toCalendar(after));
    }

}