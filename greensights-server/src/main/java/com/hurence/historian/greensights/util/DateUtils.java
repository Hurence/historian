package com.hurence.historian.greensights.util;

import com.hurence.historian.greensights.model.request.ComputeRequest;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DateUtils {


    public static Date fromDateRequest(String dateStr) {

        DateTime computedDateTime;
        if (dateStr.equalsIgnoreCase("today")) {
            computedDateTime = new DateTime().withTimeAtStartOfDay();
        } else {
            DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
            computedDateTime = parser.parseDateTime(dateStr);
        }
        return computedDateTime.toDate();
    }

    public static List<ComputeRequest> requestsBetweenDays(ComputeRequest rootRequest) {


        DateTimeFormatter formatter = ISODateTimeFormat.yearMonthDay();
        DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
        LocalDate startDate = parser.parseDateTime(rootRequest.getStartDate()).toLocalDate();
        LocalDate endDate = parser.parseDateTime(rootRequest.getEndDate()).toLocalDate();
        List<ComputeRequest> requests = new ArrayList<>();

        for (LocalDate currentdate = startDate;
             currentdate.isBefore(endDate) || currentdate.isEqual(endDate);
             currentdate = currentdate.plusDays(1)) {

            String lowerBoundDate = formatter.print(currentdate.minusDays(1));
            String upperBoundDate = formatter.print(currentdate);
            ComputeRequest request = new ComputeRequest(
                    lowerBoundDate,
                    upperBoundDate,
                    rootRequest.getDoSaveMeasures(),
                    rootRequest.getDoSaveMetrics(),
                    rootRequest.getDoComputeDayByDay(),
                    rootRequest.getRootUrlFilters(),
                    rootRequest.getAccountFilters()
            );
            requests.add(request);
        }

        return requests;
    }
}
