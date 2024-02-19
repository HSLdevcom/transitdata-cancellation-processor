package fi.hsl.transitdata.cancellation.util;

import org.apache.commons.lang3.time.DurationFormatUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class TimeUtils {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HHmm");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    /**
     * Returns dates within the given time period. For example, if time period is from 2024-01-02 to 2024-01-05, this
     * method returns the following dates as strings: '20240102', '20240103', '20240104' and '20240105'.
     * Throws RuntimeException if parameter validFrom and/or validTo is null, or if validFrom is after validTo.
     * @param validFrom alert valid from timestamp
     * @param validTo alert valid to timestamp
     * @return list of dates as string, each date has format 'YYYYMMDD'
     */
    public static List<String> getDatesAsList(LocalDateTime validFrom, LocalDateTime validTo) {
        if (validFrom == null && validTo == null) {
            throw new RuntimeException("validFrom and validTo are null");
        } else if (validFrom == null) {
            throw new RuntimeException("validFrom is null");
        } else if (validTo == null) {
            throw new RuntimeException("validTo is null");
        } else if (validFrom.isAfter(validTo)) {
            throw new RuntimeException("validFrom is after validTo");
        }
        
        List<String> dates = new ArrayList<>();
        
        String validFromAsString = getDateAsString(validFrom);
        String validToAsString = getDateAsString(validTo);
        
        LocalDateTime nextDate = validFrom;
        String nextDateAsString = validFromAsString;
        dates.add(validFromAsString);
        
        while (!validToAsString.equals(nextDateAsString)) {
            nextDate = getNextDate(nextDate);
            nextDateAsString = getDateAsString(nextDate);
            dates.add(nextDateAsString);
        }
        
        return dates;
    }
    
    // Return date as format 'YYYYMMDD', for example '20240102'
    private static String getDateAsString(LocalDateTime someDate) {
        return someDate.format(DATE_FORMATTER);
    }
    
    private static LocalDateTime getNextDate(LocalDateTime someDate) {
        return someDate.plusDays(1);
    }
    
    /**
     * Returns date object
     * @param dateAsString date as format 'YYYYMMDD'
     * @param timeAsString time as format 'HHMM'
     * @return date object
     */
    static LocalDateTime getDate(String dateAsString, String timeAsString) {
        String timestampAsString = dateAsString + " " + timeAsString;
        return LocalDateTime.parse(timestampAsString, DATE_TIME_FORMATTER);
    }
    
    /**
     * Get date in String, for example '20240108'.
     * @param serviceDay Departure date of the trip. Format: Unix timestamp (local time) in seconds.
     * @return Date as string in format 'YYYYMMDD'
     */
    public static String getDateAsString(Integer serviceDay) {
        Instant instant = Instant.ofEpochSecond(serviceDay);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("Europe/Helsinki"));
        return localDateTime.format(DATE_FORMATTER);
    }
    
    /**
     * Get time in string, for example '1555'.
     * @param scheduledDeparture Scheduled departure time. Format: seconds since midnight of the departure date.
     * @return Time as string in format 'HHMM'
     */
    public static String getTimeAsString(Integer scheduledDeparture) {
        return DurationFormatUtils.formatDuration(scheduledDeparture * 1000, "HHmm", true);
    }
    
    /**
     * Convert given LocalDateTime object into two strings according to 30-hour clock. Examples:
     * Input: '2024-01-29 15:50', Output: KEY '20240129', VALUE '1550'
     * Input: '2024-01-30 00:29', Output: KEY '20240129', VALUE '2429'
     * @param someDateTime datetime in LocalDateTime format
     * @return KEY: date (e.g. "2901"), VALUE: time (e.g. "1542")
     */
    public static AbstractMap.SimpleEntry<String, String> convertInto30hClockStrings(LocalDateTime someDateTime) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HHmm");
        
        if (someDateTime.getHour() < 6) {
            LocalDateTime newDateTime = someDateTime.minusDays(1);
            int localTime = Integer.parseInt(newDateTime.format(timeFormatter));
            String newLocalTime = String.valueOf(2400 + localTime);
            return new AbstractMap.SimpleEntry<>(newDateTime.format(dateFormatter), newLocalTime);
        }
        
        return new AbstractMap.SimpleEntry<>(someDateTime.format(dateFormatter), someDateTime.format(timeFormatter));
    }
    
    /**
     * Return boolean value that indicates whether the given timestamp (operatingDay + startTime) is between the
     * validFrom and validTo timestamps.
     * @param operatingDay date e.g. '20240129'
     * @param startTime time e.g. '1550'
     * @param validFromAsSimpleEntry KEY: date (e.g. "20242901"), VALUE: time (e.g. "1542")
     * @param validToAsSimpleEntry KEY: date (e.g. "20242901"), VALUE: time (e.g. "1542")
     * @return boolean value
     */
    public static boolean isBetween(
            String operatingDay,
            String startTime,
            AbstractMap.SimpleEntry<String, String> validFromAsSimpleEntry,
            AbstractMap.SimpleEntry<String, String> validToAsSimpleEntry) {
        
        long operatingTimestamp = Long.parseLong(operatingDay + startTime);
        long validFrom = Long.parseLong(validFromAsSimpleEntry.getKey() + validFromAsSimpleEntry.getValue());
        long validTo = Long.parseLong(validToAsSimpleEntry.getKey() + validToAsSimpleEntry.getValue());
        
        return operatingTimestamp >= validFrom && operatingTimestamp <= validTo;
    }
}