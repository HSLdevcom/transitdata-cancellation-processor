package fi.hsl.transitdata.cancellation.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TimeUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeUtils.class);
    
    private static final DateTimeFormatter SHORT_DATETIMEFORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private static final DateTimeFormatter LONG_DATETIMEFORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HHmm");
    
    // This methods is copied from transitdata-omm-cancellation-source
    public static Optional<Long> toUtcEpochMs(String localTimestamp) {
        return toUtcEpochMs(localTimestamp, "timeZone");
    }

    // This methods is copied from transitdata-omm-cancellation-source
    public static Optional<Long> toUtcEpochMs(String localTimestamp, String zoneId) {
        if (localTimestamp == null || localTimestamp.isEmpty())
            return Optional.empty();

        try {
            LocalDateTime dt = LocalDateTime.parse(localTimestamp.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
            ZoneId zone = ZoneId.of(zoneId);
            long epochMs = dt.atZone(zone).toInstant().toEpochMilli();
            return Optional.of(epochMs);
        } catch (Exception e) {
            TimeUtils.log.error("Failed to parse datetime from " + localTimestamp, e);
            return Optional.empty();
        }
    }
    
    /**
     * Returns dates within the given time period. For example, if time period is from 2024-01-02 to 2024-01-05, this
     * method returns the following dates as strings: '20240102', '20240103', '20240104' and '20240105'
     * @param validFrom alert valid from timestamp
     * @param validTo alert valid to timestamp
     * @return list of dates as string, each date has format 'YYYYMMDD'
     * @exception throws RuntimeException if parameter validFrom and/or validTo is null, or if validFrom is after validTo
     */
    public static List<String> getDates(LocalDateTime validFrom, LocalDateTime validTo) {
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
        return someDate.format(SHORT_DATETIMEFORMATTER);
    }
    
    private static LocalDateTime getNextDate(LocalDateTime someDate) {
        return someDate.plusDays(1);
    }
    
    /**
     * Returns date object
     * @param dateAsString date as format 'YYYYMMDD'
     * @param timeAsString time as format 'HHMM'
     * @return
     */
    static LocalDateTime getDate(String dateAsString, String timeAsString) {
        String timestampAsString = dateAsString + " " + timeAsString;
        return LocalDateTime.parse(timestampAsString, LONG_DATETIMEFORMATTER);
    }
}