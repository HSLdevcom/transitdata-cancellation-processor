package fi.hsl.transitdata.cancellation.util;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.Assert.*;

public class TimeUtilsTest {
    
    private static final DateTimeFormatter DATETIMEFORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public static LocalDateTime getTestDate(String dateAsString) {
        return LocalDateTime.parse(dateAsString, DATETIMEFORMATTER);
    }
    
    @Test
    public void testGetDatesTwoDates() {
        LocalDateTime validFrom = getTestDate("2024-01-04 23:05:00");
        LocalDateTime validTo = getTestDate("2024-01-05 06:30:45");
        
        List<String> dates = TimeUtils.getDatesAsList(validFrom, validTo);
        assertEquals(2, dates.size());
        assertEquals("20240104", dates.get(0));
        assertEquals("20240105", dates.get(1));
    }
    
    @Test
    public void testGetDatesThreeDates() {
        LocalDateTime validFrom = getTestDate("2024-01-04 10:05:00");
        LocalDateTime validTo = getTestDate("2024-01-06 18:30:45");
        
        List<String> dates = TimeUtils.getDatesAsList(validFrom, validTo);
        assertEquals(3, dates.size());
        assertEquals("20240104", dates.get(0));
        assertEquals("20240105", dates.get(1));
        assertEquals("20240106", dates.get(2));
    }
    
    @Test
    public void testGetDatesOneDate() {
        LocalDateTime validFrom = getTestDate("2024-01-04 10:05:00");
        LocalDateTime validTo = getTestDate("2024-01-04 18:30:45");
        
        List<String> dates = TimeUtils.getDatesAsList(validFrom, validTo);
        assertEquals(1, dates.size());
        assertEquals("20240104", dates.get(0));
    }
    
    @Test
    public void testGetDatesFourDatesInTwoMonths() {
        LocalDateTime validFrom = getTestDate("2024-01-30 10:05:00");
        LocalDateTime validTo = getTestDate("2024-02-02 18:30:45");
        
        List<String> dates = TimeUtils.getDatesAsList(validFrom, validTo);
        assertEquals(4, dates.size());
        assertEquals("20240130", dates.get(0));
        assertEquals("20240131", dates.get(1));
        assertEquals("20240201", dates.get(2));
        assertEquals("20240202", dates.get(3));
    }
    
    @Test
    public void testGetDatesValidFromIsNull() {
        LocalDateTime validFrom = null;
        LocalDateTime validTo = getTestDate("2024-01-06 18:30:45");
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDatesAsList(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validFrom is null"));
    }
    
    @Test
    public void testGetDatesValidToIsNull() {
        LocalDateTime validFrom = getTestDate("2024-01-06 18:30:45");
        LocalDateTime validTo = null;
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDatesAsList(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validTo is null"));
    }
    
    @Test
    public void testGetDatesBothInputParametersAreNull() {
        LocalDateTime validFrom = null;
        LocalDateTime validTo = null;
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDatesAsList(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validFrom and validTo are null"));
    }
    
    @Test
    public void testGetDatesValidFromIsAfterValidTo() {
        LocalDateTime validFrom = getTestDate("2024-01-06 18:30:45");
        LocalDateTime validTo = getTestDate("2024-01-04 10:05:00");
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDatesAsList(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validFrom is after validTo"));
    }
    
    @Test
    public void testGetDate() {
        String dateAsString = "20240108";
        String timeAsString = "1020";
        
        LocalDateTime outputDate = TimeUtils.getDate(dateAsString, timeAsString);
        
        assertEquals("2024-01-08 10:20:00", outputDate.format(DATETIMEFORMATTER));
    }
    
    @Test
    public void testGetDateAsString() {
        int unixTimestampInSeconds = 1704319200;
        String dateAsString = TimeUtils.getDateAsString(unixTimestampInSeconds);

        Instant expectedInstant = Instant.ofEpochSecond(unixTimestampInSeconds);
        String expected = LocalDateTime.ofInstant(expectedInstant, ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("yyyyMMdd"));


        assertEquals(expected, dateAsString);
    }
    
    @Test
    public void testGetShortTime() {
        int secondsSinceMidnight = 25200;
        String timeAsString = TimeUtils.getTimeAsString(secondsSinceMidnight);
        assertEquals("0700", timeAsString);
    }
    
    @Test
    public void testGetShortTimeAfterMidnight() {
        int secondsSinceMidnight = 88140;
        String timeAsString = TimeUtils.getTimeAsString(secondsSinceMidnight);
        assertEquals("0029", timeAsString);
    }
}