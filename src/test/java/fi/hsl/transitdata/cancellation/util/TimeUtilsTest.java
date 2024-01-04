package fi.hsl.transitdata.cancellation.util;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class TimeUtilsTest {
    
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private static Date getTestDate(String dateAsString) {
        Date date;
        try {
            date = DATE_FORMATTER.parse(dateAsString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date;
    }
    
    @Test
    public void getDatesTwoDates() {
        Date validFrom = getTestDate("2024-01-04 23:05:00");
        Date validTo = getTestDate("2024-01-05 06:30:45");
        
        List<String> dates = TimeUtils.getDates(validFrom, validTo);
        assertEquals(2, dates.size());
        assertEquals("20240104", dates.get(0));
        assertEquals("20240105", dates.get(1));
    }
    
    @Test
    public void getDatesThreeDates() {
        Date validFrom = getTestDate("2024-01-04 10:05:00");
        Date validTo = getTestDate("2024-01-06 18:30:45");
        
        List<String> dates = TimeUtils.getDates(validFrom, validTo);
        assertEquals(3, dates.size());
        assertEquals("20240104", dates.get(0));
        assertEquals("20240105", dates.get(1));
        assertEquals("20240106", dates.get(2));
    }
    
    @Test
    public void getDatesOneDate() {
        Date validFrom = getTestDate("2024-01-04 10:05:00");
        Date validTo = getTestDate("2024-01-04 18:30:45");
        
        List<String> dates = TimeUtils.getDates(validFrom, validTo);
        assertEquals(1, dates.size());
        assertEquals("20240104", dates.get(0));
    }
    
    @Test
    public void getDatesFourDatesInTwoMonths() {
        Date validFrom = getTestDate("2024-01-30 10:05:00");
        Date validTo = getTestDate("2024-02-02 18:30:45");
        
        List<String> dates = TimeUtils.getDates(validFrom, validTo);
        assertEquals(4, dates.size());
        assertEquals("20240130", dates.get(0));
        assertEquals("20240131", dates.get(1));
        assertEquals("20240201", dates.get(2));
        assertEquals("20240202", dates.get(3));
    }
    
    @Test
    public void getDatesValidFromIsNull() {
        Date validFrom = null;
        Date validTo = getTestDate("2024-01-06 18:30:45");
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDates(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validFrom is null"));
    }
    
    @Test
    public void getDatesValidToIsNull() {
        Date validFrom = getTestDate("2024-01-06 18:30:45");
        Date validTo = null;
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDates(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validTo is null"));
    }
    
    @Test
    public void getDatesBothInputParametersAreNull() {
        Date validFrom = null;
        Date validTo = null;
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDates(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validFrom and validTo are null"));
    }
    
    @Test
    public void getDatesValidFromIsAfterValidTo() {
        Date validFrom = getTestDate("2024-01-06 18:30:45");
        Date validTo = getTestDate("2024-01-04 10:05:00");
        
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> TimeUtils.getDates(validFrom, validTo));
        assertTrue(thrown.getMessage().equals("validFrom is after validTo"));
    }
}