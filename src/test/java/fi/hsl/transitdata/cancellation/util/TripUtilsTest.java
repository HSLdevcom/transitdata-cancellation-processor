package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TripUtilsTest {
    
    @Test
    public void getTripInfos() {
    }
    
    @Test
    public void filterTripInfos() {
        Date validFrom = TimeUtilsTest.getTestDate("2024-01-02 08:57:00");
        Date validTo = TimeUtilsTest.getTestDate("2024-01-04 17:30:45");
        
        List<InternalMessages.TripInfo> inputTrips = new ArrayList<>();
        // before
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240102_Ti_2_0855", "20240102", "0855", 1));
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:1079", "HSL:1079_20240102_La_1_0734", "20240102", "0734", 2));
        // inside
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240102_Ti_2_0900", "20240102", "0900", 1));
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240103_Ti_2_1000", "20240103", "1000", 1));
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:1079", "HSL:4611_20240103_Ti_2_1100", "20240103", "1100", 1));
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:1079", "HSL:1079_20240103_La_1_1200", "20240103", "1200", 2));
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240104_Ti_2_1300", "20240104", "1300", 1));
        // after
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240104_Ti_2_1731", "20240104", "1731", 1));
        inputTrips.add(TripUtilsTest.createTripInfo("HSL:1079", "HSL:1079_20240104_La_1_1950", "20240104", "1950", 2));
        
        List<InternalMessages.TripInfo> outputTrips = TripUtils.filterTripInfos(inputTrips, validFrom, validTo);
        Set<String> outputTripIds = outputTrips.stream().map(trip -> trip.getTripId()).collect(Collectors.toSet());
        
        assertEquals(5, outputTrips.size());
        assertFalse(outputTripIds.contains("HSL:4611_20240102_Ti_2_0855"));
        assertFalse(outputTripIds.contains("HSL:1079_20240102_La_1_0734"));
        
        assertTrue(outputTripIds.contains("HSL:4611_20240102_Ti_2_0900"));
        assertTrue(outputTripIds.contains("HSL:4611_20240103_Ti_2_1000"));
        assertTrue(outputTripIds.contains("HSL:4611_20240103_Ti_2_1100"));
        assertTrue(outputTripIds.contains("HSL:1079_20240103_La_1_1200"));
        assertTrue(outputTripIds.contains("HSL:4611_20240104_Ti_2_1300"));
        
        assertFalse(outputTripIds.contains("HSL:4611_20240104_Ti_2_1731"));
        assertFalse(outputTripIds.contains("HSL:1079_20240104_La_1_1950"));
        
    }
    
    @Test
    public void testGetTripInfos() {
    }
    
    public static InternalMessages.TripInfo createTripInfo(
            String routeId, String tripId, String operationDay, String startTime, int directionId) {
        InternalMessages.TripInfo.Builder builder = InternalMessages.TripInfo.newBuilder();
        builder.setRouteId(routeId);
        builder.setTripId(tripId);
        builder.setOperatingDay(operationDay);
        builder.setStartTime(startTime);
        builder.setDirectionId(directionId);
        return builder.build();
    }
}