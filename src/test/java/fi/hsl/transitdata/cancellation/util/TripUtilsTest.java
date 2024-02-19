package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TripUtilsTest {
    
    @Test
    public void testAddHSLPrefixToRouteIds() {
        List<String> routeIds = Arrays.asList("1234", "HSL:4567");
        List<String> fixedRouteIds = TripUtils.addHSLPrefixToRouteIds(routeIds);
        assertEquals(2, fixedRouteIds.size());
        assertTrue(fixedRouteIds.contains("HSL:1234"));
        assertTrue(fixedRouteIds.contains("HSL:4567"));
    }
    
    @Test
    public void testFilterTripInfos() {
        LocalDateTime validFrom = TimeUtilsTest.getTestDate("2024-02-21 08:00:00");
        LocalDateTime validTo = TimeUtilsTest.getTestDate("2024-02-23 08:00:00");
        
        List<InternalMessages.TripInfo> inputTrips = getSampleTripInfos();
        List<InternalMessages.TripInfo> outputTrips = TripUtils.filterTripInfos(inputTrips, validFrom, validTo);
        Set<String> outputTripIds = outputTrips.stream().map(InternalMessages.TripInfo::getTripId).collect(Collectors.toSet());
        
        assertEquals(8, outputTrips.size());
        assertEquals(8, outputTripIds.size());
        
        assertFalse(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_0735_20240220"));
        assertFalse(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_1200_20240220"));
        assertFalse(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_1800_20240220"));
        assertFalse(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_2420_20240220"));
        
        assertFalse(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_0735_20240221"));
        assertTrue(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_1200_20240221"));
        assertTrue(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_1800_20240221"));
        assertTrue(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_2420_20240221"));
        
        assertTrue(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_0735_20240222"));
        assertTrue(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_1200_20240222"));
        assertTrue(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_1800_20240222"));
        assertTrue(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_2420_20240222"));
        
        assertTrue(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_0735_20240223"));
        assertFalse(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_1200_20240223"));
        assertFalse(outputTripIds.contains("HSL:4611_MaTiKeToPe_1_1800_20240223"));
        assertFalse(outputTripIds.contains("HSL:1079_MaTiKeToPe_1_2420_20240223"));
    }
    
    @NotNull
    private static List<InternalMessages.TripInfo> getSampleTripInfos() {
        List<InternalMessages.TripInfo> trips = new ArrayList<>();
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240220", "0735", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240220", "1200", 1));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240220", "1800", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240220", "2420", 1));
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240221", "0735", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240221", "1200", 1));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240221", "1800", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240221", "2420", 1));
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240222", "0735", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240222", "1200", 1));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240222", "1800", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240222", "2420", 1));
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240223", "0735", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240223", "1200", 1));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240223", "1800", 1));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240223", "2420", 1));
        
        return trips;
    }
    
    @Test
    public void testGetTripId() {
        assertEquals("HSL:1071_MaTiKeToPe_2_1305_20240215", TripUtils.getTripId("HSL:1071_Ma_2_1305", "20240215"));
        assertEquals("HSL:1071_LaSu_2_2307_20240215", TripUtils.getTripId("HSL:1071_La_2_2307", "20240215"));
    }
    
    public static InternalMessages.TripInfo createTripInfo(
            String routeId, String gtfsId, String operationDay, String startTime, int directionId) {
        InternalMessages.TripInfo.Builder builder = InternalMessages.TripInfo.newBuilder();
        builder.setRouteId(routeId);
        builder.setTripId(TripUtils.getTripId(gtfsId, operationDay));
        builder.setOperatingDay(operationDay);
        builder.setStartTime(startTime);
        builder.setDirectionId(directionId);
        return builder.build();
    }
}