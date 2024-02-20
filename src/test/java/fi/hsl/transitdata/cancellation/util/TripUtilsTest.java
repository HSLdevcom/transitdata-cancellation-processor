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
    public void testRemoveDuplicates() {
        List<InternalMessages.TripInfo> inputTrips = getSampleTripInfosWithDuplicates();
        List<InternalMessages.TripInfo> outputTrips = TripUtils.removeDuplicates(inputTrips);
        
        assertEquals(3, outputTrips.size());
        boolean contains4611 = false;
        boolean contains1079 = false;
        boolean contains1030 = false;
        
        for (InternalMessages.TripInfo trip : outputTrips) {
            if ("HSL:4611".equals(trip.getRouteId())) {
                contains4611 = true;
                assertEquals("HSL:4611_MaTiKeToPe_1_0735_20240221", trip.getTripId());
                assertEquals("20240221", trip.getOperatingDay());
                assertEquals("0735", trip.getStartTime());
                assertEquals(1, trip.getDirectionId());
            } else if ("HSL:1079".equals(trip.getRouteId())) {
                contains1079 = true;
                assertEquals("HSL:1079_MaTiKeToPe_1_1200_20240222", trip.getTripId());
                assertEquals("20240222", trip.getOperatingDay());
                assertEquals("1200", trip.getStartTime());
                assertEquals(1, trip.getDirectionId());
            } else if ("HSL:1030".equals(trip.getRouteId())) {
                contains1030 = true;
                assertEquals("HSL:1030_20240212_MaTiKeToPe_2_1408_20240220", trip.getTripId());
                assertEquals("20240220", trip.getOperatingDay());
                assertEquals("1408", trip.getStartTime());
                assertEquals(1, trip.getDirectionId());
            } else {
                assertTrue("Unknown route: " + trip.getRouteId(), false);
            }
        }
        
        assertTrue(contains4611);
        assertTrue(contains1079);
        assertTrue(contains1030);
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
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240220", "0735", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240220", "1200", 1, true));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240220", "1800", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240220", "2420", 1, true));
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240221", "0735", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240221", "1200", 1, true));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240221", "1800", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240221", "2420", 1, true));
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240222", "0735", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240222", "1200", 1, true));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240222", "1800", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240222", "2420", 1, true));
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240223", "0735", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240223", "1200", 1, true));
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_1800", "20240223", "1800", 1, true));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_2420", "20240223", "2420", 1, true));
        
        return trips;
    }
    
    @NotNull
    private static List<InternalMessages.TripInfo> getSampleTripInfosWithDuplicates() {
        List<InternalMessages.TripInfo> trips = new ArrayList<>();
        
        trips.add(createTripInfo("HSL:4611", "HSL:4611_Ti_1_0735", "20240221", "0735", 1, false));
        trips.add(createTripInfo("HSL:1079", "HSL:1079_Ti_1_1200", "20240222", "1200", 1, false));
        
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240212_Ma_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240212_Ti_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240212_Ke_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240212_To_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240212_Pe_2_1408", "20240220", "1408", 1, false));
        
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240219_Ma_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240219_Ti_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240219_Ke_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240219_To_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240219_Pe_2_1408", "20240220", "1408", 1, false));
        
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240226_Ma_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240226_Ti_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240226_Ke_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240226_To_2_1408", "20240220", "1408", 1, false));
        trips.add(createTripInfo("HSL:1030", "HSL:1030_20240226_Pe_2_1408", "20240220", "1408", 1, false));
        
        return trips;
    }
    
    @Test
    public void testGetTripId() {
        assertEquals("HSL:1071_MaTiKeToPe_2_1305_20240215", TripUtils.getTripId("HSL:1071_Ma_2_1305", "20240215"));
        assertEquals("HSL:1071_LaSu_2_2307_20240215", TripUtils.getTripId("HSL:1071_La_2_2307", "20240215"));
    }
    
    public static InternalMessages.TripInfo createTripInfo(
            String routeId, String gtfsId, String operationDay, String startTime, int directionId, boolean generateTripId) {
        InternalMessages.TripInfo.Builder builder = InternalMessages.TripInfo.newBuilder();
        builder.setRouteId(routeId);
        builder.setTripId(generateTripId ? TripUtils.getTripId(gtfsId, operationDay) : gtfsId);
        builder.setOperatingDay(operationDay);
        builder.setStartTime(startTime);
        builder.setDirectionId(directionId);
        return builder.build();
    }
}