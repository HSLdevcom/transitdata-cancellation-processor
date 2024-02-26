package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;

public class BulletinUtilsTest {
    @Test
    public void testFilterMassCancellationsFromEmptyBulletinsList() {
        List<InternalMessages.Bulletin> inputBulletins = new ArrayList<>();
        List<InternalMessages.Bulletin> outputBulletins = BulletinUtils.filterMassCancellationsFromBulletins(inputBulletins);
        
        assertTrue(outputBulletins.isEmpty());
    }
    
    @Test
    public void testFilterMassCancellations() {
        List<InternalMessages.Bulletin> outputBulletins = BulletinUtils.filterMassCancellationsFromBulletins(initializeTestBulletin());
        
        assertEquals(2, outputBulletins.size());
    }
    
    private static List<InternalMessages.Bulletin> initializeTestTrips(MockedStatic<TripUtils> tripUtils) {
        List<InternalMessages.TripInfo> trips = new ArrayList<>();
        trips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240102_Ti_2_1415", "20240102", "1415", 1, true));
        trips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240102_Ti_2_1515", "20240102", "1515", 1, true));
        trips.add(TripUtilsTest.createTripInfo("HSL:1079", "HSL:1079_20240102_La_1_0734", "20240102", "0734", 2, true));
        
        tripUtils.when(() -> TripUtils.getTripInfos(
                anyList(), any(LocalDateTime.class), any(LocalDateTime.class), "Europe/Helsinki", anyString())).thenReturn(trips);
        
        InternalMessages.Bulletin bulletinMassCancellation = createBulletin(
                InternalMessages.Bulletin.Impact.CANCELLED,
                InternalMessages.Bulletin.Priority.WARNING,
                "HSL:4611", "HSL:1079");
        
        List<InternalMessages.Bulletin> inputBulletins = new ArrayList<>();
        inputBulletins.add(bulletinMassCancellation);
        return inputBulletins;
    }
    
    @Test
    public void testRemoveHSLPrefixFromRouteId() {
        String routeId1 = BulletinUtils.removeHSLPrefixFromRouteId("1234");
        String routeId2 = BulletinUtils.removeHSLPrefixFromRouteId("HSL:4567");
        assertEquals("1234", routeId1);
        assertEquals("4567", routeId2);
    }
    
    @Test
    public void testFormatTime() {
        assertEquals("07:42:00", BulletinUtils.formatTime("0742"));
    }
    
    private static List<InternalMessages.Bulletin> initializeTestBulletin() {
        InternalMessages.Bulletin bulletinMassCancellation1 = createBulletin(
                InternalMessages.Bulletin.Impact.CANCELLED,
                InternalMessages.Bulletin.Priority.WARNING,
                "HSL:1111", "HSL:2222");
        
        InternalMessages.Bulletin bulletinOther1 = createBulletin(
                InternalMessages.Bulletin.Impact.IRREGULAR_DEPARTURES,
                InternalMessages.Bulletin.Priority.WARNING,
                "HSL:3333", "HSL:4444");
        
        InternalMessages.Bulletin bulletinMassCancellation2 = createBulletin(
                InternalMessages.Bulletin.Impact.CANCELLED,
                InternalMessages.Bulletin.Priority.WARNING,
                "HSL:5555", "HSL:5555");
        
        InternalMessages.Bulletin bulletinOther2 = createBulletin(
                InternalMessages.Bulletin.Impact.RETURNING_TO_NORMAL,
                InternalMessages.Bulletin.Priority.INFO,
                "HSL:6666", "HSL:7777");
        
        InternalMessages.Bulletin bulletinOther3 = createBulletin(
                InternalMessages.Bulletin.Impact.DISRUPTION_ROUTE,
                InternalMessages.Bulletin.Priority.SEVERE,
                "HSL:8888", "HSL:9999");
        
        List<InternalMessages.Bulletin> inputBulletins = new ArrayList<>();
        inputBulletins.add(bulletinMassCancellation1);
        inputBulletins.add(bulletinOther1);
        inputBulletins.add(bulletinMassCancellation2);
        inputBulletins.add(bulletinOther2);
        inputBulletins.add(bulletinOther3);
        return inputBulletins;
    }
    
    private static InternalMessages.Bulletin createBulletin(
            InternalMessages.Bulletin.Impact impact,
            InternalMessages.Bulletin.Priority priority,
            String... routeIds) {
        List<InternalMessages.Bulletin.AffectedEntity> entities = Stream.of(routeIds)
                .map(routeId -> InternalMessages.Bulletin.AffectedEntity.newBuilder().setEntityId(routeId).build())
                .collect(Collectors.toList());
        return InternalMessages.Bulletin.newBuilder()
                .addAllAffectedRoutes(entities)
                .setImpact(impact)
                .setPriority(priority)
                .setLastModifiedUtcMs(System.currentTimeMillis())
                .setValidFromUtcMs(System.currentTimeMillis())
                .setValidToUtcMs(System.currentTimeMillis())
                .build();
    }
}