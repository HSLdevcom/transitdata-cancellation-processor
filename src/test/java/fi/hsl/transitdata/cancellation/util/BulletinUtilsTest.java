package fi.hsl.transitdata.cancellation.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.AlertHandler;
import fi.hsl.transitdata.cancellation.domain.CancellationData;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

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
        
        assertTrue(outputBulletins.size() == 2);
    }
    
    @Test
    public void parseCancellationDataFromBulletins() {
        try (MockedStatic<TripUtils> tripUtils = Mockito.mockStatic(TripUtils.class)) {
            List<CancellationData> cancellationData = BulletinUtils.parseCancellationDataFromBulletins(
                    initializeTestTrips(tripUtils), "");
            assertEquals(3, cancellationData.size());
            assertEquals("4611", cancellationData.get(0).getPayload().getRouteId());
            assertEquals("4611", cancellationData.get(1).getPayload().getRouteId());
            assertEquals("1079", cancellationData.get(2).getPayload().getRouteId());
            
            assertEquals("HSL:4611_20240102_Ti_2_1415", cancellationData.get(0).getPayload().getTripId());
            assertEquals("HSL:4611_20240102_Ti_2_1515", cancellationData.get(1).getPayload().getTripId());
            assertEquals("HSL:1079_20240102_La_1_0734", cancellationData.get(2).getPayload().getTripId());
            
            assertEquals("14:15:00", cancellationData.get(0).getPayload().getStartTime());
            assertEquals("15:15:00", cancellationData.get(1).getPayload().getStartTime());
            assertEquals("07:34:00", cancellationData.get(2).getPayload().getStartTime());
        }
    }
    
    private static List<InternalMessages.Bulletin> initializeTestTrips(MockedStatic<TripUtils> tripUtils) {
        List<InternalMessages.TripInfo> trips = new ArrayList<>();
        trips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240102_Ti_2_1415", "20240102", "1415", 1));
        trips.add(TripUtilsTest.createTripInfo("HSL:4611", "HSL:4611_20240102_Ti_2_1515", "20240102", "1515", 1));
        trips.add(TripUtilsTest.createTripInfo("HSL:1079", "HSL:1079_20240102_La_1_0734", "20240102", "0734", 2));
        
        tripUtils.when(() -> TripUtils.getTripInfos(
                any(List.class), any(LocalDateTime.class), any(LocalDateTime.class), anyString())).thenReturn(trips);
        
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
    
    @Test
    public void testProcessBulletinCancellations_NewBulletin_NoOtherBulletins() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache = createEmptyBulletinsCache();
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache = createEmptycancellationStatusCache();
        /* TODO: fix
        List<CancellationData> cancellationsToBeSent = BulletinUtils.processBulletinCancellations(
                "bulletin_00", createCancellations(), bulletinsCache, cancellationStatusCache);
        
        assertEquals(10, cancellationsToBeSent.size());
        assertEquals(10, bulletinsCache.getIfPresent("bulletin_00").size());
        assertEquals(10, cancellationStatusCache.asMap().keySet().size());
         */
    }
    
    @Test
    public void testProcessBulletinCancellations_NewBulletin_OtherBulletinsExists() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache = createPopulatedBulletinsCache();
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache = createPopulatedcancellationStatusCache();
        /* TODO: fix
        List<CancellationData> cancellationsToBeSent = BulletinUtils.processBulletinCancellations(
                "bulletin_00", createCancellations(), bulletinsCache, cancellationStatusCache);
        
        assertEquals(7, cancellationsToBeSent.size());
        //assertEquals(10, bulletinsCache.stats().loadCount());
        //assertEquals(10, cancellationStatusCache.stats().loadCount());
         */
    }
    
    @Test
    public void testBulletinExistsInCache() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache = createPopulatedBulletinsCache();
        
        assertTrue(BulletinUtils.bulletinExistsInCache("bulletin_01", bulletinsCache));
        assertTrue(BulletinUtils.bulletinExistsInCache("bulletin_02", bulletinsCache));
        assertFalse(BulletinUtils.bulletinExistsInCache("bulletin_999", bulletinsCache));
    }
    
    @Test
    public void testHasActiveCancellationInCache() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache = createPopulatedcancellationStatusCache();
        
        assertTrue(BulletinUtils.hasActiveCancellationInCache("trip_07", cancellationStatusCache));
        assertFalse(BulletinUtils.hasActiveCancellationInCache("trip_09", cancellationStatusCache));
        assertTrue(BulletinUtils.hasActiveCancellationInCache("trip_12", cancellationStatusCache));
        assertFalse(BulletinUtils.hasActiveCancellationInCache("trip_999", cancellationStatusCache));
    }
    
    @Test
    public void testProcessBulletinCancellations_BulletinValidUntilShortened_NoOtherBulletins() {
    
    }
    
    @Test
    public void testProcessBulletinCancellations_BulletinValidUntilShortened_OtherBulletinsExists() {
    
    }
    
    @Test
    public void testProcessBulletinCancellations_BulletinValidUntilExtended_NoOtherBulletins() {
    
    }
    
    @Test
    public void testProcessBulletinCancellations_BulletinValidUntilExtended_OtherBulletinsExists() {
    
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
        InternalMessages.Bulletin bulletin = InternalMessages.Bulletin.newBuilder()
                .addAllAffectedRoutes(entities)
                .setImpact(impact)
                .setPriority(priority)
                .setLastModifiedUtcMs(System.currentTimeMillis())
                .setValidFromUtcMs(System.currentTimeMillis())
                .setValidToUtcMs(System.currentTimeMillis())
                .build();
        
        return bulletin;
    }
    
    private static List<CancellationData> createCancellations() {
        List<CancellationData> cancellations = new ArrayList<>();
        cancellations.add(createCancellation("trip_00"));
        cancellations.add(createCancellation("trip_01"));
        cancellations.add(createCancellation("trip_02"));
        cancellations.add(createCancellation("trip_03"));
        cancellations.add(createCancellation("trip_04"));
        cancellations.add(createCancellation("trip_05"));
        cancellations.add(createCancellation("trip_06"));
        cancellations.add(createCancellation("trip_07"));
        cancellations.add(createCancellation("trip_08"));
        cancellations.add(createCancellation("trip_09"));
        return cancellations;
    }
    
    private static CancellationData createCancellation(String tripId) {
        return new CancellationData(null, System.currentTimeMillis(), null, 1000, tripId);
    }
    
    private static Cache<String, Map<String, InternalMessages.TripCancellation.Status>> createEmptyBulletinsCache() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache;
        
        bulletinsCache = Caffeine.newBuilder()
                .expireAfterAccess(AlertHandler.CACHE_DURATION)
                .build(key -> new HashMap<>());
        
        return bulletinsCache;
    }
    
    private static Cache<String, Map<String, InternalMessages.TripCancellation.Status>> createEmptycancellationStatusCache() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache;
        
        cancellationStatusCache = Caffeine.newBuilder()
                .expireAfterAccess(AlertHandler.CACHE_DURATION)
                .build(key -> new HashMap<>());
        
        return cancellationStatusCache;
    }
    
    private static Cache<String, Map<String, InternalMessages.TripCancellation.Status>> createPopulatedBulletinsCache() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache;
        
        bulletinsCache = Caffeine.newBuilder()
                .expireAfterAccess(AlertHandler.CACHE_DURATION)
                .build(key -> new HashMap<>());
        
        Map<String, InternalMessages.TripCancellation.Status> tripStatusMap01 = new HashMap<>();
        tripStatusMap01.put("trip_07", InternalMessages.TripCancellation.Status.CANCELED);
        tripStatusMap01.put("trip_08", InternalMessages.TripCancellation.Status.CANCELED);
        tripStatusMap01.put("trip_09", InternalMessages.TripCancellation.Status.RUNNING);
        tripStatusMap01.put("trip_10", InternalMessages.TripCancellation.Status.CANCELED);
        tripStatusMap01.put("trip_11", InternalMessages.TripCancellation.Status.CANCELED);
        tripStatusMap01.put("trip_12", InternalMessages.TripCancellation.Status.CANCELED);
        
        // KEY: bulletinId, VALUE: Map<KEY: tripId, VALUE: tripCancellationStatus>
        bulletinsCache.put("bulletin_01", tripStatusMap01);
        
        Map<String, InternalMessages.TripCancellation.Status> tripStatusMap02 = new HashMap<>();
        tripStatusMap02.put("trip_11", InternalMessages.TripCancellation.Status.CANCELED);
        tripStatusMap02.put("trip_12", InternalMessages.TripCancellation.Status.RUNNING);
        tripStatusMap02.put("trip_13", InternalMessages.TripCancellation.Status.CANCELED);
        tripStatusMap02.put("trip_14", InternalMessages.TripCancellation.Status.CANCELED);
        
        // KEY: bulletinId, VALUE: Map<KEY: tripId, VALUE: tripCancellationStatus>
        bulletinsCache.put("bulletin_02", tripStatusMap02);
        
        return bulletinsCache;
    }
    
    private static Cache<String, Map<String, InternalMessages.TripCancellation.Status>> createPopulatedcancellationStatusCache() {
        Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache;
        
        cancellationStatusCache = Caffeine.newBuilder()
                .expireAfterAccess(AlertHandler.CACHE_DURATION)
                .build(key -> new HashMap<>());
        
        Map<String, InternalMessages.TripCancellation.Status> bulletinStatusMap01Canceled = new HashMap<>();
        bulletinStatusMap01Canceled.put("bulletin_01", InternalMessages.TripCancellation.Status.CANCELED);
        
        Map<String, InternalMessages.TripCancellation.Status> bulletinStatusMap01Running = new HashMap<>();
        bulletinStatusMap01Running.put("bulletin_01", InternalMessages.TripCancellation.Status.RUNNING);
        
        Map<String, InternalMessages.TripCancellation.Status> bulletinStatusMap0102Canceled = new HashMap<>();
        bulletinStatusMap0102Canceled.put("bulletin_01", InternalMessages.TripCancellation.Status.CANCELED);
        bulletinStatusMap0102Canceled.put("bulletin_02", InternalMessages.TripCancellation.Status.CANCELED);
        
        Map<String, InternalMessages.TripCancellation.Status> bulletinStatusMap0102Both = new HashMap<>();
        bulletinStatusMap0102Both.put("bulletin_01", InternalMessages.TripCancellation.Status.CANCELED);
        bulletinStatusMap0102Both.put("bulletin_02", InternalMessages.TripCancellation.Status.RUNNING);
        
        Map<String, InternalMessages.TripCancellation.Status> bulletinStatusMap02Canceled = new HashMap<>();
        bulletinStatusMap02Canceled.put("bulletin_02", InternalMessages.TripCancellation.Status.CANCELED);
        
        // KEY: tripId, VALUE: Map<KEY: bulletinId, VALUE: tripCancellationStatus>
        cancellationStatusCache.put("trip_07", bulletinStatusMap01Canceled);
        cancellationStatusCache.put("trip_08", bulletinStatusMap01Canceled);
        cancellationStatusCache.put("trip_09", bulletinStatusMap01Running);
        cancellationStatusCache.put("trip_10", bulletinStatusMap01Canceled);
        cancellationStatusCache.put("trip_11", bulletinStatusMap0102Canceled);
        cancellationStatusCache.put("trip_12", bulletinStatusMap0102Both);
        cancellationStatusCache.put("trip_13", bulletinStatusMap02Canceled);
        cancellationStatusCache.put("trip_14", bulletinStatusMap02Canceled);
        
        return cancellationStatusCache;
    }
}