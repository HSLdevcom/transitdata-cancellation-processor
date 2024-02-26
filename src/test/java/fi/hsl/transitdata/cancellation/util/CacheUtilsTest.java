package fi.hsl.transitdata.cancellation.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.Assert.*;

public class CacheUtilsTest {

    private List<InternalMessages.TripCancellation> buildTripCancellations() {
        InternalMessages.TripCancellation tripCancellation1 = InternalMessages.TripCancellation.newBuilder().setTripId("trip1").setSchemaVersion(1).setStatus(InternalMessages.TripCancellation.Status.CANCELED).build();
        InternalMessages.TripCancellation tripCancellation2 = InternalMessages.TripCancellation.newBuilder().setTripId("trip2").setSchemaVersion(1).setStatus(InternalMessages.TripCancellation.Status.CANCELED).build();

        List<InternalMessages.TripCancellation> tripCancellations = new ArrayList<>();
        tripCancellations.add(tripCancellation1);
        tripCancellations.add(tripCancellation2);

        return tripCancellations;
    }

    private List<CancellationData> buildCancellationDataList() {
        List<CancellationData> cancellationDataList = new ArrayList<>();

        for (InternalMessages.TripCancellation tripCancellation : buildTripCancellations()) {
            cancellationDataList.add(getSampleCancellation(tripCancellation));
        }

        return cancellationDataList;
    }
    
    @NotNull
    private static CancellationData getSampleCancellation(InternalMessages.TripCancellation tripCancellation) {
        return new CancellationData(tripCancellation, 1706616017, "none", 123);
    }
    
    @Test
    public void testSingleCancellationDataIsSavedToCache() {
        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);

        assertTrue(bulletinsCache.getIfPresent("bulletin1").containsKey("trip1"));
    }

    @Test
    public void testModifiedCancellationWithCancelledCancellationIsCorrectlySaved() {

        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);

        ArrayList<CancellationData> cancelledBulletin = new ArrayList<>();
        cancelledBulletin.add(buildCancellationDataList().get(0)); // trip1

        CacheUtils.handleBulletinCancellations("bulletin1", cancelledBulletin, bulletinsCache);

        assertFalse(bulletinsCache.getIfPresent("bulletin1").containsKey("trip2"));
        assertTrue(bulletinsCache.getIfPresent("bulletin1").containsKey("trip1"));
    }
    
    @Test
    public void testModifiedCancellationWithAddedCancellationIsCorrectlySaved() {
        InternalMessages.TripCancellation tripCancellation3 = InternalMessages.TripCancellation.newBuilder().setTripId("trip3").setSchemaVersion(1).setStatus(InternalMessages.TripCancellation.Status.CANCELED).build();
        
        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);
        
        ArrayList<CancellationData> cancelledBulletin = new ArrayList<>();
        cancelledBulletin.add(buildCancellationDataList().get(0)); // trip1
        cancelledBulletin.add(buildCancellationDataList().get(1)); // trip2
        cancelledBulletin.add(getSampleCancellation(tripCancellation3)); // added
        
        CacheUtils.handleBulletinCancellations("bulletin1", cancelledBulletin, bulletinsCache);
        
        assertTrue(bulletinsCache.getIfPresent("bulletin1").containsKey("trip1"));
        assertTrue(bulletinsCache.getIfPresent("bulletin1").containsKey("trip2"));
        assertTrue(bulletinsCache.getIfPresent("bulletin1").containsKey("trip3"));
    }
    
    @Test
    public void testGetTripCancellationMapFromCache() {
        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);
        assertNull(CacheUtils.getTripCancellationMap("shouldNotExist", bulletinsCache));
        assertTrue(CacheUtils.getTripCancellationMap("bulletin1", bulletinsCache).containsKey("trip1"));
        assertTrue(CacheUtils.getTripCancellationMap("bulletin1", bulletinsCache).containsKey("trip2"));
    }
    
    @Test
    public void testGetTripCancellationMapNoNullFromCache() {
        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);
        assertEquals(0, CacheUtils.getTripCancellationMapNoNull("shouldNotExist", bulletinsCache).size());
        assertTrue(CacheUtils.getTripCancellationMap("bulletin1", bulletinsCache).containsKey("trip1"));
        assertTrue(CacheUtils.getTripCancellationMap("bulletin1", bulletinsCache).containsKey("trip2"));
    }
}