package fi.hsl.transitdata.cancellation.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;
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
            cancellationDataList.add(new CancellationData(tripCancellation, 1706616017, "none", 123));
        }

        return cancellationDataList;
    }
    @Test
    public void singleCancellationDataIsSavedToCache() {
        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);

        assertTrue(Objects.requireNonNull(bulletinsCache.getIfPresent("bulletin1")).containsKey("trip1"));
    }

    @Test
    public void modifiedCancellationIsCorrectlySaved() {

        Cache<String, Map<String, CancellationData>> bulletinsCache =  Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(4)).build(key -> new HashMap<>());
        CacheUtils.handleBulletinCancellations("bulletin1", buildCancellationDataList() , bulletinsCache);

        ArrayList<CancellationData> cancelledBulletin = new ArrayList<>();
        cancelledBulletin.add(buildCancellationDataList().get(0)); // trip1

        CacheUtils.handleBulletinCancellations("bulletin1", cancelledBulletin, bulletinsCache);

        assertFalse(Objects.requireNonNull(bulletinsCache.getIfPresent("bulletin1")).containsKey("trip2"));
        assertTrue(Objects.requireNonNull(bulletinsCache.getIfPresent("bulletin1")).containsKey("trip1"));
    }
}