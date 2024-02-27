package fi.hsl.transitdata.cancellation.util;

import com.github.benmanes.caffeine.cache.Cache;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class CacheUtils {
    
    private static final Logger log = LoggerFactory.getLogger(CacheUtils.class);
    
    /**
     * Using the bulletins cache that will be passed to this method as parameter, it is sorted out which cancellations
     * have been added and/or removed from the previous version of the bulletin. If there is no previous version in the
     * cache, it will be added. If there is a previous version, it is completely replaced.
     * @param bulletinId mass cancellation bulletin identifier
     * @param modifiedCancellationDataList list of cancellation data objects included in the bulletin
     * @param bulletinsCache KEY: bulletinId, VALUE: Map<KEY: tripId, VALUE: cancellationData>
     * @return list of cancellation data objects that haven't been sent yet, and cancellation-of-cancellations if valid
     * time period has been shortened
     */
    public static List<CancellationData> handleBulletinCancellations(
            String bulletinId,
            List<CancellationData> modifiedCancellationDataList,
            Cache<String, Map<String, CancellationData>> bulletinsCache) {
        
        List<CancellationData> cancellationDataList;
        
        // KEY: tripId, VALUE: cancellationData
        Map<String, CancellationData> tripCancellationDataInCache = getTripCancellationMap(bulletinId, bulletinsCache);
        
        // bulletin doesn't exist in the cache
        if (tripCancellationDataInCache == null || tripCancellationDataInCache.isEmpty()) {
            tripCancellationDataInCache = new HashMap<>();
            cancellationDataList = modifiedCancellationDataList;

            for (CancellationData cancellationData : modifiedCancellationDataList) {
                tripCancellationDataInCache.put(cancellationData.getTripId(), cancellationData);
            }
            
            bulletinsCache.put(bulletinId, tripCancellationDataInCache);
            log.info("Added {} new cancellation data objects to bulletins cache", modifiedCancellationDataList.size());
        } else { // previous version of bulletin exists in the cache
            int originalNumberOfCancellationsInCache = tripCancellationDataInCache.keySet().size();
            cancellationDataList = new ArrayList<>();
            List<CancellationData> newCancellationDataList = new ArrayList<>();
            List<CancellationData> cancelledCancellationDataList = new ArrayList<>();
            List<CancellationData> unchangedCancellationDataList = new ArrayList<>();
            
            // KEY: tripId, VALUE: cancellationData
            Map<String, CancellationData> newTripCancellationDataMap = new HashMap<>();
            
            // loop cancellations of new bulletin
            for (CancellationData cancellationData : modifiedCancellationDataList) {
                newTripCancellationDataMap.put(cancellationData.getTripId(), cancellationData);
                
                if (tripCancellationDataInCache.containsKey(cancellationData.getTripId())) {
                    // unchanged cancellation
                    unchangedCancellationDataList.add(cancellationData);
                } else {
                    // new cancellation
                    newCancellationDataList.add(cancellationData);
                }
            }
            cancellationDataList.addAll(newCancellationDataList);
            cancellationDataList.addAll(unchangedCancellationDataList);
            
            // loop cancellations of cache
            for (String tripId : tripCancellationDataInCache.keySet()) {
                if (!newTripCancellationDataMap.containsKey(tripId)) {
                    // cancelled cancellation
                    InternalMessages.TripCancellation tripCancellationToBeCancelled =
                            tripCancellationDataInCache.get(tripId).getPayload();
                    
                    InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
                    builder.setRouteId(tripCancellationToBeCancelled.getRouteId());
                    builder.setDirectionId(tripCancellationToBeCancelled.getDirectionId());
                    builder.setStartDate(tripCancellationToBeCancelled.getStartDate());
                    builder.setStartTime(tripCancellationToBeCancelled.getStartTime());
                    builder.setStatus(InternalMessages.TripCancellation.Status.RUNNING);
                    builder.setSchemaVersion(builder.getSchemaVersion());
                    builder.setTripId(tripCancellationToBeCancelled.getTripId());
                    
                    final InternalMessages.TripCancellation cancelledTripCancellation = builder.build();
                    CancellationData cachedCancellationData = tripCancellationDataInCache.get(tripId);
                    CancellationData cancelledCancellationData = new CancellationData(
                            cancelledTripCancellation,
                            cachedCancellationData.getTimestamp(),
                            cachedCancellationData.getDvjId(),
                            cachedCancellationData.deviationCaseId);
                    cancelledCancellationDataList.add(cancelledCancellationData);
                }
            }
            cancellationDataList.addAll(cancelledCancellationDataList);
            
            bulletinsCache.put(bulletinId, newTripCancellationDataMap);
            log.info("Bulletin modified. Previous version had {} cancellations. New version has {} cancellations: "
                    + "unchanged {}, new {}, cancellations-of-cancellations {}",
                    originalNumberOfCancellationsInCache,
                    modifiedCancellationDataList.size(),
                    unchangedCancellationDataList.size(),
                    newCancellationDataList.size(),
                    cancelledCancellationDataList.size());
        }
        
        // check
        if (getTripCancellationMapNoNull(bulletinId, bulletinsCache).keySet().size() != modifiedCancellationDataList.size()) {
            log.warn("Number of cancellations in bulletin does is not equal to number of cancellations in cache. BulletinId={}", bulletinId);
        }
        
        return cancellationDataList;
    }
    
    /**
     * Thread-safe implementation to get a value from Cafeine cache. This method does not modify the cache.
     * @param bulletinId bulletin identifier
     * @param bulletinsCache KEY: bulletinId, VALUE: Map<KEY: tripId, VALUE: cancellationData>
     * @return map with KEY: tripId, VALUE: cancellationData (or null if no bulletin is found with given bulletin
     * identifier)
     */
    public static Map<String, CancellationData> getTripCancellationMap(
            String bulletinId, Cache<String, Map<String, CancellationData>> bulletinsCache) {
        ConcurrentMap<String, Map<String, CancellationData>> cacheAsMap = bulletinsCache.asMap();
        return cacheAsMap.get(bulletinId);
    }
    
    /**
     * Same as getTripCancellationMap method except that this method does not return null if no value is found with the given
     * bulletin identifier. This method returns an empty map in this case.
     * @param bulletinId bulletin identifier
     * @param bulletinsCache KEY: bulletinId, VALUE: Map<KEY: tripId, VALUE: cancellationData>
     * @return map with KEY: tripId, VALUE: cancellationData
     */
    public static Map<String, CancellationData> getTripCancellationMapNoNull(
            String bulletinId, Cache<String, Map<String, CancellationData>> bulletinsCache) {
        Map<String, CancellationData> tripCancellationMap = getTripCancellationMap(bulletinId, bulletinsCache);
        
        if (tripCancellationMap == null) {
            return new HashMap<>();
        }
        
        return tripCancellationMap;
    }
}
