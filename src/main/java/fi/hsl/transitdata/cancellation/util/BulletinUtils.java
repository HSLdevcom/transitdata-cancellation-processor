package fi.hsl.transitdata.cancellation.util;

import com.github.benmanes.caffeine.cache.Cache;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BulletinUtils {

    public static List<InternalMessages.Bulletin> filterMassCancellationsFromBulletins(List<InternalMessages.Bulletin> bulletins) {
        return bulletins.stream().filter(
                        bulletin ->
                                bulletin.getImpact() == InternalMessages.Bulletin.Impact.CANCELLED &&
                                        bulletin.getPriority() == InternalMessages.Bulletin.Priority.WARNING)
                .collect(Collectors.toList());
    }
    
    public static List<CancellationData> parseCancellationDataFromBulletins(List<InternalMessages.Bulletin> massCancellations, String digitransitDeveloperApiUri) {
        List<CancellationData> tripCancellations = massCancellations.stream().
                flatMap(mc -> createTripCancellations(mc, digitransitDeveloperApiUri).stream()).collect(Collectors.toList());
        return tripCancellations;
    }
    
    // One cancellation contains one trip
    // A route consists of many trips
    public static List<CancellationData> createTripCancellations(InternalMessages.Bulletin massCancellation, String digitransitDeveloperApiUri) {
        // TODO: this implementation is not final
        List<CancellationData> tripCancellations = new ArrayList<>();
        
        LocalDateTime validFrom = Instant.ofEpochMilli(
                massCancellation.getValidFromUtcMs()).atZone(ZoneId.of("Europe/Helsinki")).toLocalDateTime();
        
        LocalDateTime validTo = Instant.ofEpochMilli(
                massCancellation.getValidToUtcMs()).atZone(ZoneId.of("Europe/Helsinki")).toLocalDateTime();
        
        List<String> routeIds = massCancellation.getAffectedRoutesList().stream().
                map(x -> x.getEntityId()).collect(Collectors.toList());
        
        for (InternalMessages.TripInfo trip : TripUtils.getTripInfos(routeIds, validFrom, validTo, digitransitDeveloperApiUri)) {
            InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
            long deviationCaseId = InternalMessages.TripCancellation.DeviationCasesType.CANCEL_DEPARTURE.getNumber();
            //long deviationCaseId = trip.getDeviationCaseId();
            //builder.setDeviationCaseId(deviationCaseId);
            builder.setRouteId(removeHSLPrefixFromRouteId(trip.getRouteId()));
            builder.setDirectionId(trip.getDirectionId() + 1);
            builder.setStartDate(trip.getOperatingDay());
            builder.setStartTime(formatTime(trip.getStartTime()));
            builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);
            builder.setSchemaVersion(builder.getSchemaVersion());
            String dvjId = trip.getTripId();
            //String dvjId = Long.toString(trip.getDvjId());
            builder.setTripId(dvjId); // Ei ehkä tarvita, ehkä joku muu id cacheen
            //builder.setDeviationCasesType(InternalMessages.TripCancellation.DeviationCasesType.valueOf(trip.getDeviationCasesType()));
            //builder.setAffectedDeparturesType(null);
            builder.setTitle(massCancellation.getBulletinId());
            //builder.setDescription(null);
            //builder.setCategory(null);
            //builder.setSubCategory(null);
            
            final InternalMessages.TripCancellation cancellation = builder.build();
            /*
            //Date timestamp = trip.getAffectedDeparturesLastModified();
            Date timestamp = new Date();
            
            Optional<Long> epochTimestamp = TimeUtils.toUtcEpochMs(timestamp.toString());
            if (epochTimestamp.isEmpty()) {
                throw new RuntimeException("Failed to parse epoch timestamp from resultset: " + timestamp);
                //log.error("Failed to parse epoch timestamp from resultset: {}", timestamp);
            } else {
                CancellationData data = new CancellationData(cancellation, epochTimestamp.get(), dvjId, deviationCaseId);
                tripCancellations.add(data);
            }
             */
            
            CancellationData data = new CancellationData(cancellation, massCancellation.getLastModifiedUtcMs(), dvjId,
                    deviationCaseId, trip.getTripId());
            tripCancellations.add(data);
        }
        
        return tripCancellations;
    }
    
    /**
     * Returns routeId without 'HSL:' prefix
     */
    static String removeHSLPrefixFromRouteId(String routeId) {
        if (routeId.startsWith("HSL:")) {
            return routeId.substring(4);
        }
        return routeId;
    }
    
    /**
     * Format time for pulsar message.
     * @param timeAsString time in format 'HHMM' (e.g. '0742')
     * @return time in format 'HH:MM:SS' (e.g. '07:42:00')
     */
    static String formatTime(String timeAsString) {
        String hours = timeAsString.substring(0, 2);
        String minutes = timeAsString.substring(2, 4);
        return hours + ":" + minutes + ":00";
    }
    
    /**
     * Returns cancellations that can be sent as Pulsar message. Cancellations that have not been sent yet are returned.
     *
     * @param bulletinId
     * @param bulletinCancellations
     * @param bulletinsCache
     * @param cancellationStatusCache
     * @return
     */
    public static List<CancellationData> processBulletinCancellations(
            String bulletinId,
            List<CancellationData> bulletinCancellations,
            Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache,
            Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache,
            Cache<String, CancellationData> cancellationsCache) {
        
        List<CancellationData> cancellationsToBeReturned = new ArrayList<>();
        
        // If the bulletin doesn't already exist in the cache:
        // 1. all cancellations can be added to the cache
        // 2. those cancellations that have not been already sent will be returned
        if (!bulletinExistsInCache(bulletinId, bulletinsCache)) {
            // KEY: tripId, VALUE: tripCancellationStatus
            Map<String, InternalMessages.TripCancellation.Status> tripStatusMap = new HashMap<>();
            
            for (CancellationData cancellation : bulletinCancellations) {
                tripStatusMap.put(cancellation.getTripId(), InternalMessages.TripCancellation.Status.CANCELED);
                cancellationsCache.put(cancellation.getTripId(), cancellation);
                
                if (!hasActiveCancellationInCache(cancellation.getTripId(), cancellationStatusCache)) {
                    cancellationsToBeReturned.add(cancellation);
                }
            }
            
            bulletinsCache.put(bulletinId, tripStatusMap);
            return cancellationsToBeReturned;
        }
        
        // If the bulletin already exists in the cache, check if it contains all the same cancellations
        List<CancellationData> newCancellations = new ArrayList<>();
        int numberOfSameCancellations = 0;
        
        // KEY: tripId, VALUE: tripCancellationStatus
        Map<String, InternalMessages.TripCancellation.Status> tripStatusMap = bulletinsCache.getIfPresent(bulletinId);
        
        // KEY: tripId, VALUE: cancellationData
        Map<String, CancellationData> bulletinCancellationsMap = new HashMap<>();
        
        for (CancellationData cancellation : bulletinCancellations) {
            bulletinCancellationsMap.put(cancellation.getTripId(), cancellation);
            InternalMessages.TripCancellation.Status status = tripStatusMap.get(cancellation.getTripId());
            
            if (status == null || status == InternalMessages.TripCancellation.Status.RUNNING) {
                newCancellations.add(cancellation);
            } else {
                numberOfSameCancellations++;
            }
        }
        
        // If there are cancellations in the cache that are missing from the bulletin,
        // it means that their status is cancellation-of-cancellation
        List<String> cancelledCancellations = new ArrayList<>();
        
        for (String tripIdFromCache : tripStatusMap.keySet()) {
            if (!bulletinCancellationsMap.containsKey(tripIdFromCache)) {
                cancelledCancellations.add(tripIdFromCache);
                tripStatusMap.put(tripIdFromCache, InternalMessages.TripCancellation.Status.RUNNING);
                
                if (!hasActiveCancellationInCache(tripIdFromCache, cancellationStatusCache)) {
                
                }
                //TODO
                // z) Return those cancellation-of-cancellations that have no active cancellations in the cache.
                // Flag cancellation-of-cancellation is set to true(?)
            }
        }
        
        // a) If yes, do nothing, perhaps print a log info message
        // b) If there are cancellations in the cache that are missing from the bulletin,
        // it means that their status is cancellation-of-cancellation. That is:
        // 1. They will be marked as such in the cache
        // 2. Go to z)
        // c) If there are cancellations in the bulletin that are missing from the cache
        // 1. They will be added to the cache
        // 2. Go to y)
        
        // y) Those cancellations that have not been already sent will be returned
        // z) Return those cancellation-of-cancellations that have no active cancellations in the cache.
        // Flag cancellation-of-cancellation is set to true(?)
        
        return cancellationsToBeReturned;
    }
    
    static boolean bulletinExistsInCache(
            String bulletinId, Cache<String, Map<String, InternalMessages.TripCancellation.Status>> bulletinsCache) {
        return bulletinsCache.getIfPresent(bulletinId) == null ? false : true;
    }
    
    static boolean hasActiveCancellationInCache(
            String tripId, Cache<String, Map<String, InternalMessages.TripCancellation.Status>> cancellationStatusCache) {
        
        Map<String, InternalMessages.TripCancellation.Status> statusInBulletinMap = cancellationStatusCache.getIfPresent(tripId);
        
        if (statusInBulletinMap == null) {
            return false;
        }
        
        if (statusInBulletinMap.containsValue(InternalMessages.TripCancellation.Status.CANCELED)) {
            return true;
        }
        
        return false;
    }
}