package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BulletinUtils {
    
    private static final Logger log = LoggerFactory.getLogger(BulletinUtils.class);

    public static List<InternalMessages.Bulletin> filterMassCancellationsFromBulletins(List<InternalMessages.Bulletin> bulletins) {
        return bulletins.stream().filter(
                        bulletin ->
                                bulletin.getImpact() == InternalMessages.Bulletin.Impact.CANCELLED &&
                                        bulletin.getPriority() == InternalMessages.Bulletin.Priority.WARNING)
                .collect(Collectors.toList());
    }
    
    // One cancellation contains one trip
    // A route consists of many trips
    public static List<CancellationData> createTripCancellations(InternalMessages.Bulletin massCancellation, String digitransitDeveloperApiUri) {
        List<CancellationData> tripCancellations = new ArrayList<>();
        
        LocalDateTime validFrom = Instant.ofEpochMilli(
                massCancellation.getValidFromUtcMs()).atZone(ZoneId.of("Europe/Helsinki")).toLocalDateTime();
        
        LocalDateTime validTo = Instant.ofEpochMilli(
                massCancellation.getValidToUtcMs()).atZone(ZoneId.of("Europe/Helsinki")).toLocalDateTime();
        
        List<String> routeIds = massCancellation.getAffectedRoutesList().stream().
                map(InternalMessages.Bulletin.AffectedEntity::getEntityId).collect(Collectors.toList());
        
        for (InternalMessages.TripInfo trip : TripUtils.getTripInfos(routeIds, validFrom, validTo, digitransitDeveloperApiUri)) {
            InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
            long deviationCaseId = InternalMessages.TripCancellation.DeviationCasesType.CANCEL_DEPARTURE.getNumber();
            builder.setRouteId(removeHSLPrefixFromRouteId(trip.getRouteId()));
            builder.setDirectionId(trip.getDirectionId() + 1);
            builder.setStartDate(trip.getOperatingDay());
            builder.setStartTime(formatTime(trip.getStartTime()));
            builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);
            builder.setSchemaVersion(builder.getSchemaVersion());
            String dvjId = trip.getTripId();
            builder.setTripId(dvjId);
            builder.setTitle(massCancellation.getBulletinId());
            
            final InternalMessages.TripCancellation cancellation = builder.build();
            
            CancellationData data = new CancellationData(cancellation, massCancellation.getLastModifiedUtcMs(), dvjId, deviationCaseId);
            tripCancellations.add(data);
        }
        
        log.info("Added {} cancellations from mass cancellation bulletin.{}", tripCancellations.size(), getBulletinLog(massCancellation));
        
        Set<String> originalRouteIdsSet = new HashSet<>(routeIds);
        java.util.Set<String> tripRouteIdsSet = tripCancellations.stream().map(x -> x.getPayload().getRouteId()).collect(Collectors.toSet());
        
        if (originalRouteIdsSet.size() > tripRouteIdsSet.size()) {
            Set<String> difference = findDifference(originalRouteIdsSet, tripRouteIdsSet);
            log.warn("Bulletin id: {}. No trips found for these routes: {}", massCancellation.getBulletinId(), difference);
        }
        
        return tripCancellations;
    }
    
    private static Set<String> findDifference(Set<String> setA, Set<String> setB) {
        // Create a new set to store the difference
        Set<String> differenceSet = new HashSet<>();
        
        // Iterate through each element in setA
        for (String element : setA) {
            // If the element is not present in setB, add it to the difference set
            if (!setB.contains(element)) {
                differenceSet.add(element);
            }
        }
        
        return differenceSet;
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
    
    public static String getBulletinLog(InternalMessages.Bulletin massCancellation) {
        StringBuilder bulletinLog = new StringBuilder(" BULLETIN");
        
        LocalDateTime validFrom = Instant.ofEpochMilli(
                massCancellation.getValidFromUtcMs()).atZone(ZoneId.of("Europe/Helsinki")).toLocalDateTime();
        
        LocalDateTime validTo = Instant.ofEpochMilli(
                massCancellation.getValidToUtcMs()).atZone(ZoneId.of("Europe/Helsinki")).toLocalDateTime();
        
        bulletinLog.append(" Id: ").
                append(massCancellation.getBulletinId()).
                append(", Valid from: ").append(validFrom).
                append(", Valid to: ").append(validTo).
                append(", Affected routes: ").
                append(massCancellation.getAffectedRoutesList());
        
        return bulletinLog.toString();
    }
}