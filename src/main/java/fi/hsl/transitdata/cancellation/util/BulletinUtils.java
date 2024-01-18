package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
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
    private static List<CancellationData> createTripCancellations(InternalMessages.Bulletin massCancellation, String digitransitDeveloperApiUri) {
        // TODO: this implementation is not final
        List<CancellationData> tripCancellations = new ArrayList<>();
        
        LocalDateTime validFrom = Instant.ofEpochMilli(
                massCancellation.getValidFromUtcMs()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        
        LocalDateTime validTo = Instant.ofEpochMilli(
                massCancellation.getValidToUtcMs()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        
        List<String> routeIds = massCancellation.getAffectedRoutesList().stream().
                map(x -> x.getEntityId()).collect(Collectors.toList());
        
        for (InternalMessages.TripInfo trip : TripUtils.getTripInfos(routeIds, validFrom, validTo, digitransitDeveloperApiUri)) {
            InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
            long deviationCaseId = InternalMessages.TripCancellation.DeviationCasesType.CANCEL_DEPARTURE.getNumber();
            //long deviationCaseId = trip.getDeviationCaseId();
            //builder.setDeviationCaseId(deviationCaseId);
            builder.setRouteId(trip.getRouteId());
            builder.setDirectionId(trip.getDirectionId());
            builder.setStartDate(trip.getOperatingDay());
            builder.setStartTime(trip.getStartTime());
            builder.setStatus(InternalMessages.TripCancellation.Status.RUNNING);
            builder.setSchemaVersion(builder.getSchemaVersion());
            String dvjId = trip.getTripId();
            //String dvjId = Long.toString(trip.getDvjId());
            builder.setTripId(dvjId); // Ei ehkä tarvita, ehkä joku muu id cacheen
            //builder.setDeviationCasesType(InternalMessages.TripCancellation.DeviationCasesType.valueOf(trip.getDeviationCasesType()));
            //builder.setAffectedDeparturesType(null);
            //builder.setTitle(null);
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
            
            CancellationData data = new CancellationData(cancellation, massCancellation.getLastModifiedUtcMs(), dvjId, deviationCaseId);
            tripCancellations.add(data);
        }
        
        return tripCancellations;
    }
}