package fi.hsl.transitdata.cancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class AlertHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(AlertHandler.class);

    public static final String AGENCY_ENTITY_SELECTOR = "HSL";

    private final Consumer<byte[]> consumer;
    private final Producer<byte[]> producer;

    private final boolean globalNoServiceAlerts;

    public AlertHandler(final PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getSingleProducer();

        globalNoServiceAlerts = context.getConfig().getBoolean("application.enableGlobalNoServiceAlerts");
    }
    
    @Override
    public void handleMessage(final Message message) {
        try {
            List<CancellationData> cancellationDataList = new ArrayList<>();
            
            if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert)) {
                InternalMessages.ServiceAlert serviceAlert = InternalMessages.ServiceAlert.parseFrom(message.getData());
                List<InternalMessages.Bulletin> massCancellations = filterBulletins(serviceAlert.getBulletinsList());
                cancellationDataList = getCancellationData(massCancellations);
            } else if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation)) {
                InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.parseFrom(message.getData());
                CancellationData data = new CancellationData(tripCancellation, message.getEventTime(), message.getKey(), -1);
                cancellationDataList.add(data);
            } else {
                throw new Exception("Invalid protobuf schema");
            }
            
            // TODO: is cache of trip cancellations needed?
            // Cachesta voidaan katsoa onko ko. peruutus jo lähetetty
            // Jos peruutuksen linjoja muutetaan, voidaan cachesta katsoa mitkä lähdöt voidaan ottaa pois peruutuksista
            sendCancellations(cancellationDataList);
        } catch (final Exception e) {
            log.error("Exception while handling message", e);
        } finally {
            ack(message.getMessageId());
        }
    }
    
    // We are interested in mass cancellations only. Other types of bulletins are filtered out.
    List<InternalMessages.Bulletin> filterBulletins(List<InternalMessages.Bulletin> bulletins) {
        return bulletins.stream().filter(
                        bulletin ->
                                bulletin.getImpact() == InternalMessages.Bulletin.Impact.CANCELLED &&
                                        bulletin.getPriority() == InternalMessages.Bulletin.Priority.WARNING)
                .collect(Collectors.toList());
    }
    
    @NotNull
    private List<CancellationData> getCancellationData(List<InternalMessages.Bulletin> massCancellations) {
        List<CancellationData> tripCancellations = massCancellations.stream().
                flatMap(mc -> createTripCancellations(mc).stream()).collect(Collectors.toList());
        return tripCancellations;
    }
    
    List<CancellationData> createTripCancellations(InternalMessages.Bulletin massCancellation) {
        // TODO: this implementation is not final
        List<CancellationData> tripCancellations = new ArrayList<>();
        
        for (Trip trip : findTrips()) {
            InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
            long deviationCaseId = trip.getDeviationCaseId();
            builder.setDeviationCaseId(deviationCaseId);
            builder.setRouteId(trip.routeName);
            builder.setDirectionId(trip.getDirection());
            builder.setStartDate(trip.getOperatingDay());
            builder.setStartTime(trip.getStartTime());
            builder.setStatus(InternalMessages.TripCancellation.Status.RUNNING);
            builder.setSchemaVersion(builder.getSchemaVersion());
            String dvjId = Long.toString(trip.getDvjId());
            builder.setTripId(dvjId); // Ei ehkä tarvita, ehkä joku muu id cacheen
            builder.setDeviationCasesType(InternalMessages.TripCancellation.DeviationCasesType.valueOf(trip.getDeviationCasesType()));
            builder.setAffectedDeparturesType(null);
            builder.setTitle(null);
            builder.setDescription(null);
            builder.setCategory(null);
            builder.setSubCategory(null);
    
            final InternalMessages.TripCancellation cancellation = builder.build();
            Date timestamp = trip.getAffectedDeparturesLastModified();
            
            Optional<Long> epochTimestamp = toUtcEpochMs(timestamp.toString());
            if (epochTimestamp.isEmpty()) {
                log.error("Failed to parse epoch timestamp from resultset: {}", timestamp);
            } else {
                CancellationData data = new CancellationData(cancellation, epochTimestamp.get(), dvjId, deviationCaseId);
                tripCancellations.add(data);
            }
        }
        
        return tripCancellations;
    }
    
    List<Trip> findTrips() {
        // Input: id, pvm, alkuaika, loppuaika
        // Output: id, pvm, lähtöaika, suunta (0 ja 1, tai 1 ja 2)
        // GraphQL-haku, digitransitin api key secreteihin
        return new ArrayList<>();
    }
    
    class Trip {
        long dvjId;
        long deviationCaseId;
        String routeName;
        int direction;
        String operatingDay;
        String startTime;
        String affectedDeparturesStatus;
        String deviationCasesType;
        
        Date AffectedDeparturesLastModified;
    
        public long getDvjId() {
            return dvjId;
        }
    
        public void setDvjId(long dvjId) {
            this.dvjId = dvjId;
        }
    
        public long getDeviationCaseId() {
            return deviationCaseId;
        }
    
        public void setDeviationCaseId(long deviationCaseId) {
            this.deviationCaseId = deviationCaseId;
        }
    
        public String getRouteName() {
            return routeName;
        }
    
        public void setRouteName(String routeName) {
            this.routeName = routeName;
        }
    
        public int getDirection() {
            return direction;
        }
    
        public void setDirection(int direction) {
            this.direction = direction;
        }
    
        public String getOperatingDay() {
            return operatingDay;
        }
    
        public void setOperatingDay(String operatingDay) {
            this.operatingDay = operatingDay;
        }
    
        public String getStartTime() {
            return startTime;
        }
    
        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }
    
        public String getAffectedDeparturesStatus() {
            return affectedDeparturesStatus;
        }
    
        public void setAffectedDeparturesStatus(String affectedDeparturesStatus) {
            this.affectedDeparturesStatus = affectedDeparturesStatus;
        }
    
        public String getDeviationCasesType() {
            return deviationCasesType;
        }
    
        public void setDeviationCasesType(String deviationCasesType) {
            this.deviationCasesType = deviationCasesType;
        }
    
        public Date getAffectedDeparturesLastModified() {
            return AffectedDeparturesLastModified;
        }
    
        public void setAffectedDeparturesLastModified(Date affectedDeparturesLastModified) {
            AffectedDeparturesLastModified = affectedDeparturesLastModified;
        }
    }
    
    // identical method is in many repos
    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    static List<GtfsRealtime.FeedEntity> createFeedEntities(final List<InternalMessages.Bulletin> bulletins, final boolean globalNoServiceAlerts) {
        return bulletins.stream().map(bulletin -> {
            final Optional<GtfsRealtime.Alert> maybeAlert = createAlert(bulletin, globalNoServiceAlerts);
            return maybeAlert.map(alert -> {
                GtfsRealtime.FeedEntity.Builder builder = GtfsRealtime.FeedEntity.newBuilder();
                builder.setId(bulletin.getBulletinId());
                builder.setAlert(alert);
                return builder.build();
            });
        }).filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private static boolean bulletinAffectsAll(InternalMessages.Bulletin bulletin) {
        return bulletin.getAffectsAllRoutes() || bulletin.getAffectsAllStops();
    }

    static Optional<GtfsRealtime.Alert> createAlert(final InternalMessages.Bulletin bulletin, final boolean globalNoServiceAlerts) {
        Optional<GtfsRealtime.Alert> maybeAlert;
        try {
            if (bulletin.hasDisplayOnly() && bulletin.getDisplayOnly()) {
                log.debug("No alert created for bulletin {} that is meant to be published only on vehicle displays", bulletin.getBulletinId());
                return Optional.empty();
            }

            final long startInUtcSecs = bulletin.getValidFromUtcMs() / 1000;
            final long stopInUtcSecs = bulletin.getValidToUtcMs() / 1000;
            final GtfsRealtime.TimeRange timeRange = GtfsRealtime.TimeRange.newBuilder()
                    .setStart(startInUtcSecs)
                    .setEnd(stopInUtcSecs)
                    .build();

            final GtfsRealtime.Alert.Builder builder = GtfsRealtime.Alert.newBuilder();
            builder.addActivePeriod(timeRange);
            builder.setCause(toGtfsCause(bulletin.getCategory()));
            builder.setEffect(getGtfsEffect(bulletin, globalNoServiceAlerts));
            if (bulletin.getTitlesCount() > 0) {
                builder.setHeaderText(toGtfsTranslatedString(bulletin.getTitlesList()));
            }
            if (bulletin.getDescriptionsCount() > 0) {
                builder.setDescriptionText(toGtfsTranslatedString(bulletin.getDescriptionsList()));
            }
            if (bulletin.getUrlsCount() > 0) {
                builder.setUrl(toGtfsTranslatedString(bulletin.getUrlsList()));
            }
            final Optional<GtfsRealtime.Alert.SeverityLevel> maybeSeverityLevel = toGtfsSeverityLevel(bulletin.getPriority());
            maybeSeverityLevel.ifPresent(builder::setSeverityLevel);

            Collection<GtfsRealtime.EntitySelector> entitySelectors = entitySelectorsForBulletin(bulletin);
            if (entitySelectors.isEmpty()) {
                log.error("Failed to find any Informed Entities for bulletin Id {}. Discarding alert.", bulletin.getBulletinId());
                maybeAlert = Optional.empty();
            }
            else {
                builder.addAllInformedEntity(entitySelectors);
                maybeAlert = Optional.of(builder.build());
            }
        } catch (Exception e) {
            log.error("Exception while creating an alert for bulletin {}!", bulletin.getBulletinId(), e);
            maybeAlert = Optional.empty();
        }

        maybeAlert.ifPresent(alert -> {
            final Optional<String> titleEn = alert.getHeaderText().getTranslationList().stream().filter(translation -> "en".equals(translation.getLanguage())).findAny().map(GtfsRealtime.TranslatedString.Translation::getText);
            log.info("Created an alert with title {} for bulletin {}", titleEn.orElse("null"), bulletin.getBulletinId());
        });

        return maybeAlert;
    }

    static Collection<GtfsRealtime.EntitySelector> entitySelectorsForBulletin(final InternalMessages.Bulletin bulletin) {
        Set<GtfsRealtime.EntitySelector> selectors = new HashSet<>();
        if (bulletinAffectsAll(bulletin)) {
            log.debug("Bulletin {} affects all routes or stops", bulletin.getBulletinId());

            GtfsRealtime.EntitySelector agency = GtfsRealtime.EntitySelector.newBuilder()
                    .setAgencyId(AGENCY_ENTITY_SELECTOR)
                    .build();
            selectors.add(agency);
        }
        if (bulletin.getAffectedRoutesCount() > 0) {
            for (final InternalMessages.Bulletin.AffectedEntity route : bulletin.getAffectedRoutesList()) {
                GtfsRealtime.EntitySelector entity = GtfsRealtime.EntitySelector.newBuilder()
                        .setRouteId(RouteIdUtils.normalizeRouteId(route.getEntityId())) //Normalize route ID to avoid publishing IDs that are not present in the static feed
                        .build();
                selectors.add(entity);
            }
        }
        if (bulletin.getAffectedStopsCount() > 0) {
            for (final InternalMessages.Bulletin.AffectedEntity stop : bulletin.getAffectedStopsList()) {
                GtfsRealtime.EntitySelector entity = GtfsRealtime.EntitySelector.newBuilder()
                        .setStopId(stop.getEntityId()).build();
                selectors.add(entity);
            }
        }

        return selectors;
    }

    private void sendPulsarMessage(final GtfsRealtime.FeedMessage feedMessage, long timestampMs) throws PulsarClientException {
        try {
            producer.newMessage().value(feedMessage.toByteArray())
                    .eventTime(timestampMs)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert.toString())
                    .send();
            log.info("Produced a new GTFS-RT service alert message with timestamp {}", timestampMs);
        }
        catch (PulsarClientException e) {
            log.error("Failed to send message to Pulsar", e);
            throw e;
        }
        catch (Exception e) {
            log.error("Failed to handle alert message", e);
        }
    }

    public static GtfsRealtime.Alert.Cause toGtfsCause(final InternalMessages.Category category) {
        switch (category) {
            case OTHER_DRIVER_ERROR:
            case TOO_MANY_PASSENGERS:
            case MISPARKED_VEHICLE:
            case TEST:
            case STATE_VISIT:
            case TRACK_BLOCKED:
            case EARLIER_DISRUPTION:
            case OTHER:
            case NO_TRAFFIC_DISRUPTION:
            case TRAFFIC_JAM:
            case PUBLIC_EVENT:
            case STAFF_DEFICIT:
            case DISTURBANCE:
                return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case ITS_SYSTEM_ERROR:
            case SWITCH_FAILURE:
            case TECHNICAL_FAILURE:
            case VEHICLE_BREAKDOWN:
            case POWER_FAILURE:
            case VEHICLE_DEFICIT:
                return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case STRIKE:
                return GtfsRealtime.Alert.Cause.STRIKE;
            case VEHICLE_OFF_THE_ROAD:
            case TRAFFIC_ACCIDENT:
            case ACCIDENT:
                return GtfsRealtime.Alert.Cause.ACCIDENT;
            case SEIZURE:
            case MEDICAL_INCIDENT:
                return GtfsRealtime.Alert.Cause.MEDICAL_EMERGENCY;
            case WEATHER:
            case WEATHER_CONDITIONS:
                return GtfsRealtime.Alert.Cause.WEATHER;
            case ROAD_MAINTENANCE:
            case TRACK_MAINTENANCE:
                return GtfsRealtime.Alert.Cause.MAINTENANCE;
            case ROAD_CLOSED:
            case ROAD_TRENCH:
                return GtfsRealtime.Alert.Cause.CONSTRUCTION;
            case ASSAULT:
                return GtfsRealtime.Alert.Cause.POLICE_ACTIVITY;
            default:
                return GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE;
        }
    }

    public static GtfsRealtime.Alert.Effect getGtfsEffect(final InternalMessages.Bulletin bulletin, final boolean globalNoServiceAlerts) {
        final boolean affectsAll = bulletinAffectsAll(bulletin);
        final InternalMessages.Bulletin.Impact impact = bulletin.getImpact();

        final GtfsRealtime.Alert.Effect effect = toGtfsEffect(impact);
        if (effect == GtfsRealtime.Alert.Effect.NO_SERVICE && affectsAll && !globalNoServiceAlerts) {
            //If the bulletin affects all traffic (i.e. entity selector list contains agency), we don't want to use NO_SERVICE effect, because otherwise Google and others will display all traffic as cancelled
            return GtfsRealtime.Alert.Effect.REDUCED_SERVICE;
        }

        return effect;
    }

    public static GtfsRealtime.Alert.Effect toGtfsEffect(final InternalMessages.Bulletin.Impact impact) {
        switch (impact) {
            case CANCELLED:
                return GtfsRealtime.Alert.Effect.NO_SERVICE;
            case DELAYED:
            case IRREGULAR_DEPARTURES:
                return GtfsRealtime.Alert.Effect.SIGNIFICANT_DELAYS;
            case DEVIATING_SCHEDULE:
            case POSSIBLE_DEVIATIONS:
                return GtfsRealtime.Alert.Effect.MODIFIED_SERVICE;
            case DISRUPTION_ROUTE:
                return GtfsRealtime.Alert.Effect.DETOUR;
            case POSSIBLY_DELAYED:
            case VENDING_MACHINE_OUT_OF_ORDER:
            case RETURNING_TO_NORMAL:
            case OTHER:
                return GtfsRealtime.Alert.Effect.OTHER_EFFECT;
            case REDUCED_TRANSPORT:
                return GtfsRealtime.Alert.Effect.REDUCED_SERVICE;
            case NO_TRAFFIC_IMPACT:
                return GtfsRealtime.Alert.Effect.NO_EFFECT;
            default:
                return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
        }
    }

    public static Optional<GtfsRealtime.Alert.SeverityLevel> toGtfsSeverityLevel(final InternalMessages.Bulletin.Priority priority) {
        switch (priority) {
            case INFO: return Optional.of(GtfsRealtime.Alert.SeverityLevel.INFO);
            case WARNING: return Optional.of(GtfsRealtime.Alert.SeverityLevel.WARNING);
            case SEVERE: return Optional.of(GtfsRealtime.Alert.SeverityLevel.SEVERE);
            default: return Optional.empty();
        }
    }

    public static GtfsRealtime.TranslatedString toGtfsTranslatedString(final List<InternalMessages.Bulletin.Translation> translations) {
        GtfsRealtime.TranslatedString.Builder builder = GtfsRealtime.TranslatedString.newBuilder();
        for (final InternalMessages.Bulletin.Translation translation: translations) {
            GtfsRealtime.TranslatedString.Translation gtfsTranslation = GtfsRealtime.TranslatedString.Translation.newBuilder()
                    .setText(translation.getText())
                    .setLanguage(translation.getLanguage())
                    .build();
            builder.addTranslation(gtfsTranslation);
        }
        return builder.build();
    }
    
    // This method is copied from transitdata-omm-cancellation-source
    static class CancellationData {
        public final InternalMessages.TripCancellation payload;
        public final long timestampEpochMs;
        public final String dvjId;
        public final long deviationCaseId;
        
        public CancellationData(InternalMessages.TripCancellation payload, long timestampEpochMs, String dvjId, long deviationCaseId) {
            this.payload = payload;
            this.timestampEpochMs = timestampEpochMs;
            this.dvjId = dvjId;
            this.deviationCaseId = deviationCaseId;
        }
        
        public String getDvjId() {
            return dvjId;
        }
        
        public InternalMessages.TripCancellation getPayload() {
            return payload;
        }
        
        public long getTimestamp() {
            return timestampEpochMs;
        }
    }
    
    // This method is copied from transitdata-omm-cancellation-source
    private void sendCancellations(List<CancellationData> cancellations) throws PulsarClientException {
        for (CancellationData data: cancellations) {
            sendPulsarMessage(data.payload, data.timestampEpochMs, data.dvjId);
        }
    }
    
    // This method is copied from transitdata-omm-cancellation-source
    private void sendPulsarMessage(InternalMessages.TripCancellation tripCancellation, long timestamp, String dvjId) throws PulsarClientException {
        try {
            producer.newMessage().value(tripCancellation.toByteArray())
                    .eventTime(timestamp)
                    .key(dvjId)
                    .property(TransitdataProperties.KEY_DVJ_ID, dvjId)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                    .send();
            
            if (tripCancellation.getDeviationCasesType() == InternalMessages.TripCancellation.DeviationCasesType.CANCEL_DEPARTURE && tripCancellation.getAffectedDeparturesType() == InternalMessages.TripCancellation.AffectedDeparturesType.CANCEL_ENTIRE_DEPARTURE) {
                log.info("Produced entire departure cancellation for trip: " + tripCancellation.getRouteId() + "/" +
                        tripCancellation.getDirectionId() + "-" + tripCancellation.getStartTime() + "-" +
                        tripCancellation.getStartDate());
            }
        } catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        } catch (Exception e) {
            log.error("Failed to handle cancellation message", e);
        }
    }
    
    // This method is copied from transitdata-omm-cancellation-source
    public Optional<Long> toUtcEpochMs(String localTimestamp) {
        return toUtcEpochMs(localTimestamp, "timeZone");
    }
    
    // This method is copied from transitdata-omm-cancellation-source
    public static Optional<Long> toUtcEpochMs(String localTimestamp, String zoneId) {
        if (localTimestamp == null || localTimestamp.isEmpty())
            return Optional.empty();
        
        try {
            LocalDateTime dt = LocalDateTime.parse(localTimestamp.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
            ZoneId zone = ZoneId.of(zoneId);
            long epochMs = dt.atZone(zone).toInstant().toEpochMilli();
            return Optional.of(epochMs);
        }
        catch (Exception e) {
            log.error("Failed to parse datetime from " + localTimestamp, e);
            return Optional.empty();
        }
    }
}
