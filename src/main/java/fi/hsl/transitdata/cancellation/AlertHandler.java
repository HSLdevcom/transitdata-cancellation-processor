package fi.hsl.transitdata.cancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;

import fi.hsl.transitdata.cancellation.domain.CancellationData;
import fi.hsl.transitdata.cancellation.domain.Trip;

import fi.hsl.transitdata.cancellation.util.BulletinUtils;
import fi.hsl.transitdata.cancellation.util.GtfsUtils;
import fi.hsl.transitdata.cancellation.util.TimeUtils;

import org.apache.pulsar.client.api.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    // TODO: Siirrä messageHandler luokkaan?
    public void handleMessage(final Message message) {
        try {
            List<CancellationData> cancellationDataList = new ArrayList<>();
            
            if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert)) {
                InternalMessages.ServiceAlert serviceAlert = InternalMessages.ServiceAlert.parseFrom(message.getData());
                List<InternalMessages.Bulletin> massCancellations = BulletinUtils.filterMassCancellationsFromBulletins(serviceAlert.getBulletinsList());
                cancellationDataList = parseCancellationDataFromBulletins(massCancellations);
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

    @NotNull
    private List<CancellationData> parseCancellationDataFromBulletins(List<InternalMessages.Bulletin> massCancellations) {
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
            builder.setRouteId(trip.getRouteName());
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
            
            Optional<Long> epochTimestamp = TimeUtils.toUtcEpochMs(timestamp.toString());
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
            builder.setCause(GtfsUtils.toGtfsCause(bulletin.getCategory()));
            builder.setEffect(GtfsUtils.getGtfsEffect(bulletin, globalNoServiceAlerts));
            if (bulletin.getTitlesCount() > 0) {
                builder.setHeaderText(GtfsUtils.toGtfsTranslatedString(bulletin.getTitlesList()));
            }
            if (bulletin.getDescriptionsCount() > 0) {
                builder.setDescriptionText(GtfsUtils.toGtfsTranslatedString(bulletin.getDescriptionsList()));
            }
            if (bulletin.getUrlsCount() > 0) {
                builder.setUrl(GtfsUtils.toGtfsTranslatedString(bulletin.getUrlsList()));
            }
            final Optional<GtfsRealtime.Alert.SeverityLevel> maybeSeverityLevel = GtfsUtils.toGtfsSeverityLevel(bulletin.getPriority());
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
        if (BulletinUtils.bulletinAffectsAll(bulletin)) {
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

    // TODO: Siirrä messageHandler luokkaan?
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

    // This method is copied from transitdata-omm-cancellation-source
    private void sendCancellations(List<CancellationData> cancellations) throws PulsarClientException {
        for (CancellationData data: cancellations) {
            sendPulsarMessage(data.payload, data.timestampEpochMs, data.dvjId);
        }
    }
    
    // This method is copied from transitdata-omm-cancellation-source
    // TODO: Siirrä messageHandler luokkaan?
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

}
