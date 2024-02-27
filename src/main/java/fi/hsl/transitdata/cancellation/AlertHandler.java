package fi.hsl.transitdata.cancellation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.domain.CancellationData;
import fi.hsl.transitdata.cancellation.util.BulletinUtils;

import fi.hsl.transitdata.cancellation.util.CacheUtils;
import org.apache.pulsar.client.api.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class AlertHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(AlertHandler.class);

    private final Consumer<byte[]> consumer;
    private final Producer<byte[]> producer;
    
    public static final Duration CACHE_DURATION = Duration.of(4, ChronoUnit.HOURS);
    
    // KEY: bulletinId, VALUE: Map<KEY: tripId, VALUE: cancellationData>
    private final Cache<String, Map<String, CancellationData>> bulletinsCache;
    
    private final String timezone;
    
    private final String digitransitDeveloperApiUri;

    public AlertHandler(final PulsarApplicationContext context, String timezone, String digitransitDeveloperApiUri) {
        this.consumer = context.getConsumer();
        this.producer = context.getSingleProducer();
        
        this.timezone = timezone;
        this.digitransitDeveloperApiUri = digitransitDeveloperApiUri;
        
        this.bulletinsCache = Caffeine.newBuilder()
                .expireAfterAccess(CACHE_DURATION)
                .build(key -> new HashMap<>());
    }
    
    @Override
    public void handleMessage(@NotNull final Message message) {
        try {
            List<CancellationData> cancellationDataList = new ArrayList<>();
            
            if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert)) {
                InternalMessages.ServiceAlert serviceAlert = InternalMessages.ServiceAlert.parseFrom(message.getData());
                serviceAlert.getBulletinsList().forEach(bulletin -> log.info(
                        "Bulletin: impact={}, priority={}, category={}",
                        bulletin.getImpact(), bulletin.getPriority(), bulletin.getCategory()));
                List<InternalMessages.Bulletin> massCancellations = BulletinUtils.filterMassCancellationsFromBulletins(serviceAlert.getBulletinsList());
                
                if (massCancellations.isEmpty()) {
                    log.info("No mass cancellation bulletins, total number of bulletins: " + serviceAlert.getBulletinsList().size());
                } else {
                    List<String> routeIds = massCancellations.stream().flatMap(massCancellation ->
                            massCancellation.getAffectedRoutesList().stream().map(
                                    InternalMessages.Bulletin.AffectedEntity::getEntityId)).collect(Collectors.toList());
                    log.info("Affected routes: {}", routeIds);
                    for (InternalMessages.Bulletin massCancellation : massCancellations) {
                        List<CancellationData> bulletinCancellations =
                                BulletinUtils.createTripCancellations(massCancellation, timezone, digitransitDeveloperApiUri);
                        cancellationDataList.addAll(
                                CacheUtils.handleBulletinCancellations(massCancellation.getBulletinId(),
                                        bulletinCancellations, bulletinsCache));
                    }
                    log.info("Added {} cancellations from mass cancellation service alert", cancellationDataList.size());
                }
            } else if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation)) {
                InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.parseFrom(message.getData());
                CancellationData data = new CancellationData(tripCancellation, message.getEventTime(), message.getKey(), -1);
                cancellationDataList.add(data);
            } else {
                throw new Exception("Invalid protobuf schema");
            }
            
            sendCancellations(cancellationDataList);
        } catch (final Exception e) {
            log.error("Exception while handling message", e);
        } finally {
            ack(message.getMessageId());
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
        } catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        } catch (Exception e) {
            log.error("Failed to handle cancellation message", e);
        }
    }
}
