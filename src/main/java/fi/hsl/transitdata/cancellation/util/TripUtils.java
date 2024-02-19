package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.schema.Route;
import fi.hsl.transitdata.cancellation.schema.Trip;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.core.Document;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.graphql.client.vertx.dynamic.VertxDynamicGraphQLClientBuilder;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static io.smallrye.graphql.client.core.Argument.arg;
import static io.smallrye.graphql.client.core.Argument.args;
import static io.smallrye.graphql.client.core.Document.document;
import static io.smallrye.graphql.client.core.Field.field;
import static io.smallrye.graphql.client.core.Operation.operation;

public class TripUtils {

    private static final Logger log = LoggerFactory.getLogger(TripUtils.class);

    /**
     * Get routes using a GraphQL query.
     *
     * @param date                       date as string, with format 'YYYYMMDD' (e.g. '20240131')
     * @param routeIds                   route identifiers
     * @param digitransitDeveloperApiUri Digitransit API URL
     * @return routes
     */
    public static List<Route> getRoutes(String date, List<String> routeIds, String digitransitDeveloperApiUri) {
        List<Route> routes = new ArrayList<>();
        Vertx vertx = Vertx.vertx();
        List<String> fixedRouteIds = addHSLPrefixToRouteIds(routeIds);

        DynamicGraphQLClient client = new VertxDynamicGraphQLClientBuilder()
                .url(digitransitDeveloperApiUri)
                .vertx(vertx)
                .build();

        List<Document> documents = new ArrayList<>();

        for (String id : fixedRouteIds) {
            List<String> singletonId = Collections.singletonList(id);

            Document document = document(operation(
                    field(
                            "routes",
                            args(arg("ids", singletonId)),
                            field("id"),
                            field("gtfsId"),
                            field(
                                    "trips",
                                    field("gtfsId"),
                                    field("directionId"),
                                    field("activeDates"),
                                    field(
                                            "departureStoptime",
                                            args(arg("serviceDate", date)),
                                            field("serviceDay"),
                                            field("scheduledDeparture")
                                    )
                            )
                    )
            ));

            documents.add(document);
        }

        for (Document document : documents) {
            Response response;
            try {
                response = client.executeSync(document);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get trip data", e);
            }

            if (response != null) {
                routes.addAll(response.getList(Route.class, "routes"));
            }
        }

        try {
            client.close();
            vertx.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close DynamicGraphQLClient", e);
        }

        return routes;
    }

    /**
     * Returns routeIds in format: 'HSL:1234'
     */
    static List<String> addHSLPrefixToRouteIds(List<String> routeIds) {
        return routeIds.stream().map(
                routeId -> routeId.startsWith("HSL:") ? routeId.trim() : "HSL:" + routeId.trim()).collect(Collectors.toList());
    }

    /**
     * Get trip infos of a time period.
     */
    public static List<InternalMessages.TripInfo> getTripInfos(
            List<String> routeIds, LocalDateTime validFrom, LocalDateTime validTo, String digitransitDeveloperApiUri) {
        List<String> dates = TimeUtils.getDatesAsList(validFrom, validTo);

        List<InternalMessages.TripInfo> tripInfos = dates.stream().flatMap(
                dateAsString -> getTripInfos(dateAsString, routeIds, digitransitDeveloperApiUri).stream()
        ).collect(Collectors.toList());

        List<InternalMessages.TripInfo> filteredTripInfos = filterTripInfos(tripInfos, validFrom, validTo);
        return removeDuplicates(filteredTripInfos);
    }

    static String getTripId(String originalTripId, String operatingDay) {
        String modifiedTripId;

        if (originalTripId.contains("Ma")) {
            modifiedTripId = originalTripId.replaceFirst("Ma", "MaTiKeToPe");
        } else if (originalTripId.contains("Ti")) {
            modifiedTripId = originalTripId.replaceFirst("Ti", "MaTiKeToPe");
        } else if (originalTripId.contains("Ke")) {
            modifiedTripId = originalTripId.replaceFirst("Ke", "MaTiKeToPe");
        } else if (originalTripId.contains("To")) {
            modifiedTripId = originalTripId.replaceFirst("To", "MaTiKeToPe");
        } else if (originalTripId.contains("Pe")) {
            modifiedTripId = originalTripId.replaceFirst("Pe", "MaTiKeToPe");
        } else if (originalTripId.contains("La")) {
            modifiedTripId = originalTripId.replaceFirst("La", "LaSu");
        } else if (originalTripId.contains("Su")) {
            modifiedTripId = originalTripId.replaceFirst("Su", "LaSu");
        } else {
            modifiedTripId = originalTripId;
        }

        return modifiedTripId + "_" + operatingDay;
    }

    static List<InternalMessages.TripInfo> removeDuplicates(List<InternalMessages.TripInfo> trips) {
        Set<String> seen = new HashSet<>();
        List<InternalMessages.TripInfo> tripsNoDuplicates = new ArrayList<>();
        for (InternalMessages.TripInfo trip : trips) {
            String key = trip.getRouteId() + "--" + trip.getOperatingDay() + "--"
                    + trip.getStartTime() + "--" + trip.getDirectionId();
            if (!seen.contains(key)) {
                seen.add(key);
                tripsNoDuplicates.add(trip);
            }
        }

        List<InternalMessages.TripInfo> tripsNewIds = new ArrayList<>();

        for (InternalMessages.TripInfo trip : tripsNoDuplicates) {
            InternalMessages.TripInfo.Builder builder = InternalMessages.TripInfo.newBuilder();
            builder.setRouteId(trip.getRouteId());
            builder.setTripId(getTripId(trip.getTripId(), trip.getOperatingDay()));
            builder.setOperatingDay(trip.getOperatingDay());
            builder.setStartTime(trip.getStartTime());
            builder.setDirectionId(trip.getDirectionId());
            tripsNewIds.add(builder.build());
        }
        return tripsNewIds;
    }

    /**
     * Filter out those trips whose first departure time is not inside the time period limited by validFrom and validTo
     * parameters.
     */
    static List<InternalMessages.TripInfo> filterTripInfos(
            List<InternalMessages.TripInfo> inputTripInfos, LocalDateTime validFrom, LocalDateTime validTo) {

        // KEY: date (e.g. "20242901"), VALUE: time (e.g. "1542")
        AbstractMap.SimpleEntry<String, String> validFromAsSimpleEntry = TimeUtils.convertInto30hClockStrings(validFrom);
        AbstractMap.SimpleEntry<String, String> validToAsSimpleEntry = TimeUtils.convertInto30hClockStrings(validTo);

        List<InternalMessages.TripInfo> outputTripInfos = inputTripInfos.stream().filter(tripInfo ->
                TimeUtils.isBetween(tripInfo.getOperatingDay(), tripInfo.getStartTime(),
                        validFromAsSimpleEntry, validToAsSimpleEntry)).collect(Collectors.toList());

        log.info("There are {} trip infos after filtering (before filtering {} trip infos). validFrom={}, validTo={}, timeZone={}",
                outputTripInfos.size(), inputTripInfos.size(), validFrom, validTo, TimeZone.getDefault().getDisplayName());

        return outputTripInfos;
    }

    /**
     * Get trip infos of a single day and routeIds.
     *
     * @param date                       date as string, with format 'YYYYMMDD' (e.g. '20240131')
     * @param routeIds                   route identifiers
     * @param digitransitDeveloperApiUri Digitransit API URL
     * @return trip infos
     */
    public static List<InternalMessages.TripInfo> getTripInfos(
            String date, List<String> routeIds, String digitransitDeveloperApiUri) {
        List<Route> routes = getRoutes(date, routeIds, digitransitDeveloperApiUri);
        log.info("Found {} routes (date={}, routeIds={}, digitransitDeveloperApiUri={})",
                routes.size(), date, routeIds, digitransitDeveloperApiUri.startsWith("https://dev-api.digitransit.fi"));
        
        List<InternalMessages.TripInfo> tripInfos = new ArrayList<>();

        for (Route route : routes) {
            if (route == null) {
                continue;
            }

            for (Trip trip : route.getTrips()) {
                String operatingDay = TimeUtils.getDateAsString(trip.getDepartureStoptime().getServiceDay());
                String startTime = TimeUtils.getTimeAsString(trip.getDepartureStoptime().getScheduledDeparture());

                InternalMessages.TripInfo.Builder builder = InternalMessages.TripInfo.newBuilder();
                builder.setRouteId(route.getGtfsId());
                builder.setTripId(trip.getGtfsId());
                builder.setOperatingDay(operatingDay);
                builder.setStartTime(startTime);
                builder.setDirectionId(Integer.parseInt(trip.getDirectionId()));
                tripInfos.add(builder.build());
            }
        }

        return tripInfos;
    }
}
