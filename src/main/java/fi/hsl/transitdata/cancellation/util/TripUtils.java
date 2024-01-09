package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.schema.Route;
import fi.hsl.transitdata.cancellation.schema.Trip;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.core.Document;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.graphql.client.vertx.dynamic.VertxDynamicGraphQLClientBuilder;
import io.vertx.core.Vertx;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.smallrye.graphql.client.core.Argument.arg;
import static io.smallrye.graphql.client.core.Argument.args;
import static io.smallrye.graphql.client.core.Document.document;
import static io.smallrye.graphql.client.core.Field.field;
import static io.smallrye.graphql.client.core.Operation.operation;

public class TripUtils {
    
    /**
     * Get routes using a GraphQL query.
     * @param date date as string, with format 'YYYYMMDD' (e.g. '20240131')
     * @param routeIds route identifiers
     * @param digitransitDeveloperApiUri
     * @return
     */
    public static List<Route> getRoutes(String date, List<String> routeIds, String digitransitDeveloperApiUri) {
        List<Route> routes = new ArrayList<>();
        Vertx vertx = Vertx.vertx();
        
        DynamicGraphQLClient client = new VertxDynamicGraphQLClientBuilder()
                .url(digitransitDeveloperApiUri)
                .vertx(vertx)
                .build();
        
        Document document = document(operation(
                field(
                        "routes",
                        args(arg("ids", routeIds)),
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
        
        Response response = null; // <2>
        try {
            response = client.executeSync(document);
            routes = response.getList(Route.class, "routes");
        } catch (Exception e) {
            throw new RuntimeException("Failed to get trip data", e);
        } finally {
            try {
                client.close();
                vertx.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close DynamicGraphQLClient", e);
            }
        }
        
        return routes;
    }
    
    /**
     * Get trip infos of a time period.
     */
    public static List<InternalMessages.TripInfo> getTripInfos(
            List<String> routeIds, LocalDateTime validFrom, LocalDateTime validTo, String digitransitDeveloperApiUri) {
        List<String> dates = TimeUtils.getDates(validFrom, validTo);
        
        List<InternalMessages.TripInfo> tripInfos = dates.stream().flatMap(
                dateAsString -> getTripInfos(dateAsString, routeIds, digitransitDeveloperApiUri).stream()
        ).collect(Collectors.toList());
        
        return filterTripInfos(tripInfos, validFrom, validTo);
    }
    
    /**
     * Filter out those trips whose first departure time is not inside the time period limited by validFrom and validTo
     * parameters.
     */
    static List<InternalMessages.TripInfo> filterTripInfos(
            List<InternalMessages.TripInfo> inputTripInfos, LocalDateTime validFrom, LocalDateTime validTo) {
        List<InternalMessages.TripInfo> outputTripInfos = inputTripInfos.stream().filter(tripInfo -> {
            LocalDateTime tripInfoDate = TimeUtils.getDate(tripInfo.getOperatingDay(), tripInfo.getStartTime());
            return tripInfoDate.isAfter(validFrom) && tripInfoDate.isBefore(validTo);
        }).collect(Collectors.toList());
        
        return outputTripInfos;
    }
    
    /**
     * Get trip infos of a single day and routeIds.
     * @param date date as string, with format 'YYYYMMDD' (e.g. '20240131')
     * @param routeIds route identifiers
     * @param digitransitDeveloperApiUri
     * @return
     */
    public static List<InternalMessages.TripInfo> getTripInfos(
            String date, List<String> routeIds, String digitransitDeveloperApiUri) {
        List<Route> routes = getRoutes(date, routeIds, digitransitDeveloperApiUri);
        List<InternalMessages.TripInfo> tripInfos = new ArrayList<>();
        
        for (Route route : routes) {
            for (Trip trip : route.getTrips()) {
                String operatingDay = TimeUtils.getShortDate(trip.getDepartureStoptime().getServiceDay());
                String startTime = TimeUtils.getShortTime(trip.getDepartureStoptime().getScheduledDeparture());
                
                InternalMessages.TripInfo.Builder builder = InternalMessages.TripInfo.newBuilder();
                builder.setRouteId(route.getGtfsId());
                builder.setTripId(trip.getGtfsId());
                builder.setOperatingDay(operatingDay);
                builder.setStartTime(startTime);
                builder.setDirectionId(Integer.valueOf(trip.getDirectionId()));
                tripInfos.add(builder.build());
            }
        }
        
        return tripInfos;
    }
}
