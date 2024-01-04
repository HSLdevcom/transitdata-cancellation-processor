package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.cancellation.schema.Route;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.core.Document;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.graphql.client.vertx.dynamic.VertxDynamicGraphQLClientBuilder;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

import static io.smallrye.graphql.client.core.Argument.arg;
import static io.smallrye.graphql.client.core.Argument.args;
import static io.smallrye.graphql.client.core.Document.document;
import static io.smallrye.graphql.client.core.Field.field;
import static io.smallrye.graphql.client.core.Operation.operation;

public class TripUtils {
    
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
    
    public static List<InternalMessages.TripInfo> getTripInfos(String date, List<String> routeIds, String digitransitDeveloperApiUri) {
        // Input: reitti-id, pvm, alkuaika, loppuaika
        // Output: reitti-id, pvm, lähtöaika, suunta (0 ja 1, tai 1 ja 2)
        // GraphQL-haku, digitransitin api key secreteihin
        return null;
    }
}
