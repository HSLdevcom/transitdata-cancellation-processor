package fi.hsl.transitdata.cancellation;

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

/**
 * https://smallrye.io/smallrye-graphql/latest/
 */

public class SmallryeGraphqlClient {
    
    public List<Route> getRoutes(String date, List<String> routeIds) {
        List<Route> routes = new ArrayList<>();
        Vertx vertx = Vertx.vertx();
        
        DynamicGraphQLClient client = new VertxDynamicGraphQLClientBuilder()
                .url("https://api.digitransit.fi/routing/v1/routers/hsl/index/graphql?digitransit-subscription-key=a1e437f79628464c9ea8d542db6f6e94")
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
}
