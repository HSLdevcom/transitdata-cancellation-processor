package fi.hsl.transitdata.cancellation;

import fi.hsl.transitdata.cancellation.schema.Route;

import java.util.Arrays;
import java.util.List;

public class SmallryeGraphqlClientTest {
    
    public static void main(String args[]) {
        SmallryeGraphqlClient smallryeGraphqlClient = new SmallryeGraphqlClient();
        List<Route> routes = smallryeGraphqlClient.getRoutes("20231216", Arrays.asList("HSL:4611", "HSL:1079"));
        System.out.println("Number of routes: " + routes.size());
    }
}
