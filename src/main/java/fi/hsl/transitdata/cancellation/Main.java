package fi.hsl.transitdata.cancellation;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class Main {
    
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    
    public static void main(String[] args) {
        log.info("Starting CancellationProcessor");
        Config config = ConfigParser.createConfig();
        String timezone = config.getString("processor.timezone");
        log.info("Using timezone " + timezone);
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            String digitransitDeveloperApiUri = getDigitransitDeveloperApiUri();
            
            PulsarApplicationContext context = app.getContext();
            final AlertHandler handler = new AlertHandler(context, timezone, digitransitDeveloperApiUri);
        
            log.info("Start handling the messages");
            app.launchWithHandler(handler);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
    
    private static String getDigitransitDeveloperApiUri() throws Exception {
        String digitransitDeveloperApiUri;
        
        if (ConfigUtils.getEnv("FILEPATH_DIGITRANSIT_DEVAPI_SECRET").isEmpty()) {
            throw new Exception("Environment variable FILEPATH_DIGITRANSIT_DEVAPI_SECRET is missing");
        }
        
        try {
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_DIGITRANSIT_DEVAPI_SECRET").get();
            digitransitDeveloperApiUri = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read Digitransit Developer API URI from secrets", e);
            throw e;
        }
        
        if (StringUtils.isBlank(digitransitDeveloperApiUri)) {
            throw new Exception("Failed to find Digitransit Developer API URI, exiting application");
        }
        
        return digitransitDeveloperApiUri;
    }
}
