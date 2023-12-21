package fi.hsl.transitdata.cancellation.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

public class TimeUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeUtils.class);

    // This methods is copied from transitdata-omm-cancellation-source
    public static Optional<Long> toUtcEpochMs(String localTimestamp) {
        return toUtcEpochMs(localTimestamp, "timeZone");
    }

    // This methods is copied from transitdata-omm-cancellation-source
    public static Optional<Long> toUtcEpochMs(String localTimestamp, String zoneId) {
        if (localTimestamp == null || localTimestamp.isEmpty())
            return Optional.empty();

        try {
            LocalDateTime dt = LocalDateTime.parse(localTimestamp.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
            ZoneId zone = ZoneId.of(zoneId);
            long epochMs = dt.atZone(zone).toInstant().toEpochMilli();
            return Optional.of(epochMs);
        } catch (Exception e) {
            TimeUtils.log.error("Failed to parse datetime from " + localTimestamp, e);
            return Optional.empty();
        }
    }
}