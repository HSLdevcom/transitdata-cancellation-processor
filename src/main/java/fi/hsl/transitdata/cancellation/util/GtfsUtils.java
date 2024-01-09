package fi.hsl.transitdata.cancellation.util;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;

import java.util.List;
import java.util.Optional;

public class GtfsUtils {
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
            case INFO:
                return Optional.of(GtfsRealtime.Alert.SeverityLevel.INFO);
            case WARNING:
                return Optional.of(GtfsRealtime.Alert.SeverityLevel.WARNING);
            case SEVERE:
                return Optional.of(GtfsRealtime.Alert.SeverityLevel.SEVERE);
            default:
                return Optional.empty();
        }
    }

    public static GtfsRealtime.TranslatedString toGtfsTranslatedString(final List<InternalMessages.Bulletin.Translation> translations) {
        GtfsRealtime.TranslatedString.Builder builder = GtfsRealtime.TranslatedString.newBuilder();
        for (final InternalMessages.Bulletin.Translation translation : translations) {
            GtfsRealtime.TranslatedString.Translation gtfsTranslation = GtfsRealtime.TranslatedString.Translation.newBuilder()
                    .setText(translation.getText())
                    .setLanguage(translation.getLanguage())
                    .build();
            builder.addTranslation(gtfsTranslation);
        }
        return builder.build();
    }
}