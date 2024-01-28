package fi.hsl.transitdata.cancellation.domain;

import fi.hsl.common.transitdata.proto.InternalMessages;

public class CancellationData {
    public final InternalMessages.TripCancellation payload;
    public final long timestampEpochMs;
    public final String dvjId;
    public final long deviationCaseId;
    private final String tripId; // this is needed only internally in transitdata-cancellation-processor

    public CancellationData(InternalMessages.TripCancellation payload, long timestampEpochMs, String dvjId, long deviationCaseId) {
        this.payload = payload;
        this.timestampEpochMs = timestampEpochMs;
        this.dvjId = dvjId;
        this.deviationCaseId = deviationCaseId;
        this.tripId = payload.getTripId();
    }

    public String getDvjId() {
        return dvjId;
    }

    public InternalMessages.TripCancellation getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestampEpochMs;
    }
    
    public String getTripId() {
        return tripId;
    }
}
