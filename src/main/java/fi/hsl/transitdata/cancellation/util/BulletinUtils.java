package fi.hsl.transitdata.cancellation.util;

import fi.hsl.common.transitdata.proto.InternalMessages;

import java.util.List;
import java.util.stream.Collectors;

public class BulletinUtils {

    public static List<InternalMessages.Bulletin> filterMassCancellationsFromBulletins(List<InternalMessages.Bulletin> bulletins) {
        return bulletins.stream().filter(
                        bulletin ->
                                bulletin.getImpact() == InternalMessages.Bulletin.Impact.CANCELLED &&
                                        bulletin.getPriority() == InternalMessages.Bulletin.Priority.WARNING)
                .collect(Collectors.toList());
    }

    public static boolean bulletinAffectsAll(InternalMessages.Bulletin bulletin) {
        return bulletin.getAffectsAllRoutes() || bulletin.getAffectsAllStops();
    }
}