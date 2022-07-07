package fr.an.attrtreestore.util;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AttrTreeStoreUtils {

    private static final int SECONDS_PER_HOUR = 3600;
    private static final int SECONDS_PER_MINUTE = 60;

    public static String millisToText(long millis) {
        if (millis < 1000) {
            return millis + " ms";
        } else {
            long seconds = millis / 1000;
            if (seconds < 60) {
                return seconds + " s";
            } else {
                // cf from java.util.time.Duration
                long hours = seconds / SECONDS_PER_HOUR;
                int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
                int secs = (int) (seconds % SECONDS_PER_MINUTE);
                String res = ((hours > 0)? hours + "H" : "") + minutes + "mn" + secs + "s";
                return res + " (" + seconds + " s)";
            }
        }
    }

    
    public static <T,TSrc> List<T> map(Collection<TSrc> src, Function<TSrc,T> transform) {
        return src.stream().map(transform).collect(Collectors.toList());
    }

}
