package bixo.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import bixo.datum.UrlStatus;
import bixo.fetcher.http.IRobotRules;


public class GroupingKey {
    private static final String KEY_PREFIX = GroupingKey.class.getSimpleName() + "-";
    
    // Special results for grouping key used in FetchPipe.
    
    // URL was blocked by robots.txt
    public static final String BLOCKED_GROUPING_KEY = KEY_PREFIX + "blocked";
    
    // Host couldn't be resolved to an IP address.
    public static final String UNKNOWN_HOST_GROUPING_KEY = KEY_PREFIX + "unknown";
    
    // Couldn't process robots.txt, so defer fetch of URL
    public static final String DEFERRED_GROUPING_KEY = KEY_PREFIX + "deferred";

    // IScoreGenerator returned SKIP_URL_SCORE
    public static final String SKIPPED_GROUPING_KEY = KEY_PREFIX + "skipped";
    
    // URL isn't valid
    public static final String INVALID_URL_GROUPING_KEY = KEY_PREFIX + "invalid";

    // Pattern for grouping key. This must be kept in sync with the
    // UNSET_DURATION constant and the makeGroupingKey code.
    private static final Pattern GROUPING_KEY_PATTERN = Pattern.compile("(.+)-(\\d+|unset)");
    
    private static final String UNSET_DURATION = "unset";
    
    public static boolean isSpecialKey(String key) {
        return key.startsWith(KEY_PREFIX);
    }
    
    public static String makeGroupingKey(String domain, long crawlDelay) {
        if (crawlDelay == IRobotRules.UNSET_CRAWL_DELAY) {
            return String.format("%s-%s", domain, UNSET_DURATION);
        } else {
            return String.format("%s-%d", domain, crawlDelay);
        }
    }
    
    public static String getDomainFromKey(String key) {
        Matcher m = GROUPING_KEY_PATTERN.matcher(key);
        if (!m.matches()) {
            throw new RuntimeException("Invalid grouping key: " + key);
        }

        return m.group(1);
    }
    
    public static long getCrawlDelayFromKey(String key) {
        Matcher m = GROUPING_KEY_PATTERN.matcher(key);
        if (!m.matches()) {
            throw new RuntimeException("Invalid grouping key: " + key);
        }
        
        String durationString = m.group(2);
        // If we have <domain>-unset, then the crawl delay wasn't set.
        if (durationString.equals(UNSET_DURATION)) {
            return IRobotRules.UNSET_CRAWL_DELAY;
        }

        try {
            return Long.parseLong(durationString);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid crawl delay in grouping key: " + key);
        }
    }
    
    public static UrlStatus makeUrlStatusFromKey(String key) {
        UrlStatus status;
        
        if (!isSpecialKey(key)) {
            status = UrlStatus.UNFETCHED;
        } else if (key.equals(GroupingKey.BLOCKED_GROUPING_KEY)) {
            status = UrlStatus.SKIPPED_BLOCKED;
        } else if (key.equals(GroupingKey.UNKNOWN_HOST_GROUPING_KEY)) {
            status = UrlStatus.SKIPPED_UNKNOWN_HOST;
        } else if (key.equals(GroupingKey.INVALID_URL_GROUPING_KEY)) {
            status = UrlStatus.SKIPPED_INVALID_URL;
        } else if (key.equals(GroupingKey.DEFERRED_GROUPING_KEY)) {
            status = UrlStatus.SKIPPED_DEFERRED;
        } else if (key.equals(GroupingKey.SKIPPED_GROUPING_KEY)) {
            status = UrlStatus.SKIPPED_BY_SCORER;
        } else {
            throw new RuntimeException("Unknown value for special grouping key: " + key);
        }

        return status;
    }
}
