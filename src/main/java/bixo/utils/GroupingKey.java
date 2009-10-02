package bixo.utils;


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
    
    public static boolean isSpecialKey(String key) {
        return key.startsWith(KEY_PREFIX);
    }
    
    public static String makeGroupingKey(String domain, long crawlDelay) {
        return String.format("%s-%d", domain, crawlDelay);
    }
    
    public static String getDomainFromKey(String key) {
        int dividerPos = key.lastIndexOf('-');
        if (dividerPos == -1) {
            throw new RuntimeException("Invalid grouping key: " + key);
        }

        return key.substring(0, dividerPos);
    }
    
    public static long getCrawlDelayFromKey(String key) {
        int dividerPos = key.lastIndexOf('-');
        if (dividerPos == -1) {
            throw new RuntimeException("Invalid grouping key: " + key);
        }

        try {
            return Long.parseLong(key.substring(dividerPos + 1));
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid crawl delay in grouping key: " + key);
        }
    }
}
