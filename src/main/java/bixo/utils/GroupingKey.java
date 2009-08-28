package bixo.utils;

// TODO KKr - make these static methods in BaseGroupingKeyGenerator once that's an abstract class
// versus an interface.

public class GroupingKey {
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
