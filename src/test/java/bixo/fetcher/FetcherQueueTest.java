package bixo.fetcher;

import java.net.MalformedURLException;
import java.util.Random;

import junit.framework.TestCase;
import bixo.items.FetchItem;

public class FetcherQueueTest extends TestCase {
    public final void testMaxURLs() throws MalformedURLException {
        FetcherPolicy policy = new FetcherPolicy(30, 1, 1);
        FetcherQueue queue = new FetcherQueue("domain.com", policy, 1);

        queue.offer("http://domain.com/page1", 0.0f);
        String bestUrl = "http://domain.com/page2";
        assertTrue(queue.offer(bestUrl, 1.0f));
        assertFalse(queue.offer("http://domain.com/page3", 0.5f));

        FetchList items = queue.poll();
        assertNotNull(items);
        assertEquals(1, items.size());
        assertTrue(items.get(0).getUrl().equals(bestUrl));

        assertNull(queue.poll());
    }


    public final void testSortedUrls() throws MalformedURLException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(0);

        FetcherQueue queue = new FetcherQueue("domain.com", policy, 100);
        Random rand = new Random(1L);

        for (int i = 0; i < 1000; i++) {
            queue.offer("http://domain.com/page" + rand.nextInt(), rand.nextFloat());
        }

        double curScore = 2.0;
        int totalItems = 0;
        while (totalItems < 100) {
            FetchList items = queue.poll();
            assertNotNull(items);
            totalItems += items.size();

            assertTrue(items.get(0).getScore() <= curScore);
            for (FetchItem item : items) {
                assertTrue(item.getScore() <= curScore);
                curScore = item.getScore();
            }

            queue.release(items);
        }

        assertEquals(100, totalItems);
        assertNull(queue.poll());
    }


    public final void testTimeLimit() throws MalformedURLException, InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(1);
        policy.setRequestsPerConnect(1);
        FetcherQueue queue = new FetcherQueue("domain.com", policy, 100);
        queue.offer("http://domain.com/page1", 0.5f);
        queue.offer("http://domain.com/page2", 0.0f);

        FetchList items = queue.poll();
        assertNotNull(items);
        queue.release(items);
        assertNull(queue.poll());
        Thread.sleep(policy.getCrawlDelay() * 1000L);
        assertNotNull(queue.poll());
    }

    // TODO KKr - add test for multiple threads hitting the queue at the same time.
}
