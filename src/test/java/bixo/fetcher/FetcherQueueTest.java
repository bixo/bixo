/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.fetcher;

import java.net.MalformedURLException;
import java.util.Random;

import junit.framework.TestCase;
import bixo.cascading.BixoFlowProcess;
import bixo.fetcher.beans.FetchItem;
import bixo.fetcher.beans.FetcherPolicy;
import bixo.tuple.UrlWithScoreTuple;

public class FetcherQueueTest extends TestCase {
    public final void testMaxURLs() throws MalformedURLException {
        FetcherPolicy policy = new FetcherPolicy(30, 1, 1, FetcherPolicy.NO_MIN_RESPONSE_RATE);
        FetcherQueue queue = new FetcherQueue("domain.com", policy, 1, new BixoFlowProcess(), null);

        UrlWithScoreTuple urlScore1 = new UrlWithScoreTuple();
        urlScore1.setUrl("http://domain.com/page1");
        urlScore1.SetScore(0.0d);
        FetchItem fetchItem1 = new FetchItem(urlScore1);

        UrlWithScoreTuple urlScore2 = new UrlWithScoreTuple();
        urlScore2.setUrl("http://domain.com/page2");
        urlScore2.SetScore(1.0d);
        FetchItem fetchItem2 = new FetchItem(urlScore2);

        UrlWithScoreTuple urlScore3 = new UrlWithScoreTuple();
        urlScore3.setUrl("http://domain.com/page3");
        urlScore3.SetScore(0.5d);
        FetchItem fetchItem3 = new FetchItem(urlScore3);

        queue.offer(fetchItem1);
        String bestUrl = "http://domain.com/page2";
        assertTrue(queue.offer(fetchItem2));
        assertFalse(queue.offer(fetchItem3));

        FetchList items = queue.poll();
        assertNotNull(items);
        assertEquals(1, items.size());
        assertTrue(items.get(0).getUrl().equals(bestUrl));

        assertNull(queue.poll());
    }

    public final void testSortedUrls() throws MalformedURLException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(0);

        FetcherQueue queue = new FetcherQueue("domain.com", policy, 100, new BixoFlowProcess(), null);
        Random rand = new Random(1L);

        for (int i = 0; i < 1000; i++) {
            UrlWithScoreTuple urlScore = new UrlWithScoreTuple();
            urlScore.setUrl("http://domain.com/page" + rand.nextInt());
            urlScore.SetScore(rand.nextFloat());
            FetchItem fetchItem = new FetchItem(urlScore);

            queue.offer(fetchItem);
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
        FetcherQueue queue = new FetcherQueue("domain.com", policy, 100, new BixoFlowProcess(), null);

        UrlWithScoreTuple urlScore1 = new UrlWithScoreTuple();
        urlScore1.setUrl("http://domain.com/page1");
        urlScore1.SetScore(0.5d);
        FetchItem fetchItem1 = new FetchItem(urlScore1);

        UrlWithScoreTuple urlScore2 = new UrlWithScoreTuple();
        urlScore2.setUrl("http://domain.com/page2");
        urlScore2.SetScore(0.0d);
        FetchItem fetchItem2 = new FetchItem(urlScore2);

        queue.offer(fetchItem1);
        queue.offer(fetchItem2);

        FetchList items = queue.poll();
        assertNotNull(items);
        queue.release(items);
        assertNull(queue.poll());
        Thread.sleep(policy.getCrawlDelay() * 1000L);
        assertNotNull(queue.poll());
    }

    public final void testMultipleRequestsPerConnection() throws InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(1);
        policy.setRequestsPerConnect(2);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, 100, new BixoFlowProcess(), null);

        UrlWithScoreTuple urlScore1 = new UrlWithScoreTuple();
        urlScore1.setUrl("http://domain.com/page1");
        urlScore1.SetScore(1.0d);
        FetchItem fetchItem1 = new FetchItem(urlScore1);

        UrlWithScoreTuple urlScore2 = new UrlWithScoreTuple();
        urlScore2.setUrl("http://domain.com/page2");
        urlScore2.SetScore(0.5d);
        FetchItem fetchItem2 = new FetchItem(urlScore2);

        UrlWithScoreTuple urlScore3 = new UrlWithScoreTuple();
        urlScore3.setUrl("http://domain.com/page3");
        urlScore3.SetScore(0.2d);
        FetchItem fetchItem3 = new FetchItem(urlScore3);

        assertTrue(queue.offer(fetchItem1));
        assertTrue(queue.offer(fetchItem2));
        assertTrue(queue.offer(fetchItem3));

        FetchList items = queue.poll();
        assertNotNull(items);
        assertEquals(2, items.size());
        queue.release(items);
        
        assertNull(queue.poll());
        Thread.sleep(2 * policy.getCrawlDelay() * 1000L);
        
        items = queue.poll();
        assertNotNull(items);
        assertEquals(1, items.size());
    }
    
    // TODO KKr - add test for multiple threads hitting the queue at the same
    // time.
}
