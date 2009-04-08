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
import bixo.fetcher.beans.FetcherPolicy;
import bixo.tuple.FetchTuple;

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
            for (FetchTuple item : items) {
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
