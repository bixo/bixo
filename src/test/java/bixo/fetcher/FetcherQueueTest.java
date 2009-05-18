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

import org.junit.Assert;
import org.junit.Test;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.ScoredUrlDatum;
import bixo.utils.DomainNames;

public class FetcherQueueTest {
    
    private static ScoredUrlDatum makeSUD(String url, double score) {
        return new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, DomainNames.getPLD(url), score, null);
    }
    
    @Test
    public void testCrawlDurationLimit() throws MalformedURLException {
        // Set the target end of the crawl to now, which means we'll only try to fetch a single URL.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlEndTime(System.currentTimeMillis());
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 0.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 1.0d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.5d);

        Assert.assertTrue(queue.offer(fetchItem1));
        Assert.assertTrue(queue.offer(fetchItem2));
        Assert.assertFalse(queue.offer(fetchItem3));

        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(1, items.size());
        Assert.assertTrue(items.get(0).getNormalizedUrl().equals("http://domain.com/page2"));

        Assert.assertNull(queue.poll());
    }

    @Test
    public void testSortedUrls() throws MalformedURLException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(0);
        policy.setRequestsPerConnection(1);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);
        Random rand = new Random(1L);

        for (int i = 0; i < 100; i++) {
            ScoredUrlDatum fetchQueueEntry = makeSUD("http://domain.com/page" + rand.nextInt(), rand.nextFloat());
            queue.offer(fetchQueueEntry);
        }

        double curScore = 2.0;
        int totalItems = 0;
        while (totalItems < 10) {
            FetchList items = queue.poll();
            Assert.assertNotNull(items);
            totalItems += items.size();

            Assert.assertTrue(items.get(0).getScore() <= curScore);
            for (ScoredUrlDatum item : items) {
                Assert.assertTrue(item.getScore() <= curScore);
                curScore = item.getScore();
            }

            queue.release(items);
        }

        Assert.assertEquals(10, totalItems);
        
        queue.setMaxUrls(2);
        curScore = 2.0;
        totalItems = 0;
        while (totalItems < 2) {
            FetchList items = queue.poll();
            Assert.assertNotNull(items);
            totalItems += items.size();

            Assert.assertTrue(items.get(0).getScore() <= curScore);
            for (ScoredUrlDatum item : items) {
                Assert.assertTrue(item.getScore() <= curScore);
                curScore = item.getScore();
            }

            queue.release(items);
        }

        Assert.assertEquals(2, totalItems);
        Assert.assertNull(queue.poll());
    }
    

    @Test
    public void testCrawlDelay() throws MalformedURLException, InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(1);
        policy.setRequestsPerConnection(1);
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 0.5d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.0d);

        queue.offer(fetchItem1);
        queue.offer(fetchItem2);

        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        queue.release(items);
        Assert.assertNull(queue.poll());
        Thread.sleep(policy.getCrawlDelay() * 1000L);
        Assert.assertNotNull(queue.poll());
    }

    @Test
    public void testMultipleRequestsPerConnection() throws InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlDelay(1);
        policy.setRequestsPerConnection(2);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.2d);

        Assert.assertTrue(queue.offer(fetchItem1));
        Assert.assertTrue(queue.offer(fetchItem2));
        Assert.assertTrue(queue.offer(fetchItem3));

        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(2, items.size());
        queue.release(items);
        
        Assert.assertNull(queue.poll());
        Thread.sleep(2 * policy.getCrawlDelay() * 1000L);
        
        items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(1, items.size());
    }
    
    // TODO KKr - add test for multiple threads hitting the queue at the same
    // time.
}
