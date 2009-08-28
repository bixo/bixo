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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import bixo.cascading.BixoFlowProcess;
import bixo.config.AdaptiveFetcherPolicy;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.AbortedFetchException;
import bixo.exceptions.AbortedFetchReason;
import bixo.utils.DomainNames;

public class FetcherQueueTest {
    private class MyCollector extends TupleEntryCollector {
        private int _numCollected;
        
        @Override
        protected void collect(Tuple tuple) {
            _numCollected += 1;
            
            // Verify we can convert tuple to FetchedDatum
            new FetchedDatum(tuple, new Fields());
            
            AbortedFetchException e = (AbortedFetchException)tuple.get(FetchedDatum.FIELDS.size());
            Assert.assertEquals(AbortedFetchReason.TIME_LIMIT, e.getAbortReason());
        }

        public Object getNumCollected() {
            return _numCollected;
        }
    }

    @SuppressWarnings("serial")
    private class ControlledFetcherPolicy extends FetcherPolicy {
        private int _maxUrls;
        private int _numUrlsPerRequest;
   
        public ControlledFetcherPolicy(int maxUrls, int numUrlsPerRequest, long crawlDelay) {
            super();
            
            _maxUrls = maxUrls;
            _numUrlsPerRequest = numUrlsPerRequest;
            
            setCrawlDelay(crawlDelay);
        }

        @Override
        public int getMaxUrls() {
            return _maxUrls;
        }
        
        @Override
        public FetchRequest getFetchRequest(int maxUrls) {
            int numUrls = Math.min(_numUrlsPerRequest, maxUrls);
            long nextFetchTime = System.currentTimeMillis() + (numUrls * _crawlDelay);
            return new FetchRequest(numUrls, nextFetchTime);
        }
    }
    
    private static ScoredUrlDatum makeSUD(String url, double score) {
        return new ScoredUrlDatum(url, 0, 0, UrlStatus.UNFETCHED, DomainNames.getPLD(url) + "-30000", score, null);
    }
    
    @Test
    public void testCrawlDurationLimit() throws MalformedURLException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlEndTime(System.currentTimeMillis() + 1000);
        MyCollector collector = new MyCollector();
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), collector);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 0.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 1.0d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.5d);

        Assert.assertTrue(queue.offer(fetchItem1));
        Assert.assertTrue(queue.offer(fetchItem2));
        Assert.assertFalse(queue.offer(fetchItem3));

        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(1, items.size());
        Assert.assertTrue(items.get(0).getUrl().equals("http://domain.com/page2"));

        Assert.assertNull(queue.poll());
    }

    @Test
    public void testSortedUrls() throws MalformedURLException {
        FetcherPolicy policy = new ControlledFetcherPolicy(1000, 1, 0);
        
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
    }
    

    @Test
    public void testCrawlDelay() throws MalformedURLException, InterruptedException {
        FetcherPolicy policy = new ControlledFetcherPolicy(1000, 1, 1 * 1000L);
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 0.5d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.0d);

        queue.offer(fetchItem1);
        queue.offer(fetchItem2);

        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        queue.release(items);
        Assert.assertNull(queue.poll());
        Thread.sleep(1000L);
        Assert.assertNotNull(queue.poll());
    }

    @Test
    public void testMultipleRequestsPerConnection() throws InterruptedException {
        FetcherPolicy policy = new ControlledFetcherPolicy(1000, 2, 1 * 1000L);
        
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
        Thread.sleep(2 * 1000L);
        
        items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(1, items.size());
    }
    
    @Test
    public void testAdaptiveFetchPolicyNoDelay() throws InterruptedException {
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + 1000L, 0);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        // First, we should be able to queue everything, since there's no minimum crawl delay
        for (int i = 0; i < 100; i++) {
            ScoredUrlDatum fetchQueueEntry = makeSUD("http://domain.com/page-" + i + ".html", 1.0);
            Assert.assertTrue(queue.offer(fetchQueueEntry));
        }

        // Now we should get back everything we queued in one request.
        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(100, items.size());
    }
    
    @Test
    public void testAdaptiveFetchPolicyMinDelay() throws InterruptedException {
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + 2000L, 1 * 1000L);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        // We should be able to queue one item, maybe two or three, but definitely not four.
        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.2d);
        ScoredUrlDatum fetchItem4 = makeSUD("http://domain.com/page4", 0.2d);

        Assert.assertTrue(queue.offer(fetchItem1));
        queue.offer(fetchItem2);
        queue.offer(fetchItem3);
        Assert.assertFalse(queue.offer(fetchItem4));
    }
    
    @Test
    public void testAbortingWhenPastEndOfCrawl() throws InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlEndTime(System.currentTimeMillis() + 100);
        
        MyCollector collector = new MyCollector();
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), collector);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);

        Assert.assertTrue(queue.offer(fetchItem1));
        Thread.sleep(100);
        Assert.assertFalse(queue.offer(fetchItem2));
        
        Assert.assertNull(queue.poll());
        Assert.assertEquals(1, collector.getNumCollected());
    }
    
    // TODO KKr - add test for multiple threads hitting the queue at the same
    // time.
}
