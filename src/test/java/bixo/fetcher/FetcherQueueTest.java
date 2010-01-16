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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import bixo.config.AdaptiveFetcherPolicy;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.utils.DomainNames;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FetcherQueueTest {
    private class MyCollector extends TupleEntryCollector {
        private int _numCollected;
        
        @Override
        protected void collect(Tuple tuple) {
            _numCollected += 1;
            
            // Verify we can convert tuple to FetchedDatum
            new FetchedDatum(tuple, new Fields());
            
            // Verify the UrlStatus is correct.
            
            UrlStatus status = UrlStatus.valueOf((String)tuple.get(FetchedDatum.FIELDS.size()));
            assertEquals(UrlStatus.SKIPPED_TIME_LIMIT, status);
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
        public FetcherPolicy makeNewPolicy(long crawlDelay) {
            return new ControlledFetcherPolicy(getMaxUrls(), _numUrlsPerRequest, crawlDelay);
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
    public void testCrawlDurationLimit() {
        FetcherPolicy spy = Mockito.spy(new FetcherPolicy());
        MyCollector collector = new MyCollector();
        FetcherQueue queue = new FetcherQueue("domain.com", spy, collector);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.0d);

        assertTrue(queue.offer(fetchItem1));
        assertTrue(queue.offer(fetchItem2));
        
        Mockito.doReturn(0L).when(spy).getCrawlEndTime();
        assertFalse(queue.offer(fetchItem3));

        Mockito.doReturn(FetcherPolicy.NO_CRAWL_END_TIME).when(spy).getCrawlEndTime();
        List<ScoredUrlDatum> items = queue.poll();
        assertNotNull(items);
        assertEquals(2, items.size());
        assertTrue(items.get(0).getUrl().equals("http://domain.com/page1"));

        assertNull(queue.poll());
    }
    

    @Test
    public void testCrawlDelay() throws InterruptedException {
        FetcherPolicy policy = new ControlledFetcherPolicy(1000, 1, 1 * 1000L);
        FetcherQueue queue = new FetcherQueue("domain.com", policy, null);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 0.5d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.0d);

        queue.offer(fetchItem1);
        queue.offer(fetchItem2);

        List<ScoredUrlDatum> items = queue.poll();
        assertNotNull(items);
        queue.release(items);
        assertNull(queue.poll());
        Thread.sleep(1000L);
        assertNotNull(queue.poll());
    }

    @Test
    public void testMultipleRequestsPerConnection() throws InterruptedException {
        FetcherPolicy policy = new ControlledFetcherPolicy(1000, 2, 1 * 1000L);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, null);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.2d);

        assertTrue(queue.offer(fetchItem1));
        assertTrue(queue.offer(fetchItem2));
        assertTrue(queue.offer(fetchItem3));

        List<ScoredUrlDatum> items = queue.poll();
        assertNotNull(items);
        assertEquals(2, items.size());
        queue.release(items);
        
        assertNull(queue.poll());
        Thread.sleep(2 * 1000L);
        
        items = queue.poll();
        assertNotNull(items);
        assertEquals(1, items.size());
    }
    
    @Test
    public void testAdaptiveFetchPolicyNoDelay() {
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + 1000L, 0);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, null);

        // First, we should be able to queue everything, since there's no minimum crawl delay
        for (int i = 0; i < 100; i++) {
            ScoredUrlDatum fetchQueueEntry = makeSUD("http://domain.com/page-" + i + ".html", 1.0);
            assertTrue(queue.offer(fetchQueueEntry));
        }
        
        // Now we should get back everything we queued in one request.
        List<ScoredUrlDatum> items = queue.poll();
        assertNotNull(items);
        assertEquals(100, items.size());
    }
    
    @Test
    public void testAdaptiveFetchPolicyMinDelay() {
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + 2000L, 1 * 1000L);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, Mockito.mock(TupleEntryCollector.class));

        // We should be able to queue one item, maybe two or three, but definitely not four.
        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.2d);
        ScoredUrlDatum fetchItem4 = makeSUD("http://domain.com/page4", 0.2d);

        assertTrue(queue.offer(fetchItem1));
        assertTrue(queue.offer(fetchItem2));
        assertFalse(queue.offer(fetchItem3));
        assertFalse(queue.offer(fetchItem4));
    }
    
    @Test
    public void testAbortingWhenPastEndOfCrawl() throws InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlEndTime(System.currentTimeMillis() + 100);
        
        MyCollector collector = new MyCollector();
        FetcherQueue queue = new FetcherQueue("domain.com", policy, collector);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        
        assertTrue(queue.offer(fetchItem1));
        Thread.sleep(100);
        assertFalse(queue.offer(fetchItem2));
        
        assertNull(queue.poll());
        assertEquals(2, collector.getNumCollected());
    }
    
    @Test
    public void testDelayValue() {
        FetcherPolicy policy = Mockito.mock(FetcherPolicy.class);
        Mockito.when(policy.getCrawlEndTime()).thenReturn(FetcherPolicy.NO_CRAWL_END_TIME);
        Mockito.when(policy.getMaxUrls()).thenReturn(1000);
        Mockito.when(policy.getFetchRequest(Mockito.anyInt())).thenReturn(new FetchRequest(1, System.currentTimeMillis() + 10000));
        FetcherQueue queue = new FetcherQueue("domain.com", policy, Mockito.mock(TupleEntryCollector.class));
        
        ScoredUrlDatum fetchItem = makeSUD("http://domain.com/page1", 1.0d);
        
        for (int i = 0; i < 100; i++) {
            queue.offer(fetchItem);
        }

        assertTrue(queue.getDelay(TimeUnit.MILLISECONDS) < 0);
        assertNotNull(queue.poll());
        assertTrue(queue.getDelay(TimeUnit.MILLISECONDS) > 0);
    }
    
    @Test
    public void testComparison() {
        FetcherPolicy policy1 = Mockito.mock(FetcherPolicy.class);
        Mockito.when(policy1.getCrawlEndTime()).thenReturn(FetcherPolicy.NO_CRAWL_END_TIME);
        Mockito.when(policy1.getMaxUrls()).thenReturn(1000);
        Mockito.when(policy1.getFetchRequest(Mockito.anyInt())).thenReturn(new FetchRequest(1, System.currentTimeMillis() + 1000));
        FetcherQueue queue1 = new FetcherQueue("domain.com", policy1, Mockito.mock(TupleEntryCollector.class));

        ScoredUrlDatum fetchItem = makeSUD("http://domain.com/page1", 1.0d);
        
        queue1.offer(fetchItem);
        queue1.offer(fetchItem);

        FetcherPolicy policy2 = Mockito.mock(FetcherPolicy.class);
        Mockito.when(policy2.getCrawlEndTime()).thenReturn(FetcherPolicy.NO_CRAWL_END_TIME);
        Mockito.when(policy2.getMaxUrls()).thenReturn(1000);
        Mockito.when(policy2.getFetchRequest(Mockito.anyInt())).thenReturn(new FetchRequest(1, System.currentTimeMillis() + 10000));

        FetcherQueue queue2 = new FetcherQueue("domain.com", policy2, Mockito.mock(TupleEntryCollector.class));

        queue2.offer(fetchItem);
        queue2.offer(fetchItem);

        assertNotNull(queue1.poll());
        assertNotNull(queue2.poll());
        
        // queue1 should sort ahead of queue2, because the time until next fetch
        // is less (policy1 fetch request time == cur time + 1000, vs. cur time + 10000)
        assertEquals(-1, queue1.compareTo(queue2));
    }
    
    @Test
    public void testEstimatedEndTime() {
        final int crawlDelay = 100;
        final int numUrlsPerRequest = 10;
        
        long now = System.currentTimeMillis();

        FetcherPolicy policy = Mockito.mock(FetcherPolicy.class);
        Mockito.when(policy.getMaxUrls()).thenReturn(1000);
        Mockito.when(policy.getFetchRequest(Mockito.anyInt())).thenReturn(new FetchRequest(numUrlsPerRequest, now + crawlDelay));
        FetcherQueue queue = new FetcherQueue("domain.com", policy, Mockito.mock(TupleEntryCollector.class));
        
        // Result from empty queue should be basically "now".
        assertTrue(queue.getFinishTime() <= System.currentTimeMillis() + 10);
        
        ScoredUrlDatum fetchItem = makeSUD("http://domain.com/page1", 1.0d);
        for (int i = 0; i < numUrlsPerRequest; i++) {
            assertTrue(queue.offer(fetchItem));
        }
        
        // Finish time should be roughly the same as what we return from getFetchRequest
        long delta = (queue.getFinishTime() - now) - crawlDelay;
        assertTrue(Math.abs(delta) <= 20);
    }
    
    // TODO KKr - add test for multiple threads hitting the queue at the same
    // time.
}
