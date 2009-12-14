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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import bixo.cascading.BixoFlowProcess;
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
            Assert.assertEquals(UrlStatus.SKIPPED_TIME_LIMIT, status);
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
    public void testCrawlDurationLimit() {
        FetcherPolicy spy = Mockito.spy(new FetcherPolicy());
        MyCollector collector = new MyCollector();
        FetcherQueue queue = new FetcherQueue("domain.com", spy, new BixoFlowProcess(), collector);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.0d);

        queue.offer(fetchItem1);
        queue.offer(fetchItem2);
        Assert.assertEquals(2, queue.getNumQueued());
        
        Mockito.doReturn(0L).when(spy).getCrawlEndTime();
        queue.offer(fetchItem3);
        Assert.assertEquals(1, queue.getNumSkipped());

        Mockito.doReturn(FetcherPolicy.NO_CRAWL_END_TIME).when(spy).getCrawlEndTime();
        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(2, items.size());
        Assert.assertTrue(items.get(0).getUrl().equals("http://domain.com/page1"));

        Assert.assertNull(queue.poll());
    }
    

    @Test
    public void testCrawlDelay() throws InterruptedException {
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

        queue.offer(fetchItem1);
        queue.offer(fetchItem2);
        queue.offer(fetchItem3);
        Assert.assertEquals(3, queue.getNumQueued());

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
    public void testAdaptiveFetchPolicyNoDelay() {
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + 1000L, 0);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), null);

        // First, we should be able to queue everything, since there's no minimum crawl delay
        for (int i = 0; i < 100; i++) {
            ScoredUrlDatum fetchQueueEntry = makeSUD("http://domain.com/page-" + i + ".html", 1.0);
            queue.offer(fetchQueueEntry);
        }
        
        Assert.assertEquals(100, queue.getNumQueued());

        // Now we should get back everything we queued in one request.
        FetchList items = queue.poll();
        Assert.assertNotNull(items);
        Assert.assertEquals(100, items.size());
    }
    
    @Test
    public void testAdaptiveFetchPolicyMinDelay() {
        AdaptiveFetcherPolicy policy = new AdaptiveFetcherPolicy(System.currentTimeMillis() + 2000L, 1 * 1000L);
        
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), Mockito.mock(TupleEntryCollector.class));

        // We should be able to queue one item, maybe two or three, but definitely not four.
        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        ScoredUrlDatum fetchItem3 = makeSUD("http://domain.com/page3", 0.2d);
        ScoredUrlDatum fetchItem4 = makeSUD("http://domain.com/page4", 0.2d);

        queue.offer(fetchItem1);
        Assert.assertEquals(1, queue.getNumQueued());
        queue.offer(fetchItem2);
        queue.offer(fetchItem3);
        queue.offer(fetchItem4);
        Assert.assertEquals(2, queue.getNumSkipped());
    }
    
    @Test
    public void testAbortingWhenPastEndOfCrawl() throws InterruptedException {
        FetcherPolicy policy = new FetcherPolicy();
        policy.setCrawlEndTime(System.currentTimeMillis() + 100);
        
        MyCollector collector = new MyCollector();
        FetcherQueue queue = new FetcherQueue("domain.com", policy, new BixoFlowProcess(), collector);

        ScoredUrlDatum fetchItem1 = makeSUD("http://domain.com/page1", 1.0d);
        ScoredUrlDatum fetchItem2 = makeSUD("http://domain.com/page2", 0.5d);
        
        queue.offer(fetchItem1);
        Assert.assertEquals(1, queue.getNumQueued());
        Thread.sleep(100);
        queue.offer(fetchItem2);
        Assert.assertEquals(1, queue.getNumSkipped());
        
        Assert.assertNull(queue.poll());
        Assert.assertEquals(2, collector.getNumCollected());
    }
    
    // TODO KKr - add test for multiple threads hitting the queue at the same
    // time.
}
