package bixo.fetcher;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.config.QueuePolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IRobotRules;
import bixo.utils.GroupingKey;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;


public class FetcherQueueMgrTest {
    
    private class MyCollector extends TupleEntryCollector {
        private int _numCollected;
        
        @SuppressWarnings("unchecked")
        @Override
        protected void collect(Tuple tuple) {
            _numCollected += 1;
            
            Comparable status = tuple.get(FetchedDatum.FIELDS.size());
            Assert.assertTrue(status instanceof String);
            UrlStatus urlStatus = UrlStatus.valueOf((String)status);
            Assert.assertEquals(UrlStatus.SKIPPED_TIME_LIMIT, urlStatus);
        }

        public Object getNumCollected() {
            return _numCollected;
        }
    }
    

    @SuppressWarnings("serial")
    private class TestFetcherPolicy extends FetcherPolicy {
        
        TestFetcherPolicy(long crawlDelay, int maxUrlsPerRequest) {
            super();
            
            setCrawlDelay(crawlDelay);
            setMaxRequestsPerConnection(maxUrlsPerRequest);
        }
        
        @Override
        public FetcherPolicy makeNewPolicy(long crawlDelay) {
            return new TestFetcherPolicy(crawlDelay, getMaxRequestsPerConnection());
        }
    }
    
    @Test
    public void testUnsetCrawlDelay() {
        FetcherPolicy fetcherPolicy = new FetcherPolicy();
        fetcherPolicy.setCrawlDelay(1L);
        BixoFlowProcess process = new BixoFlowProcess();
        QueuePolicy queuePolicy = new QueuePolicy(FetcherQueueMgr.DEFAULT_MAX_URLS_IN_MEMORY, fetcherPolicy);
        FetcherQueueMgr queueMgr = new FetcherQueueMgr(process, fetcherPolicy, queuePolicy);
        
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);
        FetcherQueue queue = queueMgr.createQueue("domain.com", collector, IRobotRules.UNSET_CRAWL_DELAY);
        
        ScoredUrlDatum datum = new ScoredUrlDatum("http://domain.com");
        int numQueued = 0;
        for (int i = 0; i < 30; i++) {
            if (queue.offer(datum)) {
                numQueued += 1;
            }
        }
        
        Assert.assertEquals(30, numQueued);
        List<ScoredUrlDatum> urls = queue.poll();
        Assert.assertNotNull(urls);
        Assert.assertEquals(30, urls.size());
    }
    
    @Test
    public void testPollWhenCrawlIsDone() {
        FetcherPolicy fetcherPolicy = new FetcherPolicy();
        BixoFlowProcess process = new BixoFlowProcess();
        QueuePolicy queuePolicy = new QueuePolicy(FetcherQueueMgr.DEFAULT_MAX_URLS_IN_MEMORY, fetcherPolicy);
        FetcherQueueMgr queueMgr = new FetcherQueueMgr(process, fetcherPolicy, queuePolicy);
        
        MyCollector collector = new MyCollector();
        FetcherQueue newQueue = queueMgr.createQueue("domain.com", collector, fetcherPolicy.getCrawlDelay());
        
        ScoredUrlDatum scoredDatum = new ScoredUrlDatum("http://domain.com", 0, 0, UrlStatus.UNFETCHED, "domain.com-30000", 1.0, null);
        Assert.assertTrue(newQueue.offer(scoredDatum));
        Assert.assertTrue(queueMgr.offer(newQueue));
        
        Assert.assertEquals(0, collector.getNumCollected());
        
        // Let's say the crawl is done - so we need to skip everything
        queueMgr.skipAll(UrlStatus.SKIPPED_TIME_LIMIT);
        
        // Now when we poll, there shouldn't be anything to fetch because we're past our end
        // of crawl, and the collector should have a skipped entry.
        Assert.assertNull(queueMgr.poll());
        Assert.assertEquals(1, collector.getNumCollected());
        Assert.assertTrue(newQueue.isEmpty());
    }
    


    @Test
    public void testThreadedAccess() throws InterruptedException {
        // Let's create a queue with many entries, and verify that we get all of the URLs by the
        // time the queue mgr says that it's done.
        final long defaultCrawlDelay = 100;
        final int maxUrlsPerRequest = 2;
        FetcherPolicy fetcherPolicy = new TestFetcherPolicy(defaultCrawlDelay, maxUrlsPerRequest);

        final int urlsPerDomain = 10;
        final int targetDomains = 1000;
        
        QueuePolicy queuePolicy = new QueuePolicy(targetDomains * urlsPerDomain, urlsPerDomain);
        final FetcherQueueMgr queueMgr = new FetcherQueueMgr(new BixoFlowProcess(), fetcherPolicy, queuePolicy);
        
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);

        int numUrlsQueued = 0;
        int domainId = 1;
        while (true) {
            String domain = "domain-" + domainId + ".com";
            long crawlDelay = defaultCrawlDelay + (domainId % 10);
            FetcherQueue queue = queueMgr.createQueue(domain, collector, crawlDelay);
            
            String key = GroupingKey.makeGroupingKey(domainId, domain, crawlDelay);
            ScoredUrlDatum scoredDatum = new ScoredUrlDatum("http://" + domain, 0, 0, UrlStatus.UNFETCHED, key, 1.0, null);
            
            int numUrls = 1 + (domainId % urlsPerDomain);
            for (int i = 0; i < numUrls; i++) {
                Assert.assertTrue(queue.offer(scoredDatum));
            }
            
            if (!queueMgr.offer(queue)) {
                break;
            }

            numUrlsQueued += numUrls;
            domainId += 1;
        }
        
        // We should have targetDomains domains queued up
        // Now fire up threads to process the entries.
        final int numThreads = 100;
        final AtomicInteger fetched = new AtomicInteger();
        
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    while (!isInterrupted()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            interrupt();
                        }
                        
                        FetchList list = queueMgr.poll();
                        if (list != null) {
                            fetched.addAndGet(list.size());
                            list.finished();
                        }
                    }
                }
            };
            
            t.start();
        }
        
        Thread.sleep(20);
        final Logger logger = Logger.getLogger(FetcherQueueMgrTest.class);
        queueMgr.logPendingQueues(logger, 10);
        
        while (!queueMgr.isEmpty()) {
            queueMgr.getNextQueue();
        }
        
        Assert.assertEquals(numUrlsQueued, fetched.get());
    }
    
    @Test
    public void testNextFetchTimeTooSoon() throws Exception {
        final long defaultCrawlDelay = 100;
        final int maxUrlsPerRequest = 1;
        FetcherPolicy fetcherPolicy = new TestFetcherPolicy(defaultCrawlDelay, maxUrlsPerRequest);

        final int numDomains = 3;
        final int urlsPerDomain = 2;
        final int maxUrlsInMemory = numDomains * urlsPerDomain;
        final int maxUrlsInMemoryPerDomain = urlsPerDomain;

        QueuePolicy queuePolicy = new QueuePolicy(maxUrlsInMemory, maxUrlsInMemoryPerDomain);
        final FetcherQueueMgr queueMgr = new FetcherQueueMgr(new BixoFlowProcess(), fetcherPolicy, queuePolicy);

        for (int i = 0; i < numDomains; i++) {
            final String domain = "domain-" + i + ".com";
            TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);
            FetcherQueue queue = queueMgr.createQueue(domain, collector, defaultCrawlDelay);

            final String key = GroupingKey.makeGroupingKey(2, domain, defaultCrawlDelay);
            ScoredUrlDatum scoredDatum = new ScoredUrlDatum("http://" + domain, 0, 0, UrlStatus.UNFETCHED, key, 1.0, null);
            assertTrue(queue.offer(scoredDatum));
            assertTrue(queue.offer(scoredDatum));

            assertTrue(queueMgr.offer(queue));
        }

        final int numThreads = numDomains;
        for (int i = 0; i < numThreads; i++) {
            // First thread has really long fetches.
            final long fetchDuration = (i == 0 ? defaultCrawlDelay * 10 : defaultCrawlDelay / 10);
            
            Thread t = new Thread() {
                @Override
                public void run() {
                    while (!isInterrupted()) {
                        try {
                            FetchList list = queueMgr.poll();
                            if (list != null) {
                                System.out.println(this.getName() + " beginning fetch: " + System.currentTimeMillis());
                                Thread.sleep(fetchDuration);
                                System.out.println(this.getName() + " ending fetch: " + System.currentTimeMillis());
                                list.finished();
                            } else {
                                System.out.println(this.getName() + " has nothing to fetch: " + System.currentTimeMillis());
                                Thread.sleep(defaultCrawlDelay);
                            }
                        } catch (InterruptedException e) {
                            interrupt();
                        }

                    }
                }
            };

            t.start();
        }
        
        while (!queueMgr.isEmpty()) {
            FetcherQueue q = queueMgr.getNextQueue();
            System.out.print("Next queue at " + System.currentTimeMillis() + ": ");
            if (q != null) {
                System.out.println(q);
                System.out.println("\tDelay value is " + q.getDelay(TimeUnit.MILLISECONDS));
            } else {
                System.out.println("<nothing>");
            }
            
            Thread.sleep(defaultCrawlDelay / 2);
        }

    }
}
