package bixo.fetcher;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IRobotRules;
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
    
    @Test
    public void testUnsetCrawlDelay() {
        FetcherPolicy defaultPolicy = new FetcherPolicy();
        defaultPolicy.setCrawlDelay(1L);
        BixoFlowProcess process = new BixoFlowProcess();
        FetcherQueueMgr queueMgr = new FetcherQueueMgr(process, defaultPolicy);
        
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);
        FetcherQueue queue = queueMgr.createQueue("domain", collector, IRobotRules.UNSET_CRAWL_DELAY);
        
        ScoredUrlDatum datum = Mockito.mock(ScoredUrlDatum.class);
        for (int i = 0; i < 30; i++) {
            queue.offer(datum);
        }
        
        Assert.assertEquals(30, queue.getNumQueued());
        FetchList fl = queue.poll();
        Assert.assertNotNull(fl);
        Assert.assertEquals(30, fl.size());
    }
    
    @Test
    public void testPollWhenCrawlIsDone() {
        FetcherPolicy spy = Mockito.spy(new FetcherPolicy());
        BixoFlowProcess process = new BixoFlowProcess();
        FetcherQueueMgr queueMgr = new FetcherQueueMgr(process, spy);
        
        MyCollector collector = new MyCollector();
        FetcherQueue newQueue = queueMgr.createQueue("domain.com", collector, spy.getCrawlDelay());
        
        ScoredUrlDatum scoredDatum = new ScoredUrlDatum("http://domain.com", 0, 0, UrlStatus.UNFETCHED, "domain.com-30000", 1.0, null);
        newQueue.offer(scoredDatum);
        Assert.assertEquals(1, newQueue.getNumQueued());
        Assert.assertTrue(queueMgr.offer(newQueue));
        
        Assert.assertEquals(0, collector.getNumCollected());
        
        // Now set up for the crawl to be over (1970 target end :))
        Mockito.doReturn(0L).when(spy).getCrawlEndTime();
        Assert.assertEquals(0, spy.getCrawlEndTime());
        
        // Now when we poll, there shouldn't be anything to fetch because we're past our end
        // of crawl, and the collector should have a skipped entry.
        Assert.assertNull(queueMgr.poll());
        Assert.assertEquals(1, collector.getNumCollected());
        Assert.assertTrue(newQueue.isEmpty());
    }
}
