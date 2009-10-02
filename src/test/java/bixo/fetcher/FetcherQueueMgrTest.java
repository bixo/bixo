package bixo.fetcher;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
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
    public void testPollWhenCrawlIsDone() throws InterruptedException {
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
