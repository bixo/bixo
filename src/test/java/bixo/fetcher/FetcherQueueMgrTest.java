package bixo.fetcher;

import org.junit.Assert;
import org.junit.Test;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;


public class FetcherQueueMgrTest {
    private class MyCollector extends TupleEntryCollector {
        private int _numCollected;
        
        @Override
        protected void collect(Tuple tuple) {
            _numCollected += 1;
            
            FetchedDatum datum = new FetchedDatum(tuple, new Fields());
            Assert.assertEquals(FetchStatusCode.ABORTED, datum.getStatusCode());
        }

        public Object getNumCollected() {
            return _numCollected;
        }
    }
    
    @Test
    public void testPollWhenCrawlIsDone() throws InterruptedException {
        FetcherPolicy defaultPolicy = new FetcherPolicy();
        defaultPolicy.setCrawlEndTime(System.currentTimeMillis() + 100);

        BixoFlowProcess process = new BixoFlowProcess();
        FetcherQueueMgr queueMgr = new FetcherQueueMgr(process, defaultPolicy);
        
        MyCollector collector = new MyCollector();
        FetcherQueue newQueue = queueMgr.createQueue("domain.com", collector);
        
        ScoredUrlDatum scoredDatum = new ScoredUrlDatum("http://domain.com", 0, 0, FetchStatusCode.UNFETCHED, "domain.com-30000", 1.0, null);
        Assert.assertTrue(newQueue.offer(scoredDatum));
        Assert.assertTrue(queueMgr.offer(newQueue));
        
        Assert.assertEquals(0, collector.getNumCollected());
        
        Thread.sleep(200);
        
        // Now when we poll, there shouldn't be anything to fetch because we're past our end
        // of crawl, and the collector should have an aborted entry.
        Assert.assertNull(queueMgr.poll());
        Assert.assertEquals(1, collector.getNumCollected());
        Assert.assertTrue(newQueue.isEmpty());
    }
}
