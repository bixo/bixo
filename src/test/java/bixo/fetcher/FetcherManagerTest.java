package bixo.fetcher;

import bixo.cascading.BixoFlowProcess;
import junit.framework.TestCase;

public class FetcherManagerTest extends TestCase {
    public final void testTermination() throws InterruptedException {
        FetcherQueueMgr queueMgr = new FetcherQueueMgr();
        IHttpFetcherFactory fetcherFactory = new FakeHttpFetcherFactory(true, 10);
        FetcherManager fetcherMgr = new FetcherManager(queueMgr, fetcherFactory, new BixoFlowProcess());

        Thread fetcherThread = new Thread(fetcherMgr);
        fetcherThread.setName("Fetcher manager");
        fetcherThread.start();
        Thread.sleep(500L);
        fetcherThread.interrupt();
        
        Thread.sleep(1500L);
        
        assertFalse("Fetcher manager should be terminated", fetcherThread.isAlive());
    }
}
