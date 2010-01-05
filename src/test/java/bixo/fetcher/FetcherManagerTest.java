package bixo.fetcher;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpServer;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.fetcher.simulation.SimulationWebServer;
import bixo.hadoop.FetchCounters;
import bixo.utils.ConfigUtils;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FetcherManagerTest extends SimulationWebServer {

    private static class FakeCollector extends TupleEntryCollector {

        @Override
        protected void collect(Tuple tuple) {
            // TODO KKr - reenable this when we have a better toString for the
            // tuple, where it limits the
            // amount of data and avoids printing control characters.
            // System.out.println(tuple.toString());
        }
    }

    @Test
    public final void testTermination() throws InterruptedException {
        BixoFlowProcess process = new BixoFlowProcess();
        FetcherQueueMgr queueMgr = new FetcherQueueMgr(process);
        IHttpFetcher fetcher = new FakeHttpFetcher(true, 10);
        FetcherManager fetcherMgr = new FetcherManager(queueMgr, fetcher, process);

        Thread fetcherThread = new Thread(fetcherMgr);
        fetcherThread.setName("Fetcher manager");
        fetcherThread.start();
        Thread.sleep(500L);
        fetcherThread.interrupt();

        Thread.sleep(1500L);

        Assert.assertFalse("Fetcher manager should be terminated", fetcherThread.isAlive());
    }

    @Test
    public final void testThreadPool() {
        // System.setProperty("bixo.root.level", "TRACE");
        
        final int NUM_THREADS = 5;

        HttpServer server = null;

        try {
            // Go for really slow response, so that all threads will be used up.
            server = startServer(new RandomResponseHandler(20000, 5 * 1000L), 8089);

            BixoFlowProcess flowProcess = new BixoFlowProcess();
            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setMinResponseRate(0);
            FetcherQueueMgr queueMgr = new FetcherQueueMgr(flowProcess, defaultPolicy);
            
            FetcherManager manager = new FetcherManager(queueMgr,
            		new SimpleHttpFetcher(NUM_THREADS, ConfigUtils.BIXO_TEST_AGENT), flowProcess);

            Thread t = new Thread(manager);
            t.setName("Fetcher manager");
            t.start();

            for (int i = 0; i < NUM_THREADS; i++) {
                String host = "domain-" + i + ".com";
                FetcherQueue queue = queueMgr.createQueue(host, new FakeCollector(), 0);

                for (int j = 0; j < 1; j++) {
                    String file = "/page-" + j + ".html";

                    String url = "http://localhost:8089" + file;
                    ScoredUrlDatum urlScore = new ScoredUrlDatum(url, 0, 0, UrlStatus.UNFETCHED, null, 1.0f - j, null);
                    Assert.assertTrue(queue.offer(urlScore));
                    flowProcess.increment(FetchCounters.URLS_QUEUED, 1);
                    flowProcess.increment(FetchCounters.URLS_REMAINING, 1);
                }

                while (!queueMgr.offer(queue)) {
                    Thread.sleep(10);
                }
            }

            // We have a bunch of pages to fetch. In a few milliseconds the
            // FetcherManager should have fired up all of the threads.
            Thread.sleep(500);
            Assert.assertEquals(manager.getActiveThreadCount(), NUM_THREADS);

            // Time to terminate everything.
            t.interrupt();
        } catch (Throwable t) {
            System.out.println("Exception during test: " + t.getMessage());
            t.printStackTrace();
            Assert.fail(t.getMessage());
        } finally {
            try {
            	if (server != null) {
            		server.stop();
            	}
            } catch (Throwable t) {
                // Ignore
            }
        }

    }
}
