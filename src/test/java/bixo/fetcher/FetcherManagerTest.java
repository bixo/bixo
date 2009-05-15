package bixo.fetcher;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.http.HttpServer;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.HttpClientFetcher;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.simulation.SimulationWebServer;
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
        final int NUM_THREADS = 100;

        HttpServer server = null;

        try {
            server = startServer(new SlowResponseHandler(20000, 100 * 1000L), 8089);

            BixoFlowProcess flowProcess = new BixoFlowProcess();
            FetcherQueueMgr queueMgr = new FetcherQueueMgr(flowProcess);
            FetcherManager threadMgr = new FetcherManager(queueMgr, new HttpClientFetcher(NUM_THREADS), flowProcess);

            Thread t = new Thread(threadMgr);
            t.setName("Fetcher manager");
            t.start();

            for (int i = 0; i < 200; i++) {
                String host = "domain-" + i + ".com";
                FetcherPolicy policy = new FetcherPolicy();
                policy.setMinResponseRate(0);
                FetcherQueue queue = new FetcherQueue(host, policy, 100, flowProcess, new FakeCollector());

                for (int j = 0; j < 2; j++) {
                    String file = "/page-" + j + ".html";

                    String url = "http://localhost:8089" + file;
                    ScoredUrlDatum urlScore = new ScoredUrlDatum(url, 0, 0, FetchStatusCode.UNFETCHED, url, null, 1.0f - (float) j, null);
                    queue.offer(urlScore);
                }

                while (!queueMgr.offer(queue)) {
                    // Spin until it's accepted.
                }
            }

            // We have a bunch of pages to fetch. In a few milliseconds the
            // FetcherManager should have
            // fired up all of the threads. The ThreadPool seems to have up to
            // core+max threads, so we're
            // just doing a general test of the count here.
            Thread.sleep(1000);
            int activeThreads = flowProcess.getCounter(FetcherCounters.URLS_FETCHING);
            Assert.assertTrue(activeThreads >= NUM_THREADS);

            // Time to terminate everything.
            t.interrupt();
        } catch (Throwable t) {
            Assert.fail(t.getMessage());
        } finally {
            try {
                server.stop();
            } catch (Throwable t) {
                // Ignore
            }
        }

    }
}
