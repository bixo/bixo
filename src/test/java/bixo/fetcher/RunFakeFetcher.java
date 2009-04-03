package bixo.fetcher;

import java.net.URL;
import java.util.Random;

public class RunFakeFetcher {
    
    public static void main(String[] args) {
        
        try {
            FetcherQueueMgr queueMgr = new FetcherQueueMgr();
            FetcherManager threadMgr = new FetcherManager(queueMgr, new FakeHttpFetcherFactory(true), 10);
            
            Thread t = new Thread(threadMgr);
            t.setName("Fetcher manager");
            t.start();
            
            // Now start creating per-domain queues and passing them to the FetcherQueueMgr
            FetcherPolicy policy = new FetcherPolicy();
            Random rand = new Random();

            for (int i = 0; i < 10; i++) {
                String host = "domain-" + i + ".com";
                policy.setCrawlDelay(1 + rand.nextInt(10));
                FetcherQueue queue = new FetcherQueue(host, policy, 100 - (i * 10));

                for (int j = 0; j < 20; j++) {
                    String file = "/page-" + j + ".html";
                    queue.offer(new URL("http", "www." + host, file), rand.nextFloat());
                }
                
                while (!queueMgr.offer(queue)) {
                    // Spin until it's accepted.
                }
            }
            
            // We have a bunch of pages to "fetch". Spin until we're done.
            while (!threadMgr.isDone()) {}
            
            t.interrupt();
        } catch (Throwable t) {
            System.err.println("Exception: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }
}
