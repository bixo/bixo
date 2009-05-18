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

import java.util.Random;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchStatusCode;
import bixo.datum.ScoredUrlDatum;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class RunFakeFetcher {
    private static final Logger LOGGER = Logger.getLogger(RunFakeFetcher.class);
    
    public static void main(String[] args) {

        try {
            BixoFlowProcess flowProcess = new BixoFlowProcess();
            FetcherQueueMgr queueMgr = new FetcherQueueMgr(flowProcess);
            FetcherManager threadMgr = new FetcherManager(queueMgr, new FakeHttpFetcher(true, 4), flowProcess);

            Thread t = new Thread(threadMgr);
            t.setName("Fetcher manager");
            t.start();

            // Now start creating per-domain queues and passing them to the
            // FetcherQueueMgr
            Random rand = new Random();

            for (int i = 0; i < 10; i++) {
                String host = "domain-" + i + ".com";
                int delay = 0 + rand.nextInt(5);

                LOGGER.trace(String.format("Creating queue for %s with delay %d", host, delay));

                FetcherPolicy policy = new FetcherPolicy();
                policy.setCrawlDelay(delay);
                FetcherQueue queue = new FetcherQueue(host, policy, flowProcess, new FakeCollector());

                for (int j = 0; j < 5; j++) {
                    String file = "/page-" + j + ".html";

                    String url = "http://www." + host + file;
                    ScoredUrlDatum urlScore = new ScoredUrlDatum(url, 0,0, FetchStatusCode.UNFETCHED, url, null, rand.nextFloat(), null);
                    queue.offer(urlScore);
                }

                while (!queueMgr.offer(queue)) {
                    // Spin until it's accepted.
                }
            }

            // We have a bunch of pages to "fetch". Spin until we're done.
            while (!threadMgr.isDone()) {
            }

            t.interrupt();
            
            flowProcess.dumpCounters();
        } catch (Throwable t) {
            System.err.println("Exception: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    private static class FakeCollector extends TupleEntryCollector {

        @Override
        protected void collect(Tuple tuple) {
            // TODO KKr - reenable this when we have a better toString for the tuple, where it limits the
            // amount of data and avoids printing control characters.
            // System.out.println(tuple.toString());
        }

    }

}
