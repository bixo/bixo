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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import bixo.fetcher.beans.FetchItem;
import bixo.fetcher.beans.FetcherPolicy;
import bixo.tuple.UrlWithScoreTuple;

public class RunFakeFetcher {

    public static void main(String[] args) {

        try {
            JobConf conf = new JobConf();
            FileOutputFormat.setOutputPath(conf, new Path("build/test-data/RunFakeFetcher/working"));

            FetcherQueueMgr queueMgr = new FetcherQueueMgr();
            FetcherManager threadMgr = new FetcherManager(queueMgr, new FakeHttpFetcherFactory(true, 10));

            Thread t = new Thread(threadMgr);
            t.setName("Fetcher manager");
            t.start();

            // Now start creating per-domain queues and passing them to the
            // FetcherQueueMgr
            FetcherPolicy policy = new FetcherPolicy();
            Random rand = new Random();

            for (int i = 0; i < 10; i++) {
                String host = "domain-" + i + ".com";
                policy.setCrawlDelay(1 + rand.nextInt(10));
                FetcherQueue queue = new FetcherQueue(host, policy, 100 - (i * 10));

                for (int j = 0; j < 20; j++) {
                    String file = "/page-" + j + ".html";

                    UrlWithScoreTuple urlScore = new UrlWithScoreTuple();
                    urlScore.setUrl("http://www." + host + file);
                    urlScore.SetScore(rand.nextFloat());
                    FetchItem fetchItem = new FetchItem(urlScore, new FakeCollector());

                    queue.offer(fetchItem);
                }

                while (!queueMgr.offer(queue)) {
                    // Spin until it's accepted.
                }
            }

            // We have a bunch of pages to "fetch". Spin until we're done.
            while (!threadMgr.isDone()) {
            }

            t.interrupt();
        } catch (Throwable t) {
            System.err.println("Exception: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    private static class FakeCollector extends TupleEntryCollector {

        @Override
        protected void collect(Tuple tuple) {
            System.out.println(tuple.toString());
        }

    }

}
