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
package bixo.fetcher.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import bixo.fetcher.FetcherManager;
import bixo.fetcher.FetcherQueue;
import bixo.fetcher.FetcherQueueMgr;
import bixo.fetcher.HttpClientFactory;
import bixo.fetcher.beans.FetcherPolicy;
import bixo.tuple.FetchTuple;
import cascading.tuple.Tuple;

public class FetcherReducer implements Reducer<Tuple, Tuple, Tuple, Tuple> {
    private static Logger LOG = Logger.getLogger(FetcherReducer.class);
    
    private FetcherQueueMgr _queueMgr;
    private FetcherManager _fetcherMgr;
    private Thread _fetcherThread;
    private Reporter _lastReporter;
    private FetchCollector _collector;
    
    @Override
    public void configure(JobConf conf) {
        _queueMgr = new FetcherQueueMgr();
        _collector = new FetchCollector(conf);
        
        // TODO KKr- configure max threads in conf?
        int maxThreads = 10;
        _fetcherMgr = new FetcherManager(_queueMgr, new HttpClientFactory(maxThreads), _collector);
        
        _fetcherThread = new Thread(_fetcherMgr);
        _fetcherThread.setName("Fetcher manager");
        _fetcherThread.start();
    }


    @Override
    public void reduce(Tuple key, Iterator<Tuple> values, OutputCollector<Tuple, Tuple> collector, Reporter reporter) throws IOException {
        _lastReporter = reporter;
        
        try {
            // <key> is the PLD grouper, while each entry from <values> is a FetchTuple.
            String domain = key.getString(0);
            FetcherPolicy policy = new FetcherPolicy();
            
            // TODO KKr - base maxURLs on fetcher policy, target end of fetch
            int maxURLs = 10;
            
            FetcherQueue queue = new FetcherQueue(domain, policy, maxURLs);
            
            while (values.hasNext()) {
                FetchTuple item = new FetchTuple(values.next());
                queue.offer(item);
            }
            
            while (!_queueMgr.offer(queue)) {
                reporter.progress();
            }
        } catch (Throwable t) {
            LOG.error("Exception during reduce: " + t.getMessage(), t);
        }
    }


    @Override
    public void close() throws IOException {
        // We have a bunch of pages to "fetch". Spin until we're done.
        while (!_fetcherMgr.isDone()) {
            if (_lastReporter != null) {
                _lastReporter.progress();
            }
            
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {}
            
        }
        
        _fetcherThread.interrupt();
        _collector.close();
    }

}
