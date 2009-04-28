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

import java.util.LinkedList;

import bixo.cascading.BixoFlowProcess;
import bixo.tuple.ScoredUrlDatum;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public class FetchList extends LinkedList<ScoredUrlDatum> {
    private FetcherQueue _fromQueue;
    private BixoFlowProcess _process;
    private TupleEntryCollector _collector;
    
    public FetchList(FetcherQueue fromQueue, BixoFlowProcess process, TupleEntryCollector collector) {
        super();
        _fromQueue = fromQueue;
        _process = process;
        _collector = collector;
    }

    public FetchList(FetcherQueue fromQueue, BixoFlowProcess process, TupleEntryCollector collector, ScoredUrlDatum item) {
        super();
        
        _fromQueue = fromQueue;
        _process = process;
        _collector = collector;
        add(item);
    }
    
    public void finished() {
        _fromQueue.release(this);
    }
    
    public String getDomain() {
        return _fromQueue.getDomain();
    }

    public BixoFlowProcess getProcess() {
        return _process;
    }

    public TupleEntryCollector getCollector() {
        return _collector;
    }
    
}
