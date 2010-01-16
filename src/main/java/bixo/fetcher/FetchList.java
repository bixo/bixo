package bixo.fetcher;

import java.util.Iterator;
import java.util.List;

import bixo.cascading.BixoFlowProcess;
import bixo.datum.ScoredUrlDatum;
import cascading.tuple.TupleEntryCollector;

public class FetchList {
    private BixoFlowProcess _process;
    private TupleEntryCollector _collector;
    private FetcherQueueMgr _queueMgr;
    private String _domain;
    private List<ScoredUrlDatum> _urls;
    
    
    public FetchList(BixoFlowProcess process, TupleEntryCollector collector, FetcherQueueMgr queueMgr, String domain, List<ScoredUrlDatum> urls) {
        _process = process;
        _collector = collector;
        _queueMgr = queueMgr;
        _domain = domain;
        _urls = urls;
    }

    public List<ScoredUrlDatum> getUrls() {
        return _urls;
    }
    
    public boolean add(ScoredUrlDatum e) {
        return _urls.add(e);
    }

    public ScoredUrlDatum get(int index) {
        return _urls.get(index);
    }

    public Iterator<ScoredUrlDatum> iterator() {
        return _urls.iterator();
    }

    public int size() {
        return _urls.size();
    }

    public void finished() {
        _queueMgr.finished(this);
    }
    
    public String getDomain() {
        return _domain;
    }

    public BixoFlowProcess getProcess() {
        return _process;
    }

    public TupleEntryCollector getCollector() {
        return _collector;
    }
    
    
}
