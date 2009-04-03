package bixo.fetcher;

import java.util.LinkedList;

@SuppressWarnings("serial")
public class FetchList extends LinkedList<FetchItem> {
    private FetcherQueue _fromQueue;
    
    public FetchList(FetcherQueue fromQueue) {
        super();
        
        _fromQueue = fromQueue;
    }
    
    public FetchList(FetcherQueue fromQueue, FetchItem item) {
        super();
        
        _fromQueue = fromQueue;
        add(item);
    }
    
    public void finished() {
        _fromQueue.release(this);
    }
    
    public String getDomain() {
        return _fromQueue.getDomain();
    }
}
