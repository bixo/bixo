package bixo.fetcher;

public class FetchRequest {
    private final int _numUrls;
    private final long _nextRequestTime;
    
    public FetchRequest(int numUrls, long nextRequestTime) {
        _numUrls = numUrls;
        _nextRequestTime = nextRequestTime;
    }

    public int getNumUrls() {
        return _numUrls;
    }

    public long getNextRequestTime() {
        return _nextRequestTime;
    }
    
}
