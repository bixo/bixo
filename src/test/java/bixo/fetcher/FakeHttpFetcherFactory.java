package bixo.fetcher;

public class FakeHttpFetcherFactory implements IHttpFetcherFactory {
    private boolean _randomFetching;
    private int _maxThreads;
    
    public FakeHttpFetcherFactory(boolean randomFetching, int maxThreads) {
        _randomFetching = randomFetching;
        _maxThreads = maxThreads;
    }
    
    @Override
    public IHttpFetcher newHttpFetcher() {
        return new FakeHttpFetcher(_randomFetching);
    }

    @Override
    public int getMaxThreads() {
        return _maxThreads;
    }

}
