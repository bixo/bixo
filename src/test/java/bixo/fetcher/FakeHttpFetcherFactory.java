package bixo.fetcher;

public class FakeHttpFetcherFactory implements IHttpFetcherFactory {
    private boolean _randomFetching;
    
    public FakeHttpFetcherFactory(boolean randomFetching) {
        _randomFetching = randomFetching;
    }
    
    @Override
    public IHttpFetcher newHttpFetcher() {
        return new FakeHttpFetcher(_randomFetching);
    }

}
