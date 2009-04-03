package bixo.fetcher;

public interface IHttpFetcherFactory {
    IHttpFetcher newHttpFetcher();
    
    int getMaxThreads();
}
