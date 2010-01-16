package bixo.config;

import java.io.Serializable;
import java.security.InvalidParameterException;

import bixo.fetcher.FetcherQueueMgr;

/**
 * Policy used by FetcherQueueMgr to decide on bounds for FetcherQueues
 *
 */
@SuppressWarnings("serial")
public class QueuePolicy implements Serializable {

    private int _maxUrlsInMemory;
    private int _maxUrlsInMemoryPerQueue;
    
    /**
     * Default constructor useful for testing when we don't care about exact values.
     * 
     */
    public QueuePolicy() {
        init(FetcherQueueMgr.DEFAULT_MAX_URLS_IN_MEMORY, new FetcherPolicy().getDefaultUrlsPerRequest());
    }
    
    public QueuePolicy(int maxUrlsInMemory, FetcherPolicy fetcherPolicy) {
        init(maxUrlsInMemory, fetcherPolicy.getDefaultUrlsPerRequest());
    }
    
    public QueuePolicy(int maxUrlsInMemory, int maxUrlsInMemoryPerQueue) {
        init(maxUrlsInMemory, maxUrlsInMemoryPerQueue);
    }
    
    private void init(int maxUrlsInMemory, int maxUrlsInMemoryPerQueue) {
        if (maxUrlsInMemory <= 0) {
            throw new InvalidParameterException("maxUrlsInMemory must be > 0");
        }
        if (maxUrlsInMemoryPerQueue <= 0) {
            throw new InvalidParameterException("maxUrlsInMemoryPerQueue must be > 0");
        }
        if (maxUrlsInMemory < maxUrlsInMemoryPerQueue) {
            throw new InvalidParameterException("maxUrlsInMemory must be > maxUrlsInMemoryPerQueue");
        }
        
        _maxUrlsInMemory = maxUrlsInMemory;
        _maxUrlsInMemoryPerQueue = maxUrlsInMemoryPerQueue;
    }

    public int getMaxUrlsInMemory() {
        return _maxUrlsInMemory;
    }

    public int getMaxUrlsInMemoryPerQueue() {
        return _maxUrlsInMemoryPerQueue;
    }
    
    public int getMaxQueues() {
        return getMaxUrlsInMemory() / getMaxUrlsInMemoryPerQueue();
    }
}
