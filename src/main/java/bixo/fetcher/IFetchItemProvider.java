package bixo.fetcher;


public interface IFetchItemProvider {
    
	/**
	 * @return - next list of items to fetch, or null if nothing is available.
	 */
	public FetchList poll();
	
	
	/**
	 * @return - true if this provider has nothing more to offer, and thus
	 *           all calls to poll() will return null.
	 */
	public boolean isEmpty();
}
