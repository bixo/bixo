package bixo.fetcher;

import junit.framework.TestCase;

public class FetcherPolicyTest extends TestCase {
	public final void testNoCrawlDelay() {
		FetcherPolicy policy = new FetcherPolicy(10, 1, 1);
		assertEquals(10, policy.getCrawlDelay());
		
		policy.setThreadsPerHost(2);
		assertEquals(0, policy.getCrawlDelay());
	}
	
	public final void testBean() {
		FetcherPolicy policy = new FetcherPolicy(10, 1, 2);
		assertEquals(10, policy.getCrawlDelay());
		assertEquals(1, policy.getThreadsPerHost());
		assertEquals(2, policy.getRequestsPerConnection());
	}
}
