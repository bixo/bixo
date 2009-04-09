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

import bixo.fetcher.beans.FetcherPolicy;
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
