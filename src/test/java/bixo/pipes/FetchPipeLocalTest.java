/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.pipes;

import org.junit.Test;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;


@SuppressWarnings("serial")
public class FetchPipeLocalTest extends AbstractFetchPipeTest {
    

    @Test
    public void testHeadersInStatus() throws Exception {
        testHeadersInStatus(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testFetchPipe() throws Exception {
        testFetchPipe(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testRedirectException() throws Exception {
        testRedirectException(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testTerminatingFetchPipe() throws Exception {
        testTerminatingFetchPipe(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testPayloads() throws Exception {
        testPayloads(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testSkippingURLsByScore() throws Exception {
        testSkippingURLsByScore(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testDurationLimitSimple() throws Exception {
        testDurationLimitSimple(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    @Test
    public void testMaxUrlsPerServer() throws Exception {
        testMaxUrlsPerServer(new BixoPlatform(FetchPipeLocalTest.class, Platform.Local));
    }
    
    // TODO KKr- re-enable this test when we know how to make it work for
    // the new fetcher architecture.
    /**
    @Test
    public void testPassingAllStatus() throws Exception {
        testPassingAllStatus(new BixoPlatform(Platform.Local));
    }
    */
    
}
