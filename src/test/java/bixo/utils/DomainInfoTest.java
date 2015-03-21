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
package bixo.utils;

import java.net.URISyntaxException;

import static org.junit.Assert.*;
import org.junit.Test;


public class DomainInfoTest {

    @Test
    public void testFunkyHostname() throws Exception {
        try {
            new DomainInfo("http://-subdomain.domain.com");
            fail("Should throw exception");
        } catch (URISyntaxException e) {
            // Valid.
        }
    }
    
    @Test
    public void doNotResolveTestDomain() throws Exception {
        String domain = DomainInfo.makeTestDomain(0);
        DomainInfo di = new DomainInfo("http://" + domain);
        assertEquals(di.getDomain(), di.getHostAddress());
    }
}
