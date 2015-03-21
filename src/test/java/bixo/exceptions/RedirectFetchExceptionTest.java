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
package bixo.exceptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;
import bixo.datum.UrlStatus;
import bixo.exceptions.RedirectFetchException.RedirectExceptionReason;


public class RedirectFetchExceptionTest {

    @Test
    public void testSerialization() throws IOException {
        RedirectFetchException e = new RedirectFetchException("url", "redirectedUrl", RedirectExceptionReason.TEMP_REDIRECT_DISALLOWED);
        
        ByteArrayOutputStream backingStore = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(backingStore);
        e.write(output);
        
        RedirectFetchException e2 = new RedirectFetchException();
        
        ByteArrayInputStream sourceBytes = new ByteArrayInputStream(backingStore.toByteArray());
        DataInputStream input = new DataInputStream(sourceBytes);
        e2.readFields(input);
        
        Assert.assertEquals("url", e2.getUrl());
        Assert.assertEquals("redirectedUrl", e2.getRedirectedUrl());
        Assert.assertEquals(RedirectExceptionReason.TEMP_REDIRECT_DISALLOWED, e2.getReason());
    }
    
    @Test
    public void testMappingToUrlStatus() throws Exception {
        RedirectFetchException e = new RedirectFetchException("url", "redirectedUrl", RedirectExceptionReason.TEMP_REDIRECT_DISALLOWED);
        Assert.assertEquals(UrlStatus.HTTP_REDIRECTION_ERROR, e.mapToUrlStatus());
        
        e = new RedirectFetchException("url", "redirectedUrl", RedirectExceptionReason.PERM_REDIRECT_DISALLOWED);
        Assert.assertEquals(UrlStatus.HTTP_MOVED_PERMANENTLY, e.mapToUrlStatus());
    }
}
