package bixo.exceptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;
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
}
