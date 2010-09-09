package bixo.exceptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import bixo.datum.UrlStatus;


public class BaseFetchExceptionTest {
	
	@SuppressWarnings("serial")
	@Test
	public void testSerialization() throws IOException {
		FetchException e = new FetchException("url", "msg") {

			@Override
			public UrlStatus mapToUrlStatus() {
				return UrlStatus.UNFETCHED;
			}
		};
		
		ByteArrayOutputStream backingStore = new ByteArrayOutputStream();
		DataOutputStream output = new DataOutputStream(backingStore);
		e.writeBaseFields(output);
		
		FetchException e2 = new FetchException() {

			@Override
			public UrlStatus mapToUrlStatus() {
				return UrlStatus.UNFETCHED;
			}
		};
		
		ByteArrayInputStream sourceBytes = new ByteArrayInputStream(backingStore.toByteArray());
		DataInputStream input = new DataInputStream(sourceBytes);
		e2.readBaseFields(input);
		
		Assert.assertEquals("msg", e2.getMessage());
		Assert.assertEquals("url", e2.getUrl());
	}
}
