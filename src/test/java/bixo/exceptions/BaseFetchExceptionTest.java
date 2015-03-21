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

import junit.framework.Assert;

import org.junit.Test;

import bixo.datum.UrlStatus;


public class BaseFetchExceptionTest {
	
	@SuppressWarnings("serial")
	@Test
	public void testSerialization() throws IOException {
		BaseFetchException e = new BaseFetchException("url", "msg") {

			@Override
			public UrlStatus mapToUrlStatus() {
				return UrlStatus.UNFETCHED;
			}
		};
		
		ByteArrayOutputStream backingStore = new ByteArrayOutputStream();
		DataOutputStream output = new DataOutputStream(backingStore);
		e.writeBaseFields(output);
		
		BaseFetchException e2 = new BaseFetchException() {

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
