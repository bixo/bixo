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
package bixo.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;


public class TikaCallableTest {

    @SuppressWarnings("serial")
    private static class DelayParser implements Parser {
        private boolean _delay;
        private boolean _active;
        
        DelayParser(boolean delay) {
            _delay = delay;
        }
        
        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
            _active = true;
            while (_delay) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            _active = false;
        }

        @Override
        public Set<MediaType> getSupportedTypes(ParseContext arg0) {
            // TODO Auto-generated method stub
            return null;
        }
        
        public boolean isActive() {
            return _active;
        }
    }
    
    @Test
    public void testNotTerminating() throws Exception {
        DelayParser parser = new DelayParser(true);
        InputStream is = Mockito.mock(InputStream.class);
        Metadata md = Mockito.mock(Metadata.class);
        
        BaseContentExtractor contentExtractor = Mockito.mock(BaseContentExtractor.class);
        BaseLinkExtractor linkExtractor = Mockito.mock(BaseLinkExtractor.class);
        
        Callable<ParsedDatum> c = new TikaCallable(parser, contentExtractor, linkExtractor, is, md);
        FutureTask<ParsedDatum> task = new FutureTask<ParsedDatum>(c);
        Thread t = new Thread(task);
        t.start();

        try {
            task.get(1000, TimeUnit.MILLISECONDS);
            Assert.fail("Should have gotten a timeout");
        } catch (TimeoutException e) {
            task.cancel(true);
            t.interrupt();
        } finally {
            t = null;
        }
        
        // Verify that the thread is no longer active. We have to delay a bit so that
        // the thread can actually process the interrupt.
        Thread.sleep(200);
        Assert.assertFalse(parser.isActive());
    }
    
    @Test
    public void testTerminating() throws Exception {
        Parser parser = new DelayParser(false);
        InputStream is = Mockito.mock(InputStream.class);
        Metadata md = new Metadata();
        
        BaseContentExtractor contentExtractor = Mockito.mock(BaseContentExtractor.class);
        BaseLinkExtractor linkExtractor = Mockito.mock(BaseLinkExtractor.class);
        Mockito.when(linkExtractor.getLinks()).thenReturn(new Outlink[0]);
        
        Callable<ParsedDatum> c = new TikaCallable(parser, contentExtractor, linkExtractor, is, md);
        FutureTask<ParsedDatum> task = new FutureTask<ParsedDatum>(c);
        Thread t = new Thread(task);
        t.start();

        try {
            task.get(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            Assert.fail("Should have worked without a timeout");
        }
    }
}
