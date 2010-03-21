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

import bixo.datum.ParsedDatum;


public class TikaCallableTest {

    private static class DelayParser implements Parser {
        private boolean _delay;
        
        DelayParser(boolean delay) {
            _delay = delay;
        }
        
        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata) throws IOException, SAXException, TikaException {
            parse(stream, handler, metadata, new ParseContext());
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
            while (_delay) {
            }
        }

        @Override
        public Set<MediaType> getSupportedTypes(ParseContext arg0) {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    @Test
    public void testNotTerminating() throws Exception {
        Parser parser = new DelayParser(true);
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
            // Valid
        }
    }
    
    @Test
    public void testTerminating() throws Exception {
        Parser parser = new DelayParser(false);
        InputStream is = Mockito.mock(InputStream.class);
        Metadata md = new Metadata();
        
        BaseContentExtractor contentExtractor = Mockito.mock(BaseContentExtractor.class);
        BaseLinkExtractor linkExtractor = Mockito.mock(BaseLinkExtractor.class);
        
        Callable<ParsedDatum> c = new TikaCallable(parser, contentExtractor, linkExtractor, is, md);
        FutureTask<ParsedDatum> task = new FutureTask<ParsedDatum>(c);
        Thread t = new Thread(task);
        t.start();

        try {
            task.get(1000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            Assert.fail("Should have worked without a timeout");
        }
    }
}
