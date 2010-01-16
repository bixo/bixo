package bixo.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;

import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.fetcher.http.IHttpHeaders;
import bixo.utils.CharsetUtils;
import bixo.utils.HttpUtils;
import bixo.utils.IoUtils;

@SuppressWarnings("serial")
public class SimpleParser implements IParser {
    private static final Logger LOGGER = Logger.getLogger(SimpleParser.class);

    // We'll give Tika 20 seconds to parse the document before timing out.
    private static final long MAX_PARSE_DURATION = 20;
    
    private transient AutoDetectParser _parser;
    
    private synchronized void init() {
        if (_parser == null) {
            _parser = new AutoDetectParser();
        }
    }

    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) throws Exception {
        init();

        if (LOGGER.isTraceEnabled()) {
        	LOGGER.trace(String.format("Parsing %s", fetchedDatum.getBaseUrl()));
        }
        
        // Provide clues to the parser about the format of the content.
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, fetchedDatum.getBaseUrl());
        metadata.add(Metadata.CONTENT_TYPE, fetchedDatum.getContentType());
        metadata.add(Metadata.CONTENT_ENCODING, getCharset(fetchedDatum));
        String lang = fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_LANGUAGE);
        metadata.add(Metadata.CONTENT_LANGUAGE, lang);
        
        InputStream is = new ByteArrayInputStream(fetchedDatum.getContentBytes());

        try {
        	URL baseUrl = getContentLocation(fetchedDatum);
        	metadata.add(Metadata.CONTENT_LOCATION, baseUrl.toExternalForm());

            Callable<ParsedDatum> c = new TikaCallable(_parser, is, metadata);
            FutureTask<ParsedDatum> task = new FutureTask<ParsedDatum>(c);
            Thread t = new Thread(task);
            t.start();
            
            ParsedDatum result = task.get(MAX_PARSE_DURATION, TimeUnit.SECONDS);
            result.setMetaDataMap(fetchedDatum.getMetaDataMap());
            return result;
        } finally {
            IoUtils.safeClose(is);
        }
    }

    private URL getContentLocation(FetchedDatum fetchedDatum) throws MalformedURLException {
		URL baseUrl = new URL(fetchedDatum.getFetchedUrl());
		
		// See if we have a content location from the HTTP headers that we should use as
		// the base for resolving relative URLs in the document.
		String clUrl = fetchedDatum.getHeaders().getFirst(IHttpHeaders.CONTENT_LOCATION);
		if (clUrl != null) {
			// FUTURE KKr - should we try to keep processing if this step fails, but
			// refuse to resolve relative links?
			baseUrl = new URL(baseUrl, clUrl);
		}
		return baseUrl;
	}

    /**
     * Extract encoding from either explicit header, or from content-type
     * 
     * If a charset is returned, then it's a valid/normalized charset name that's
     * supported on this platform.
     * 
     * @param datum
     * @return charset in response headers, or null
     */
    private String getCharset(FetchedDatum datum) {
        String result = CharsetUtils.clean(datum.getHeaders().getFirst(IHttpHeaders.CONTENT_ENCODING));
        if (result == null) {
            result = CharsetUtils.clean(HttpUtils.getCharsetFromContentType(datum.getContentType()));
        }
        
        return result;
    }

}
