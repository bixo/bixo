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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.ParserPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.utils.IoUtils;

@SuppressWarnings("serial")
public class SimpleParser extends BaseParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleParser.class);

    /**
     * Fixed version of Tika 1.0's IdentityHtmlMapper
     */
    private static class FixedIdentityHtmlMapper extends IdentityHtmlMapper implements Serializable {

        public static final HtmlMapper INSTANCE = new FixedIdentityHtmlMapper();

        @Override
        public String mapSafeElement(String name) {
            return name.toLowerCase(Locale.ENGLISH);
        }
    }

    private boolean _extractLanguage = true;
    protected BaseContentExtractor _contentExtractor;
    protected BaseLinkExtractor _linkExtractor;
    protected ParseContext _parseContext;
    private transient Parser _parser;
    
    public SimpleParser() {
        this(new ParserPolicy());
    }
    
    public SimpleParser(ParserPolicy parserPolicy) {
        this(new SimpleContentExtractor(), new SimpleLinkExtractor(), parserPolicy, null);
    }
    
    /**
     * @param contentExtractor to use instead of new {@link SimpleContentExtractor}()
     * @param linkExtractor to use instead of new {@link SimpleLinkExtractor}()
     * @param parserPolicy to customize operation of the parser
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * and attributes it processes. Instead, use {@link ParserPolicy#setLinkTags}
     * and {@link ParserPolicy#setLinkAttributeTypes}, and then pass this policy
     * to {@link SimpleParser#SimpleParser(ParserPolicy)}.
     */
    public SimpleParser(BaseContentExtractor contentExtractor, BaseLinkExtractor linkExtractor, ParserPolicy parserPolicy) {
        this(contentExtractor, linkExtractor, parserPolicy, null);
    }
    
    /**
     * @param parserPolicy to customize operation of the parser
     * @param includeMarkup true if output should be raw HTML, versus extracted text
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * and attributes it processes. Instead, use {@link ParserPolicy#setLinkTags}
     * and {@link ParserPolicy#setLinkAttributeTypes}, and then pass this policy
     * to {@link SimpleParser#SimpleParser(ParserPolicy)}.
     */
    public SimpleParser(ParserPolicy parserPolicy, boolean includeMarkup) {
        this(includeMarkup ? new HtmlContentExtractor() : new SimpleContentExtractor(),
             new SimpleLinkExtractor(),
             parserPolicy, includeMarkup);
    }

    /**
     * @param parserPolicy to customize operation of the parser
     * @param includeMarkup true if output should be raw HTML, versus extracted text
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * and attributes it processes. Instead, use {@link ParserPolicy#setLinkTags}
     * and {@link ParserPolicy#setLinkAttributeTypes}, and then pass this policy
     * to {@link SimpleParser#SimpleParser(ParserPolicy)}.
     */
    public SimpleParser(BaseContentExtractor contentExtractor, BaseLinkExtractor linkExtractor, ParserPolicy parserPolicy, boolean includeMarkup) {
        super(parserPolicy);

        _contentExtractor = contentExtractor;
        _linkExtractor = linkExtractor;

        if (includeMarkup) {
            _parseContext = new ParseContext();
            _parseContext.set(HtmlMapper.class, FixedIdentityHtmlMapper.INSTANCE);
        }
    }

    /**
     * @param contentExtractor to use instead of new {@link SimpleContentExtractor}()
     * @param linkExtractor to use instead of new {@link SimpleLinkExtractor}()
     * @param parserPolicy to customize operation of the parser
     * @param parseContext used to pass context info to the parser
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * and attributes it processes. Instead, use {@link ParserPolicy#setLinkTags}
     * and {@link ParserPolicy#setLinkAttributeTypes}, and then pass this policy
     * to {@link SimpleParser#SimpleParser(ParserPolicy)}.
     */
    public SimpleParser(BaseContentExtractor contentExtractor, BaseLinkExtractor linkExtractor, ParserPolicy parserPolicy, ParseContext parseContext) {
        super(parserPolicy);
        
        _contentExtractor = contentExtractor;
        _linkExtractor = linkExtractor;
        _parseContext = parseContext;
    }

    protected synchronized void init() {
        if (_parser == null) {
            _parser = getTikaParser();
        }
        
        _contentExtractor.reset();
        _linkExtractor.setLinkTags(getParserPolicy().getLinkTags());
        _linkExtractor.setLinkAttributeTypes(getParserPolicy().getLinkAttributeTypes());
        _linkExtractor.reset();
    }

    public Parser getTikaParser() {
        return new AutoDetectParser();
    }

    public void setExtractLanguage(boolean extractLanguage) {
        _extractLanguage = extractLanguage;
    }
    
    public boolean isExtractLanguage() {
        return _extractLanguage;
    }
    
    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) throws Exception {
        init();

        if (LOGGER.isTraceEnabled()) {
        	LOGGER.trace(String.format("Parsing %s", fetchedDatum.getUrl()));
        }
        
        // Provide clues to the parser about the format of the content.
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, fetchedDatum.getUrl());
        metadata.add(Metadata.CONTENT_TYPE, fetchedDatum.getContentType());
        String charset = getCharset(fetchedDatum);
        metadata.add(Metadata.CONTENT_LANGUAGE, getLanguage(fetchedDatum, charset));
        
        InputStream is = new ByteArrayInputStream(fetchedDatum.getContentBytes(), 0, fetchedDatum.getContentLength());

        try {
        	URL baseUrl = getContentLocation(fetchedDatum);
        	metadata.add(Metadata.CONTENT_LOCATION, baseUrl.toExternalForm());

            Callable<ParsedDatum> c = new TikaCallable(_parser, _contentExtractor, _linkExtractor, is, metadata, isExtractLanguage(), _parseContext);
            FutureTask<ParsedDatum> task = new FutureTask<ParsedDatum>(c);
            Thread t = new Thread(task);
            t.start();
            
            ParsedDatum result;
            try {
                result = task.get(getParserPolicy().getMaxParseDuration(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                task.cancel(true);
                t.interrupt();
                throw e;
            } finally {
                t = null;
            }
            
            // TODO KKr Should there be a BaseParser to take care of copying
            // these two fields?
            result.setHostAddress(fetchedDatum.getHostAddress());
            result.setPayload(fetchedDatum.getPayload());
            return result;
        } finally {
            IoUtils.safeClose(is);
        }
    }

}
