/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bixo.parser.html;

import java.util.ArrayList;
import java.util.Map;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.io.*;
import java.util.regex.*;

import org.cyberneko.html.parsers.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.*;
import org.apache.html.dom.*;

import org.apache.log4j.Logger;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.parser.IParse;
import bixo.parser.IParser;
import bixo.parser.ParseData;
import bixo.parser.ParseImpl;
import bixo.parser.ParseResult;
import bixo.parser.ParseStatus;
import bixo.utils.EncodingDetector;
import bixo.utils.Metadata;

public class HtmlParser implements IParser {
    public static final Logger LOGGER = Logger.getLogger(HtmlParser.class);

    // I used 1000 bytes at first, but  found that some documents have 
    // meta tag well past the first 1000 bytes. 
    // (e.g. http://cn.promo.yahoo.com/customcare/music.html)
    private static final int CHUNK_SIZE = 2000;
    private static Pattern metaPattern =
        Pattern.compile("<meta\\s+([^>]*http-equiv=\"?content-type\"?[^>]*)>", Pattern.CASE_INSENSITIVE);
    private static Pattern charsetPattern = Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)",
                        Pattern.CASE_INSENSITIVE);

    private String defaultCharEncoding;
    private DOMContentUtils utils;
    private String cachingPolicy;

    public HtmlParser() {
        // TODO KKr - set these from conf
        this.defaultCharEncoding = "windows-1252";
        this.cachingPolicy = IBixoMetaKeys.CACHING_FORBIDDEN_CONTENT;

        this.utils = new DOMContentUtils();
    }

    /**
     * Given a <code>byte[]</code> representing an html file of an 
     * <em>unknown</em> encoding,  read out 'charset' parameter in the meta tag   
     * from the first <code>CHUNK_SIZE</code> bytes.
     * If there's no meta tag for Content-Type or no charset is specified,
     * <code>null</code> is returned.  <br />
     * FIXME: non-byte oriented character encodings (UTF-16, UTF-32)
     * can't be handled with this. 
     * We need to do something similar to what's done by mozilla
     * (http://lxr.mozilla.org/seamonkey/source/parser/htmlparser/src/nsParser.cpp#1993).
     * See also http://www.w3.org/TR/REC-xml/#sec-guessing
     * <br />
     *
     * @param content <code>byte[]</code> representation of an html file
     */

    private static String sniffCharacterEncoding(byte[] content) {
        int length = content.length < CHUNK_SIZE ? 
                        content.length : CHUNK_SIZE;

        // We don't care about non-ASCII parts so that it's sufficient
        // to just inflate each byte to a 16-bit value by padding. 
        // For instance, the sequence {0x41, 0x82, 0xb7} will be turned into 
        // {U+0041, U+0082, U+00B7}. 
        String str = "";
        try {
            str = new String(content, 0, length, Charset.forName("ASCII").toString());
        } catch (UnsupportedEncodingException e) {
            // code should never come here, but just in case... 
            return null;
        }

        Matcher metaMatcher = metaPattern.matcher(str);
        String encoding = null;
        if (metaMatcher.find()) {
            Matcher charsetMatcher = charsetPattern.matcher(metaMatcher.group(1));
            if (charsetMatcher.find()) 
                encoding = new String(charsetMatcher.group(1));
        }

        return encoding;
    }

    public ParseResult getParse(FetchedDatum fetchedDatum) {
        HTMLMetaTags metaTags = new HTMLMetaTags();

        URL base;
        try {
            base = new URL(fetchedDatum.getBaseUrl());
        } catch (MalformedURLException e) {
            return new ParseStatus(e).getEmptyParseResult(fetchedDatum.getBaseUrl());
        }

        String text = "";
        String title = "";
        Outlink[] outlinks = new Outlink[0];
        Metadata metadata = new Metadata();

        // parse the content
        DocumentFragment root;
        try {
            byte[] contentInOctets = fetchedDatum.getContent().getBytes();
            InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));

            EncodingDetector detector = new EncodingDetector();
            // TODO KKr - get contentType from content
            detector.autoDetectClues(fetchedDatum, "xxx", true);
            detector.addClue(sniffCharacterEncoding(contentInOctets), "sniffed");
            String encoding = detector.guessEncoding(fetchedDatum, defaultCharEncoding);

            metadata.set(Metadata.ORIGINAL_CHAR_ENCODING_KEY, encoding);
            metadata.set(Metadata.CHAR_ENCODING_FOR_CONVERSION_KEY, encoding);

            input.setEncoding(encoding);
            if (LOGGER.isTraceEnabled()) { LOGGER.trace("Parsing..."); }
            root = parse(input);
        } catch (IOException e) {
            return new ParseStatus(e).getEmptyParseResult(fetchedDatum.getBaseUrl());
        } catch (DOMException e) {
            return new ParseStatus(e).getEmptyParseResult(fetchedDatum.getBaseUrl());
        } catch (SAXException e) {
            return new ParseStatus(e).getEmptyParseResult(fetchedDatum.getBaseUrl());
        } catch (Exception e) {
            LOGGER.error("Exception parsing HTML", e);
            return new ParseStatus(e).getEmptyParseResult(fetchedDatum.getBaseUrl());
        }

        // get meta directives
        HTMLMetaProcessor.getMetaTags(metaTags, root, base);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Meta tags for " + base + ": " + metaTags.toString());
        }
        
        // check meta directives
        if (!metaTags.getNoIndex()) {               // okay to index
            StringBuffer sb = new StringBuffer();
            LOGGER.trace("Getting text...");
            utils.getText(sb, root);          // extract text
            text = sb.toString();
            sb.setLength(0);
            LOGGER.trace("Getting title...");
            utils.getTitle(sb, root);         // extract title
            title = sb.toString().trim();
        }

        if (!metaTags.getNoFollow()) {              // okay to follow links
            ArrayList<Outlink> l = new ArrayList<Outlink>();   // extract outlinks
            URL baseTag = utils.getBase(root);
            if (LOGGER.isTraceEnabled()) { LOGGER.trace("Getting links..."); }
            utils.getOutlinks(baseTag!=null?baseTag:base, l, root);
            outlinks = l.toArray(new Outlink[l.size()]);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("found "+outlinks.length+" outlinks in "+fetchedDatum.getBaseUrl());
            }
        }

        ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
        if (metaTags.getRefresh()) {
            status.setMinorCode(ParseStatus.SUCCESS_REDIRECT);
            status.setArgs(new String[] {metaTags.getRefreshHref().toString(),
                            Integer.toString(metaTags.getRefreshTime())});      
        }
        
        // TODO KKr - figure out content metadata vs. metadata here.
        Metadata md = new Metadata();
        ParseData parseData = new ParseData(status, title, outlinks, md, metadata);
        ParseResult parseResult = ParseResult.createParseResult(fetchedDatum.getBaseUrl(), new ParseImpl(text, parseData));

        // TODO KKr - how do we want to handle parse meta-data?
        if (metaTags.getNoCache()) {             // not okay to cache
            for (Map.Entry<org.apache.hadoop.io.Text, IParse> entry : parseResult) 
                entry.getValue().getData().getParseMeta().set(IBixoMetaKeys.CACHING_FORBIDDEN_KEY, 
                                cachingPolicy);
        }
        return parseResult;
    }

    private DocumentFragment parse(InputSource input) throws Exception {
        return parseNeko(input);
    }

    private DocumentFragment parseNeko(InputSource input) throws Exception {
        DOMFragmentParser parser = new DOMFragmentParser();
        
        try {
            parser.setFeature("http://cyberneko.org/html/features/augmentations", true);
            parser.setProperty("http://cyberneko.org/html/properties/default-encoding", defaultCharEncoding);
            parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset", true);
            parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content", false);
            parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment", true);
            parser.setFeature("http://cyberneko.org/html/features/report-errors", LOGGER.isTraceEnabled());
        } catch (SAXException e) {
            LOGGER.debug("SAXException thrown while setting features", e);
        }
        
        // convert Document to DocumentFragment
        HTMLDocumentImpl doc = new HTMLDocumentImpl();
        doc.setErrorChecking(false);
        DocumentFragment res = doc.createDocumentFragment();
        DocumentFragment frag = doc.createDocumentFragment();
        parser.parse(input, frag);
        res.appendChild(frag);

        try {
            while (true) {
                frag = doc.createDocumentFragment();
                parser.parse(input, frag);
                if (!frag.hasChildNodes()) break;
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
                }
                
                res.appendChild(frag);
            }
        } catch (Exception e) {
            LOGGER.error("Exception parsing HTML", e);
        }
        
        return res;
    }

    
    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) {
        ParseResult result = getParse(fetchedDatum);
        IParse p = result.get(fetchedDatum.getBaseUrl());        
        return new ParsedDatum(fetchedDatum.getBaseUrl(), p.getText(), p.getData().getOutlinks(), fetchedDatum.getMap());
    }

}
