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
package bixo.examples.webmining;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.NullContext;

import bixo.config.BixoPlatform;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.examples.crawl.SimpleBodyContentHandler;
import bixo.parser.DOMParser;
import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;


@SuppressWarnings("serial")
public class AnalyzeHtml extends DOMParser {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzeHtml.class);

    private static final Pattern IMG_SUFFIX_EXCLUSION_PATTERN =
        Pattern.compile("(?i)\\.(gif|jpg|jpeg|bmp|png|ico)$");

    private static final int MAX_WORDS_PER_PHRASE = 2;

    // These are all transient since we don't want to serialize them when the
    // Cascading job is submitted, so we set them up in the prepare() method.
    private transient PhraseShingleAnalyzer _analyzer;
    private transient Set<String> _positivePhrases;
    private transient Set<String> _negativePhrases;

    private transient AnalyzedDatum _result;
    
    public AnalyzeHtml() {
        super(AnalyzedDatum.FIELDS); 
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);
        
        // Load the positive and negative phrases.
        // Analyze them using the standard analyzer (no stopwords)
        // TODO Maybe figure out the max # of words, for shingling? For now use a constant.
        _analyzer = new PhraseShingleAnalyzer(MAX_WORDS_PER_PHRASE);
        _positivePhrases = loadAnalyzedPhrases("/positive-phrases.txt", _analyzer);
        _negativePhrases = loadAnalyzedPhrases("/negative-phrases.txt", _analyzer);
        
        _result = new AnalyzedDatum("", 0.0f, new PageResult[0], new Outlink[0]);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    protected void process(ParsedDatum datum, Document doc, TupleEntryCollector collector,
                    FlowProcess process) throws Exception {
        SimpleBodyContentHandler bodyContentHandler = new SimpleBodyContentHandler();
        SAXWriter writer = new SAXWriter(bodyContentHandler);
        writer.write(doc);

        float pageScore = getScore(bodyContentHandler.toString());
        
        // Get the outlinks.
        Outlink[] outlinks = getOutlinks(doc);

        // Extract all of the images, and use them as page results.
        PageResult[] pageResults = extractImages(datum.getUrl(), doc, outlinks);
        
        _result.setUrl(datum.getUrl());
        _result.setPageScore(pageScore);
        _result.setOutlinks(outlinks);
        _result.setPageResults(pageResults);
        
        collector.add(BixoPlatform.clone(_result.getTuple(), process));
    }

    @Override
    protected void handleException(ParsedDatum datum, Exception e, TupleEntryCollector collector) {
        // We'll just log it here, though normally we'd want to rethrow the exception, and
        // have our workflow set up to trap it.
        LOGGER.error("Exception parsing/processing " + datum.getUrl(), e);
        
    }

    private Outlink[] getOutlinks(Document doc) {
        ArrayList<Outlink> outlinkList = new ArrayList<Outlink>();
        List<Node> aNodes = getNodes(doc, "//a");
        
        
        for (Node node : aNodes) {
            String url = getAttributeFromNode(node, "href");
            String anchor = getAttributeFromNode(node, "name");
            String rel = getAttributeFromNode(node, "rel");
            Outlink link = new Outlink(url, anchor, rel);
            outlinkList.add(link);
        }
    
        return outlinkList.toArray(new Outlink[outlinkList.size()]);
    }

    private PageResult[] extractImages(String sourceUrl, Document doc, Outlink[] outlinks) {
        ArrayList<PageResult> pageResults = new ArrayList<PageResult>();
        // Find if we have image links that may have extracted as an Outlink
        for (Outlink outlink : outlinks) {
            String outlinkUrl = outlink.getToUrl();
            if (isImgSuffix(outlinkUrl)) {
                // TODO Maybe set description to any words found in image name? Change '-' and '_' to spaces?
                PageResult result = new PageResult(sourceUrl, outlinkUrl, "");
                pageResults.add(result);

            }
        }
        // Next extract all img 
        List<Node> imgNodes = getNodes(doc, "//img");
        for (Node node : imgNodes) {
            String src = getAttributeFromNode(node, "src");
            String alt = getAttributeFromNode(node, "alt");
            PageResult result = new PageResult(sourceUrl, src, alt);
            pageResults.add(result);
        }

        return pageResults.toArray(new PageResult[pageResults.size()]);
    }

    private String getAttributeFromNode(Node node, String attribute) {
        String attributeValue = null;
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            Element e = (Element)node;
            attributeValue = e.attributeValue(attribute);
        } 
        return  (attributeValue == null ? "" : attributeValue);
    }

    /**
     * Utility routine to get back a list of nodes from the HTML page document,
     * which match the provided XPath expression.
     * 
     * @param xPath expression to match
     * @return array of matching nodes, or an empty array if nothing matches
     * @throws ExtractionException
     */
    @SuppressWarnings("unchecked")
    private List<Node> getNodes(Node node, String xPath) {
        List<Node> result = node.selectNodes(xPath);
        if (result == null) {
            result = new ArrayList<Node>();
        }
        
        return result;
    }
    
    private static boolean isImgSuffix(String url) {
        Matcher m = IMG_SUFFIX_EXCLUSION_PATTERN.matcher(url);
       if (m.find()) {
           return true;
       }
       return false;
    }


    /* Calculate the positive term ratio (positive term count/total term count)
     * Do the same thing for the negative terms.
     * The score is the positive ratio - the negative ratio
     */
    private float getScore(String extractedContent) throws Exception {
        List<String> allTerms = _analyzer.getTermList(extractedContent);
        
        int positiveCount = 0;
        int negativeCount = 0;
        int neutralCount = 0;
        
        for (String term : allTerms) {
            if (_positivePhrases.contains(term)) {
                positiveCount += 1;
            } else if (_negativePhrases.contains(term)) {
                negativeCount += 1;
            } else {
                neutralCount += 1;
            }
        }
        
        float totalCount = (float)(positiveCount + negativeCount + neutralCount);
        float positiveRatio = 0;
        float negativeRatio = 0;
        if (totalCount > 0) {
            positiveRatio = (float)positiveCount/totalCount;
            negativeRatio = (float)negativeCount/totalCount;
        }
        
        return positiveRatio - negativeRatio;
    }


    private Set<String> loadAnalyzedPhrases(String fileName, PhraseShingleAnalyzer analyzer) {
        InputStream is = AnalyzeHtml.class.getResourceAsStream(fileName);
        Set<String> result = new HashSet<String>();
        
        try {
            List<String> lines = IOUtils.readLines(is);
            for (String line : lines) {
                if (line.trim().startsWith("#")) {
                    continue;
                }
                
                String analyzedPhrase = _analyzer.getAnalyzedPhrase(line);
                result.add(analyzedPhrase);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading file:" + fileName, e);
        }
     
        return result;
    }
    

}

