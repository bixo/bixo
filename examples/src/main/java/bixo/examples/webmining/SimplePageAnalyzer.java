/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
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
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tools.ant.filters.StringInputStream;

import bixo.datum.Outlink;

/**
 * A simple page analyzer that will extract all of the non-image links
 * for outlinks, and use the positive & negative phrases for page scoring.
 *
 */
public class SimplePageAnalyzer extends BasePageAnalyzer {
    
    private static final Pattern IMG_SUFFIX_EXCLUSION_PATTERN =
        Pattern.compile("(?i)\\.(gif|jpg|jpeg|bmp|png|ico)$");

    private static final int MAX_WORDS_PER_PHRASE = 2;

    private PhraseShingleAnalyzer _analyzer;
    private Set<String> _positivePhrases;
    private Set<String> _negativePhrases;
    private Parser _parser;

    private String _extractedContent;
    
    public SimplePageAnalyzer() {
        init();
    }
    
    @Override
    public void reset(String hostname, String url, String htmlContent, Outlink[] outlinks) throws Exception {
        super.reset(hostname, url, htmlContent, outlinks);
        
        // Extract text from htmlContent by running it through Tika
        InputStream is = new StringInputStream(htmlContent);
        BodyContentHandler bodyContentHandler = new BodyContentHandler();
        
        // TODO - ideally this would be wrapped in some kind of Future call so we don't 
        // end up waiting too long.
        _parser.parse(is, bodyContentHandler, new Metadata(), new ParseContext());
        _extractedContent = bodyContentHandler.toString();
    }

    /* (non-Javadoc)
     * @see com.scaleunlimited.webmining.BasePageAnalyzer#getLinks()
     * 
     * Return previously extracted links that are not image links
     */
    @Override
    public Outlink[] getLinks() throws Exception {
        Outlink[] allLinks = super.getLinks();
        List<Outlink> nonImageLinks = new ArrayList<Outlink>();
        for (Outlink outlink : allLinks) {
            if (!isImgSuffix(outlink.getToUrl())) {
                nonImageLinks.add(outlink);
            }
        }
        
        return nonImageLinks.toArray(new Outlink[nonImageLinks.size()]);
    }

    /* (non-Javadoc)
     * @see com.scaleunlimited.webmining.BasePageAnalyzer#getResults()
     * 
     * Return previously extracted links that are image links
     */
    @Override
    public PageResult[] getResults() throws Exception {
        String sourceUrl = super.getUrl();
        Outlink[] allLinks = super.getLinks();
        List<PageResult> pageResults = new ArrayList<PageResult>();
        for (Outlink outlink : allLinks) {
            String outlinkUrl = outlink.getToUrl();
            if (isImgSuffix(outlinkUrl)) {
                // TODO VMa - Maybe set description to any words found in image name? Change '-' and '_' to spaces?
                PageResult result = new PageResult(sourceUrl, outlinkUrl, "");
                pageResults.add(result);
            }
        }
        return pageResults.toArray(new PageResult[pageResults.size()]);
    }

    /* (non-Javadoc)
     * @see com.scaleunlimited.webmining.BasePageAnalyzer#getScore()
     * 
     * Calculate the positive term ratio (positive term count/total term count)
     * Do the same thing for the negative terms.
     * The score is the positive ratio - the negative ratio
     */
    @Override
    public float getScore() throws Exception {
        List<String> allTerms = _analyzer.getTermList(_extractedContent);
        
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
        
        float positiveRatio = (float)positiveCount/(float)(positiveCount + negativeCount + neutralCount);
        float negativeRatio = (float)negativeCount/(float)(positiveCount + negativeCount + neutralCount);
        
        return positiveRatio - negativeRatio;
    }

    private void init() {
        // Load the positive and negative phrases.
        // Analyze them using the standard analyzer (no stopwords)
        // TODO Maybe figure out the max # of words, for shingling? For now use a constant.
        _analyzer = new PhraseShingleAnalyzer(MAX_WORDS_PER_PHRASE);
        _positivePhrases = loadAnalyzedPhrases("/positive-phrases.txt", _analyzer);
        _negativePhrases = loadAnalyzedPhrases("/negative-phrases.txt", _analyzer);
        _parser = getTikaParser();
    }
    
    private Set<String> loadAnalyzedPhrases(String fileName, PhraseShingleAnalyzer analyzer) {
        InputStream is = SimplePageAnalyzer.class.getResourceAsStream(fileName);
        Set<String> result = new HashSet<String>();
        
        try {
            @SuppressWarnings("unchecked")
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
    
    private static boolean isImgSuffix(String url) {
        Matcher m = IMG_SUFFIX_EXCLUSION_PATTERN.matcher(url);
       if (m.find()) {
           return true;
       }
       return false;
    }
    
    private Parser getTikaParser() {
        return new AutoDetectParser();
    }


}
