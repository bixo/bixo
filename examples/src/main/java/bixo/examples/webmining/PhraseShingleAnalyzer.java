/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;


public class PhraseShingleAnalyzer  {
    
    private static final int MAX_WORDS_IN_SHINGLE = 2;
    
    private Analyzer _analyzer;

    public PhraseShingleAnalyzer(int maxWordsInShingle) {
        Set<String> noStopWords = Collections.emptySet();
        _analyzer = new ShingleAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29, noStopWords), maxWordsInShingle);
    }
 
    public PhraseShingleAnalyzer() {
       this(MAX_WORDS_IN_SHINGLE);
    }

    public List<String> getTermList(String contentText) {
        TokenStream stream = _analyzer.tokenStream("content", new StringReader(contentText));
        TermAttribute termAtt = (TermAttribute)stream.addAttribute(TermAttribute.class);

        List<String> result = new ArrayList<String>(contentText.length() / 10);
        
        try {
            while (stream.incrementToken()) {
                if (termAtt.termLength() > 0) {
                    String term = termAtt.term();
                    result.add(term);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Impossible error", e);
        }
        
        return result;
    }

    public String getAnalyzedPhrase(String text) {
        
        List<String> termList = getTermList(text);
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (String term : termList) {
            if (count > 0) {
                builder.append(" ");
            }
            builder.append(term);
            count++;
        }
        return builder.toString();
    }
}
