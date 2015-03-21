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


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;


public class PhraseShingleAnalyzer  {
    
    private static final int MAX_WORDS_IN_SHINGLE = 2;
    
    private Analyzer _analyzer;

    public PhraseShingleAnalyzer(int maxWordsInShingle) {
        _analyzer = new ShingleAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_42), maxWordsInShingle);
    }
 
    public PhraseShingleAnalyzer() {
       this(MAX_WORDS_IN_SHINGLE);
    }

    
    public List<String> getTermList(String contentText) {
        List<String> result = new ArrayList<String>(contentText.length() / 10);
        
        try {
            TokenStream stream = _analyzer.tokenStream("content", new StringReader(contentText));
            CharTermAttribute termAtt = (CharTermAttribute) stream.addAttribute(CharTermAttribute.class);

            stream.reset();
            while (stream.incrementToken()) {
                if (termAtt.length() > 0) {
                    String term = termAtt.toString();
                        result.add(term);
                }
            }
            stream.end();
            stream.close();
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
