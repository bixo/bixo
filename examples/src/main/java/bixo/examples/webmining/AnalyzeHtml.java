/*
 * Copyright 2009-2012 Scale Unlimited
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;

import bixo.datum.ParsedDatum;
import bixo.parser.DOMParser;
import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class AnalyzeHtml extends DOMParser {
    
    private static final Logger LOGGER = Logger.getLogger(AnalyzeHtml.class);

    private static final int MAX_WORDS_PER_PHRASE = 2;

    // These are all transient since we don't want to serialize them when the
    // Cascading job is submitted, so we set them up in the prepare() method.
    private transient PhraseShingleAnalyzer _analyzer;
    private transient Set<String> _positivePhrases;
    private transient Set<String> _negativePhrases;

    public AnalyzeHtml() {
        super(AnalyzedDatum.FIELDS); 
    }
    
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        // Load the positive and negative phrases.
        // Analyze them using the standard analyzer (no stopwords)
        // TODO Maybe figure out the max # of words, for shingling? For now use a constant.
        _analyzer = new PhraseShingleAnalyzer(MAX_WORDS_PER_PHRASE);
        _positivePhrases = loadAnalyzedPhrases("/positive-phrases.txt", _analyzer);
        _negativePhrases = loadAnalyzedPhrases("/negative-phrases.txt", _analyzer);
    }
    
    @Override
    protected void process(ParsedDatum datum, Document doc, TupleEntryCollector collector) {
        // TODO get all of the text from doc, and pass it to getScore()
        // TODO we need to add the outlinks to the AnalyzedDatum, so that we can use the score
        // from the page, divided among the links - or set the score on a per-link bases in
        // a new type of structure, and use that in the main workflow.
        
    }

    @Override
    protected void handleException(ParsedDatum datum, DocumentException e, TupleEntryCollector collector) {
        // TODO Auto-generated method stub
        
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
        
        float positiveRatio = (float)positiveCount/(float)(positiveCount + negativeCount + neutralCount);
        float negativeRatio = (float)negativeCount/(float)(positiveCount + negativeCount + neutralCount);
        
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

