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
package bixo.datum;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.scaleunlimited.cascading.PayloadDatum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


@SuppressWarnings("serial")
public class ParsedDatum extends PayloadDatum {
    
    public static final String URL_FN = fieldName(ParsedDatum.class, "url");
    public static final String HOST_ADDRESS_FN = fieldName(ParsedDatum.class, "hostAddress");
    public static final String PARSED_TEXT_FN = fieldName(ParsedDatum.class, "parsedText");
    public static final String LANGUAGE_FN = fieldName(ParsedDatum.class, "language");
    public static final String TITLE_FN = fieldName(ParsedDatum.class, "title");
    public static final String OUTLINKS_FN = fieldName(ParsedDatum.class, "outLinks");
    public static final String PARSED_META_FN = fieldName(ParsedDatum.class, "parsedMeta");

    public static final Fields FIELDS = new Fields(URL_FN, HOST_ADDRESS_FN, PARSED_TEXT_FN, LANGUAGE_FN, 
                    TITLE_FN, OUTLINKS_FN, PARSED_META_FN).append(getSuperFields(ParsedDatum.class));

    /**
     * No argument constructor for use with FutureTask
     */
    public ParsedDatum() {
        super(FIELDS);
    }
    
    public ParsedDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }
    
    public ParsedDatum(String url, String hostAddress, String parsedText, String language, String title, Outlink[] outlinks, Map<String, String> parsedMeta) {
        super(FIELDS);
        
        setUrl(url);
        setHostAddress(hostAddress);
        setParsedText(parsedText);
        setLanguage(language);
        setTitle(title);
        setOutlinks(outlinks);
        setParsedMeta(parsedMeta);
    }

    public String getUrl() {
        return _tupleEntry.getString(URL_FN);
    }

    public void setUrl(String url) {
        _tupleEntry.setString(URL_FN, url);
    }

    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }

    public void setHostAddress(String hostAddress) {
        _tupleEntry.setString(HOST_ADDRESS_FN, hostAddress);
    }

    public String getParsedText() {
        return _tupleEntry.getString(PARSED_TEXT_FN);
    }

    public void setParsedText(String parsedText) {
        _tupleEntry.setString(PARSED_TEXT_FN, parsedText);
    }

    public String getLanguage() {
        return _tupleEntry.getString(LANGUAGE_FN);
    }

    public void setLanguage(String language) {
        _tupleEntry.setString(LANGUAGE_FN, language);
    }

    public String getTitle() {
        return _tupleEntry.getString(TITLE_FN);
    }

    public void setTitle(String title) {
        _tupleEntry.setString(TITLE_FN, title);
    }

    public Outlink[] getOutlinks() {
        return convertTupleToOutlinks((Tuple)_tupleEntry.getObject(OUTLINKS_FN));
    }

    public void setOutlinks(Outlink[] outlinks) {
        _tupleEntry.setObject(OUTLINKS_FN, convertOutlinksToTuple(outlinks));
    }

    public Map<String, String> getParsedMeta() {
        return convertTupleToMap((Tuple)_tupleEntry.getObject(PARSED_META_FN));
    }

    public void setParsedMeta(Map<String, String> parsedMeta) {
        _tupleEntry.setObject(PARSED_META_FN, convertMapToTuple(parsedMeta));
    }

    private Tuple convertOutlinksToTuple(Outlink[] outLinks) {
        Tuple tuple = new Tuple();
        for (Outlink outlink : outLinks) {
            tuple.add(outlink.getToUrl());
            tuple.add(outlink.getAnchor());
            tuple.add(outlink.getRelAttributes());
        }
        
        return tuple;
    }

    private Outlink[] convertTupleToOutlinks(Tuple tuple) {
        int numOutlinks = tuple.size() / 3;
        Outlink[] result = new Outlink[numOutlinks];
        
        for (int i = 0; i < numOutlinks; i++) {
            int tupleOffset = i * 3;
            result[i] = new Outlink(tuple.getString(tupleOffset), tuple.getString(tupleOffset + 1), tuple.getString(tupleOffset + 2));
        }
        
        return result;
    }
    
    private Tuple convertMapToTuple(Map<String, String> map) {
        Tuple result = new Tuple();
        if (map != null) {
            for (Entry<String, String> entry : map.entrySet()) {
                result.add(entry.getKey());
                result.add(entry.getValue());
            }
        }
        
        return result;
    }
    
    private Map<String, String> convertTupleToMap(Tuple tuple) {
        Map<String, String> result = new HashMap<String, String>();
        Iterator<Object> iter = tuple.iterator();
        while (iter.hasNext()) {
            String key = (String)iter.next();
            String value = (String)iter.next();
            result.put(key, value);
        }
        
        return result;
    }

    public static Fields getParsedTextField() {
        return new Fields(ParsedDatum.PARSED_TEXT_FN);
    }

}
