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

import javax.xml.XMLConstants;

import org.ccil.cowan.tagsoup.Parser;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

import com.scaleunlimited.cascading.NullContext;

import bixo.datum.ParsedDatum;
import bixo.utils.IoUtils;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;


@SuppressWarnings({"serial", "rawtypes"})
public abstract class DOMParser extends BaseOperation<NullContext> implements Function<NullContext> {

    /**
     * Lowercase element names, and optionally strip out XML namespace, so that XPath can be easily 
     * used to extract elements.
     *
     */
    private static class DowngradeXmlFilter extends XMLFilterImpl {
        
        private boolean _removeNamespaces;
        
        public DowngradeXmlFilter(boolean removeNamespaces) {
            super();
            
            _removeNamespaces = removeNamespaces;
        }
        
        @Override
        public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
            // Always lower-case element names, for easier XPath matching
            String lower = localName.toLowerCase();

            if (_removeNamespaces) {
                AttributesImpl attributes = new AttributesImpl();
                for (int i = 0; i < atts.getLength(); i++) {
                    String local = atts.getLocalName(i);
                    String qname = atts.getQName(i);
                    if (!XMLConstants.NULL_NS_URI.equals(atts.getURI(i).length())
                                    && !local.equals(XMLConstants.XMLNS_ATTRIBUTE)
                                    && !qname.startsWith(XMLConstants.XMLNS_ATTRIBUTE + ":")) {
                        attributes.addAttribute(
                                        atts.getURI(i), local, qname,
                                        atts.getType(i), atts.getValue(i));
                    }
                }

                super.startElement(XMLConstants.NULL_NS_URI, lower, lower, attributes);
            } else {
                super.startElement(uri, lower, lower, atts);
            }
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            String lower = localName.toLowerCase();
            super.endElement(XMLConstants.NULL_NS_URI, lower, lower);
        }
    }
    
    
    private boolean _removeNamespaces;
    
    private transient SAXReader _reader = null;
    private transient ParsedDatum _input;
    
    public DOMParser(Fields outputFields) {
        this(outputFields, true);
    }
    
    public DOMParser(Fields outputFields, boolean removeNamespaces) {
        super(outputFields);
        
        _removeNamespaces = removeNamespaces;
    }
    
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);
        
        _reader = new SAXReader(new Parser());
        _reader.setXMLFilter(new DowngradeXmlFilter(_removeNamespaces));
        _reader.setEncoding("UTF-8");
        _input = new ParsedDatum();
    }
    
    @Override
    public boolean isSafe() {
        // Parsing is computationally intensive, so we don't want to get run
        // multiple times.
        return false;
    }
    
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        _input.setTupleEntry(funcCall.getArguments());
        InputStream is = null;
        
        try {
            is = new ByteArrayInputStream(_input.getParsedText().getBytes("UTF-8"));
            Document parsedContent = _reader.read(is);
            process(_input, parsedContent, funcCall.getOutputCollector(), process);
        } catch (Exception e) {
            handleException(_input, e, funcCall.getOutputCollector());
        } finally {
            IoUtils.safeClose(is);
        }
    }
    
    
    /**
     * The _input ParsedDatum was successfully converted into a Dom4J Document.
     * at this point you would typically emit one or more output tuples (with
     * appropriate fields), using the collector.
     * 
     * @param datum Input datum, which wraps a Cascading Tuple.
     * @param doc Result of converting incoming XML document to a Dom4J Document
     * @param collector Collector to use if you want to emit tuples.
     * @param process The FlowProcess for this operation.
     */
    protected abstract void process(ParsedDatum datum, Document doc, TupleEntryCollector collector, FlowProcess process) throws Exception;
    
    /**
     * An exception occurred while parsing or processing the _input ParsedDatum. Options are to
     * ignore it, emit a tuple (with appropriate fields), or throw a RuntimeException
     * to kill the job.
     * 
     * @param datum Input datum, which wraps a Cascading Tuple.
     * @param e Exception while parsing or processing document
     * @param collector Collector to use if you want to emit a tuple.
     */
    protected abstract void handleException(ParsedDatum datum, Exception e, TupleEntryCollector collector);
}
