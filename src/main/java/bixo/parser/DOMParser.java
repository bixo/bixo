package bixo.parser;

import java.io.InputStream;

import org.ccil.cowan.tagsoup.Parser;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.hsqldb.lib.StringInputStream;

import bixo.datum.ParsedDatum;
import bixo.utils.IoUtils;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public abstract class DOMParser extends BaseOperation<NullContext> implements Function<NullContext> {

    private transient SAXReader _reader = null;
    private transient ParsedDatum _input;
    
    public DOMParser(Fields outputFields) {
        super(outputFields);
    }
    
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);
        
        _reader = new SAXReader(new Parser());
        _reader.setEncoding("UTF-8");
        _input = new ParsedDatum();
    }
    
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        _input.setTupleEntry(funcCall.getArguments());
        InputStream is = new StringInputStream(_input.getParsedText());
        
        try {
            Document parsedContent = _reader.read(is);
            process(parsedContent, funcCall.getOutputCollector());
        } catch (DocumentException e) {
            handleException(e, funcCall.getOutputCollector());
        } finally {
            IoUtils.safeClose(is);
        }

    }
    
    /**
     * The _input ParsedDatum was successfully converted into a Dom4J Document.
     * at this point you would typically emit one or more output tuples (with
     * appropriate fields), using the collector.
     * 
     * @param doc Result of converting incoming XML document to a Dom4J Document
     * @param collector Collector to use if you want to emit tuples.
     */
    protected abstract void process(Document doc, TupleEntryCollector collector);
    
    /**
     * An exception occurred while parsing the _input ParsedDatum. Options are to
     * ignore it, emit a tuple (with appropriate fields), or throw a RuntimeException
     * to kill the job.
     * 
     * @param e Exception while parsing document
     * @param collector Collector to use if you want to emit a tuple.
     */
    protected abstract void handleException(DocumentException e, TupleEntryCollector collector);
}
