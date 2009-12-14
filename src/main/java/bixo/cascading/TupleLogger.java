package bixo.cascading;

import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class TupleLogger extends BaseOperation<Long> implements Filter<Long> {
    private static final Logger LOGGER = Logger.getLogger(TupleLogger.class);
    
    private String _prefix = null;
    private boolean _printFields = false;

    private int _printFieldsEvery = 10;
    private int _printTupleEvery = 1;
    private int _maxPrintLength = 100;
    
    public TupleLogger() {
    }

    public TupleLogger(String prefix) {
        _prefix = prefix;
    }

    public TupleLogger(String prefix, boolean printFields) {
        _prefix = prefix;
        _printFields = printFields;
    }

    public TupleLogger(boolean printFields) {
        _printFields = printFields;
    }

    public int getPrintFieldsEvery() {
        return _printFieldsEvery;
    }

    public void setPrintFieldsEvery( int printFieldsEvery ) {
        _printFieldsEvery = printFieldsEvery;
    }

    public int getPrintTupleEvery() {
        return _printTupleEvery;
    }

    public void setPrintTupleEvery( int printTupleEvery ) {
        _printTupleEvery = printTupleEvery;
    }

    public int getMaxPrintLength() {
        return _maxPrintLength;
    }
    
    public void setMaxPrintLength(int maxPrintLength) {
        _maxPrintLength = maxPrintLength;
    }
    
    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<Long> operationCall ) {
        super.prepare(flowProcess, operationCall);

        operationCall.setContext(0L);
    }

    /** @see Filter#isRemove(cascading.flow.FlowProcess, FilterCall) */
    public boolean isRemove( FlowProcess flowProcess, FilterCall<Long> filterCall ) {
        if (LOGGER.isDebugEnabled()) {
            long count = filterCall.getContext();
            TupleEntry entry = filterCall.getArguments();
            
            if (_printFields && ((count % _printFieldsEvery) == 0)) {
                log(entry.getFields().print());
            }
            
            if ((count % _printTupleEvery) == 0) {
                StringBuilder tupleString = new StringBuilder();
                log(printTuple(tupleString, entry.getTuple()));
            }
            
            filterCall.setContext(count + 1);
        }
        
        // Never filter anything
        return false;
    }

    private void log(String message) {
        if (_prefix != null) {
            log(new StringBuilder(message));
        } else {
            LOGGER.debug(message);
        }
    }
    
    private void log(StringBuilder message) {
        if (_prefix != null) {
            message.insert(0, ": ");
            message.insert(0, _prefix);
        }
        
        LOGGER.debug(message);
    }
    
    private StringBuilder printTuple(StringBuilder buffer, Tuple tuple) {

        buffer.append( "[" );
        for (int i = 0; i < tuple.size(); i++) {
            Comparable element = tuple.get( i );

            if (element instanceof Tuple) {
                printTuple(buffer, (Tuple)element);
            } else {
                buffer.append("\'");
                buffer.append(printComparable(element));
                buffer.append( "\'" );
            }

            if (i < tuple.size() - 1) {
                buffer.append(", ");
            }
        }
        
        buffer.append( "]" );

        return buffer;
    }
    
    private String printComparable(Comparable element) {
        // TODO KKr - replace all \n and \r with spaces or escape sequences.
        
        if (element instanceof String) {
            String stringElement = (String)element;
            return stringElement.substring(0, _maxPrintLength);
        } else if (element instanceof BytesWritable) {
            // TODO - convert to hex bytes, but only up to _maxPrintLength/3 bytes
            return "";
        } else {
            return element.toString().substring(0, _maxPrintLength);
        }
    }

}
