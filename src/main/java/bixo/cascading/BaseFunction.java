package bixo.cascading;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public abstract class BaseFunction<INDATUM extends BaseDatum, OUTDATUM extends BaseDatum> extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(BaseFunction.class);
    
    private INDATUM _inDatum;
    private OUTDATUM _outDatum;
    private TupleEntryCollector _collector;
    private BixoFlowProcess _flowProcess;
    
    public BaseFunction(Class<INDATUM> inClass, Class<OUTDATUM> outClass) throws Exception {
        super(outClass.newInstance().getFields());
        
        _inDatum = inClass.newInstance();
        _outDatum = outClass.newInstance();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public final void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);
        
        _flowProcess = new BixoFlowProcess(process);
        _collector = ((FunctionCall)opCall).getOutputCollector();
        
        try {
            prepare();
        } catch (Throwable t) {
            if (!handlePrepareException(t)) {
                LOGGER.error("Unhandled exception while preparing", t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }

    }
    
    @Override
    public final void cleanup(FlowProcess flowProcess, OperationCall<NullContext> opCall) {
        super.cleanup(flowProcess, opCall);

        try {
            cleanup();
        } catch (Throwable t) {
            if (!handleCleanupException(t)) {
                LOGGER.error("Unhandled exception while cleaning up", t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    @Override
    public final void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        _inDatum.setTupleEntry(funcCall.getArguments());
        
        try {
            process(_inDatum);
        } catch (Throwable t) {
            if (!handleProcessException(_inDatum, t)) {
                LOGGER.error("Unhandled exception while processing datum: " + safeToString(_inDatum), t);
                
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    private String safeToString(Object o) {
        try {
            return o.toString();
        } catch (Throwable t) {
            LOGGER.error("Exception converting object to string", t);
            return "<non-stringable object>";
        }
    }
    
    public final void emit(OUTDATUM out) {
        _collector.add(out.getTuple());
    }

    public OUTDATUM getOutDatum() {
        return _outDatum;
    }
    
    public BixoFlowProcess getFlowProcess() {
        return _flowProcess;
    }
    
    public void prepare() throws Exception {}
    abstract void process(final INDATUM in) throws Exception;
    public void cleanup() throws Exception {}
    
    public boolean handlePrepareException(Throwable t) { return false; }
    public boolean handleProcessException(final INDATUM in, Throwable t) { return false; }
    public boolean handleCleanupException(Throwable t) { return false; }

}
