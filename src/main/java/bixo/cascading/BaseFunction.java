package bixo.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public abstract class BaseFunction<INDATUM extends BaseDatum, OUTDATUM extends BaseDatum> extends BaseOperation<NullContext> implements Function<NullContext> {

    private INDATUM _inDatum;
    private TupleEntryCollector _collector;
    private BixoFlowProcess _flowProcess;
    
    public BaseFunction(Class<INDATUM> inClass, Class<OUTDATUM> outClass) throws Exception {
        super(outClass.newInstance().getFields());
        
        _inDatum = inClass.newInstance();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public final void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);
        
        _flowProcess = new BixoFlowProcess(process);
        _collector = ((FunctionCall)opCall).getOutputCollector();
        
        prepare();
    }
    
    @Override
    public final void cleanup(FlowProcess flowProcess, OperationCall<NullContext> opCall) {
        super.cleanup(flowProcess, opCall);
        
        cleanup();
    }
    
    @Override
    public final void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        _inDatum.setTupleEntry(funcCall.getArguments());
        
        try {
            process(_inDatum);
        } catch (Throwable t) {
            if (!handleException(_inDatum, t)) {
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    public final void emit(OUTDATUM out) {
        _collector.add(out.getTuple());
    }

    public BixoFlowProcess getFlowProcess() {
        return _flowProcess;
    }
    
    public void prepare() {}
    public void cleanup() {}
    public boolean handleException(INDATUM in, Throwable t) { return false; }

    abstract void process(INDATUM in);
}
