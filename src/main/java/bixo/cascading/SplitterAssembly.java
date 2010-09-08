package bixo.cascading;

import java.security.InvalidParameterException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

@SuppressWarnings("serial")
public class SplitterAssembly extends SubAssembly {
	private static final String LHS_SUFFIX = "-lhs";
	private static final String RHS_SUFFIX = "-rhs";
	
	private String _baseName;
	
	private enum SplitterCounters {
	    LHS,
	    RHS,
	}
	
    @SuppressWarnings("unchecked")
	private static class SplitterFilter extends BaseOperation<NullContext> implements Filter<NullContext> {
		private ISplitter _splitter;
		private boolean _wantLHS;
        private Enum _counter;
	    private transient BixoFlowProcess _flowProcess;
		
		public SplitterFilter(ISplitter splitter, boolean wantLHS, Enum counter) {
			_splitter = splitter;
			_wantLHS = wantLHS;
			_counter = counter;
		}
		
	    @Override
	    public void prepare(FlowProcess flowProcess,
	                        OperationCall<NullContext> operationCall) {
	        super.prepare(flowProcess, operationCall);
	        _flowProcess = new BixoFlowProcess((HadoopFlowProcess) flowProcess);
	        _flowProcess.addReporter(new LoggingFlowReporter());
	    }
	    
		@Override
		public boolean isRemove(FlowProcess flowProcess, FilterCall<NullContext> filterCall) {
		    boolean result = _splitter.isLHS(filterCall.getArguments()) != _wantLHS;
            if (!result) {
                _flowProcess.increment(_counter, 1);
            }
		    return result;
		}

	    @Override
	    public void cleanup(FlowProcess flowProcess,
	                        OperationCall<NullContext> operationCall) {
	        _flowProcess.dumpCounters();
	        super.cleanup(flowProcess, operationCall);
	    }
	}

    public SplitterAssembly(Pipe inputPipe, ISplitter splitter) {
        this(inputPipe, splitter, SplitterCounters.LHS, SplitterCounters.RHS);
    }

    @SuppressWarnings("unchecked")
	public SplitterAssembly(Pipe inputPipe,
                            ISplitter splitter,
                            Enum lhsCounter,
                            Enum rhsCounter) {
		_baseName = inputPipe.getName();
        Pipe lhsPipe = new Pipe(_baseName + LHS_SUFFIX, inputPipe);
        lhsPipe = new Each(lhsPipe, new SplitterFilter(splitter, true, lhsCounter));
        
        Pipe rhsPipe = new Pipe(_baseName + RHS_SUFFIX, inputPipe);
        rhsPipe = new Each(rhsPipe, new SplitterFilter(splitter, false, rhsCounter));

        setTails(lhsPipe, rhsPipe);
	}
	
	public Pipe getLHSPipe() {
    	return getTailPipe(_baseName + LHS_SUFFIX);
	}

	public Pipe getRHSPipe() {
    	return getTailPipe(_baseName + RHS_SUFFIX);
	}

    private Pipe getTailPipe(String pipeName) {
        String[] pipeNames = getTailNames();
        for (int i = 0; i < pipeNames.length; i++) {
            if (pipeName.equals(pipeNames[i])) {
                return getTails()[i];
            }
        }
        
        throw new InvalidParameterException("Invalid pipe name: " + pipeName);
    }

}
