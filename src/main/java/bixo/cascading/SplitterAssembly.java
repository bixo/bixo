package bixo.cascading;

import java.security.InvalidParameterException;

import bixo.cascading.NullContext;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

@SuppressWarnings("serial")
public class SplitterAssembly extends SubAssembly {
	private static final String LHS_SUFFIX = "-lhs";
	private static final String RHS_SUFFIX = "-rhs";
	
	private String _baseName;
	
	private static class SplitterFilter extends BaseOperation<NullContext> implements Filter<NullContext> {
		private ISplitter _splitter;
		private boolean _wantLHS;
		
		public SplitterFilter(ISplitter splitter, boolean wantLHS) {
			_splitter = splitter;
			_wantLHS = wantLHS;
		}
		
		@Override
		public boolean isRemove(FlowProcess flowProcess, FilterCall<NullContext> filterCall) {
			return _splitter.isLHS(filterCall.getArguments().getTuple()) != _wantLHS;
		}
	}
	
	public SplitterAssembly(Pipe inputPipe, ISplitter splitter) {
		_baseName = inputPipe.getName();
        Pipe lhsPipe = new Pipe(_baseName + LHS_SUFFIX, inputPipe);
        lhsPipe = new Each(lhsPipe, new SplitterFilter(splitter, true));
        
        Pipe rhsPipe = new Pipe(_baseName + RHS_SUFFIX, inputPipe);
        rhsPipe = new Each(rhsPipe, new SplitterFilter(splitter, false));

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
