package bixo.cascading;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.scheme.Scheme;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public class NullSinkTap extends SinkTap {
	
	@SuppressWarnings("unchecked")
	private static class NullEntryCollector extends TupleEntryCollector implements OutputCollector {

		@Override
		protected void collect(Tuple arg0) { }

		@Override
		public void collect(Object arg0, Object arg1) throws IOException { }

	}

	private static class NullScheme extends Scheme {
	    
	    public NullScheme(Fields sourceFields) {
	        super(sourceFields);
	    }
	    
	    @SuppressWarnings("unchecked")
	    @Override
	    public void sink(TupleEntry arg0, OutputCollector arg1) throws IOException { }

	    @Override
	    public void sinkInit(Tap arg0, JobConf arg1) throws IOException { }

	    @Override
	    public Tuple source(Object arg0, Object arg1) {
	        throw new RuntimeException("Can't be a source");
	    }

	    @Override
	    public void sourceInit(Tap arg0, JobConf arg1) throws IOException {
	        throw new RuntimeException("Can't be a source");
	    }
	    
	    @Override
	    public boolean isWriteDirect() {
	    	return true;
	    }
	}

	public NullSinkTap(Fields sourceFields) {
		super(new NullScheme(sourceFields));
	}

	@Override
	public boolean deletePath(JobConf arg0) throws IOException {
		return true;
	}

	@Override
	public Path getPath() {
		return new Path("NullSinkTap");
	}

	@Override
	public long getPathModified(JobConf conf) throws IOException {
		return 0;
	}

	@Override
	public boolean makeDirs(JobConf conf) throws IOException {
		return true;
	}

	@Override
	public boolean pathExists(JobConf conf) throws IOException {
		return true;
	}

	@Override
	public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
		return new NullEntryCollector();
	}
}
