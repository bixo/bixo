package bixo.fetcher;

import cascading.tuple.Tuple;

import com.bixolabs.cascading.LoggingFlowProcess;


public interface IFetchMgr {
    
    public LoggingFlowProcess getProcess();
    
    public void collect(Tuple tuple);
    
    public void finished(String ref);
    
}
