package bixo.fetcher;

import bixo.cascading.BixoFlowProcess;
import cascading.tuple.Tuple;


public interface IFetchMgr {
    
    public BixoFlowProcess getProcess();
    
    public void collect(Tuple tuple);
    
    public void finished(String ref);
    
    public boolean keepGoing();
    
}
