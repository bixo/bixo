package bixo.fetcher;

import cascading.tuple.TupleEntryCollector;
import bixo.cascading.BixoFlowProcess;


public interface IFetchMgr {
    
    public BixoFlowProcess getProcess();
    
    public TupleEntryCollector getCollector();
    
    public void finished(String ref);
}
