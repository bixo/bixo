package bixo.cascading;

import java.io.Serializable;

import cascading.tuple.TupleEntry;

public interface ISplitter extends Serializable     {
	public String getLHSName();
	
	public boolean isLHS(TupleEntry tuple);
}
