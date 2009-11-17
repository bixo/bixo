package bixo.cascading;

import java.io.Serializable;

import cascading.tuple.Tuple;

public interface ISplitter extends Serializable {
	public String getLHSName();
	
	public boolean isLHS(Tuple tuple);
}
