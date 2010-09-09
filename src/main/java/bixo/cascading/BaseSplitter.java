package bixo.cascading;

import java.io.Serializable;

import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public abstract class BaseSplitter implements Serializable {
	public String getLHSName() {
	    return this.getClass().getSimpleName() + "-LHS";
	}
	
	public abstract boolean isLHS(TupleEntry tuple);
}
