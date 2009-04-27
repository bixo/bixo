package bixo.cascading;

import org.apache.log4j.Level;

public interface IFlowReporter {
    void setStatus(Level level, String msg);
    void setStatus(String msg, Throwable t);
}
