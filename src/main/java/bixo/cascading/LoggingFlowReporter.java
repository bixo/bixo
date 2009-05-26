package bixo.cascading;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LoggingFlowReporter implements IFlowReporter {
    private static Logger LOGGER = Logger.getLogger(LoggingFlowReporter.class);

    @Override
    public void setStatus(Level level, String msg) {
        LOGGER.log(level, msg);
    }

    @Override
    public void setStatus(String msg, Throwable t) {
        LOGGER.error(msg, t);
    }
}
