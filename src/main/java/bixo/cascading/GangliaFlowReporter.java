package bixo.cascading;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import bixo.hadoop.HadoopUtils;
import bixo.utils.GMetric2;

public class GangliaFlowReporter implements IFlowReporter {

    private static final Logger LOGGER = Logger.getLogger(GangliaFlowReporter.class);

    private static final int DEFAULT_PORT = 8649;
    
    private InetAddress _address;
    private int _port;
    
    public GangliaFlowReporter(){
       String serverPort = getDefaultServerPort();
       if (serverPort != null ) {
           String server = null;
            int index = serverPort.indexOf(":");
            if (index != -1) { // we assume the string is of the type name:port
                String portStr = serverPort.substring(index+1);
                _port = Integer.parseInt(portStr);
            } else {
                server = serverPort;
                _port = DEFAULT_PORT;
            }
            try {
                _address =  InetAddress.getByName(server);
            } catch (UnknownHostException e) {
                LOGGER.info("Unable to resolve server name: " + server);
                _address = null;
            }
       }
    }
    
    public GangliaFlowReporter(InetAddress address, int port) {
        _address = address;
        _port = port;
    }
    
    @Override
    public void setStatus(Level level, String msg) {
        if (_address != null) {
            GMetric2.send(_address, _port, level.toString(), msg, GMetric2.VALUE_STRING, "", GMetric2.SLOPE_UNSPECIFIED, 100, 100);
        }
    }

    @Override
    public void setStatus(String msg, Throwable t) {
        if (_address != null){
            GMetric2.send(_address, _port, "Throwable", msg, GMetric2.VALUE_STRING, "", GMetric2.SLOPE_UNSPECIFIED, 100, 100);
        }
    }

    private String getDefaultServerPort() {
        String serverPort = null;

        try {
            ContextFactory contextFactory = ContextFactory.getFactory();
            serverPort = (String)contextFactory.getAttribute("mapred.servers");
            
        } catch (IOException e) {
            // Only log an error when not running in local mode
            try {
                JobConf jobConf = HadoopUtils.getDefaultJobConf();
                if (!HadoopUtils.isJobLocal(jobConf)) {
                    LOGGER.info("Unable to get context factory to determine ganglia server: " + e.getMessage());
                }
            } catch (IOException e1) {
                // Ignore
            }
        } 

        return serverPort;
    }
    
}


