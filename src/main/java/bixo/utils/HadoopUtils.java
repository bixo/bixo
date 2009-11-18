package bixo.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;

import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;

public class HadoopUtils {
	public static final int DEFAULT_STACKSIZE = 512;
	
    public static void safeRemove(FileSystem fs, Path path) {
    	if ((fs != null) && (path != null)) {
    		try {
    			fs.delete(path, true);
    		} catch (Throwable t) {
    			// Ignore
    		}
    	}
    }
    
    public static JobConf getDefaultJobConf() throws IOException {
    	return getDefaultJobConf(DEFAULT_STACKSIZE);
    }
    
    public static JobConf getDefaultJobConf(int stackSizeInKB) throws IOException {
        JobClient jobClient = new JobClient(new JobConf());
        ClusterStatus status = jobClient.getClusterStatus();
        int trackers = status.getTaskTrackers();

        JobConf conf = new JobConf();
        conf.setNumMapTasks(trackers * 10);
        
        conf.setNumReduceTasks((trackers * conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2)));
        
        conf.setMapSpeculativeExecution(false);
        conf.setReduceSpeculativeExecution(false);
        conf.set("mapred.child.java.opts", String.format("-server -Xmx512m -Xss%dk", stackSizeInKB));

        // Should match the value used for Xss above. Note no 'k' suffix for the ulimit command.
        // New support that one day will be in Hadoop.
        conf.set("mapred.child.ulimit.stack", String.format("%d", stackSizeInKB));

        return conf;
    }

    public static void setLoggingProperties(Properties props, Level cascadingLevel, Level bixoLevel) {
    	props.put("log4j.logger", String.format("cascading=%s,bixo=%s", cascadingLevel, bixoLevel));
    }
    
    @SuppressWarnings("unchecked")
	public static Properties getDefaultProperties(Class appJarClass, boolean debugging, JobConf conf) {
        Properties properties = new Properties();

        // Use special Cascading hack to control logging levels for code running as Hadoop jobs
        if (debugging) {
            properties.put("log4j.logger", "cascading=DEBUG,bixo=TRACE");
        } else {
            properties.put("log4j.logger", "cascading=INFO,bixo=INFO");
        }

        FlowConnector.setApplicationJarClass(properties, appJarClass);

        // Propagate properties into the Hadoop JobConf
        MultiMapReducePlanner.setJobConf(properties, conf);

        return properties;
    }

}
