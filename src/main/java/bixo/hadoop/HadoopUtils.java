package bixo.hadoop;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker.State;
import org.apache.log4j.Level;

import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;

public class HadoopUtils {
	public static final int DEFAULT_STACKSIZE = 512;
	
    private static final long STATUS_CHECK_INTERVAL = 1000;
	
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
    
    public static int getTaskTrackers(JobConf conf) throws IOException, InterruptedException {
        JobClient jobClient = new JobClient(conf);
        
        while (true) {
            ClusterStatus status = jobClient.getClusterStatus();
            if (status.getJobTrackerState() == State.RUNNING) {
                return status.getTaskTrackers();
            } else {
                Thread.sleep(STATUS_CHECK_INTERVAL);
            }
        }
    }
    
    public static JobConf getDefaultJobConf(int stackSizeInKB) throws IOException {
        JobConf conf = new JobConf();
        
        // We explicitly set task counts to 1 for local so that code which dependes on
        // things like the reducer count runs properly.
        if (isJobLocal(conf)) {
            conf.setNumMapTasks(1);
            conf.setNumReduceTasks(1);
        } else {
            JobClient jobClient = new JobClient(conf);
            ClusterStatus status = jobClient.getClusterStatus();
            int trackers = status.getTaskTrackers();

            conf.setNumMapTasks(trackers * 10);
            conf.setNumReduceTasks((trackers * conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2)));
        }
        
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
    
    public static boolean isJobLocal(JobConf conf) {
        return conf.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }

}
