/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */package bixo.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProcess.NullFlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.Level;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;

@SuppressWarnings({"rawtypes", "serial"})
public class BixoPlatform extends BasePlatform {

    // TODO Replace these with LocalPlatform.PLATFORM_TYPE and
    // HadoopPlatform.PLATFORM_TYPE?
    public enum Platform {
        Local,
        Hadoop
    }
    
    private static final long CASCADING_LOCAL_JOB_POLLING_INTERVAL = 100;
    private static final long LOCAL_HADOOP_JOB_POLLING_INTERVAL = 100;
    
    private BasePlatform _platform;
    private Level _logLevel;

    public BixoPlatform(Class applicationJarClass, Platform platform) throws Exception {
        this(applicationJarClass, platform, Level.SLF4J_DEBUG);
    }
    
    public BixoPlatform(Class applicationJarClass, Platform platform, Level logLevel) throws Exception {
        super(applicationJarClass);
        
        if (platform == Platform.Local) {
            _platform = new LocalPlatform(applicationJarClass);
            setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        } else {
            configureHadoopPlatform(applicationJarClass, new JobConf());
        }
        
        configureLogLevel(logLevel);
    }

    private void configureLogLevel(Level logLevel) {
        // Control TupleLogger settings
        _logLevel = logLevel;
        setLogLevel(_logLevel, TupleLogger.class.getName());
    }

    public BixoPlatform(Class applicationJarClass, Configuration conf) throws Exception {
        this(applicationJarClass, new JobConf(conf));
    }
    
    public BixoPlatform(Class applicationJarClass, JobConf conf) throws Exception {
        this(applicationJarClass, conf, Level.SLF4J_DEBUG);
    }
    
    public BixoPlatform(Class applicationJarClass, JobConf conf, Level logLevel) throws Exception {
        super(applicationJarClass);

        configureHadoopPlatform(applicationJarClass, conf);
        configureLogLevel(logLevel);
    }
    
    private void configureHadoopPlatform(Class applicationJarClass, JobConf jobConf) throws Exception {
        HadoopPlatform hp = new HadoopPlatform(applicationJarClass, jobConf);
        _platform = hp;
        
        // Special configuration for Hadoop.
        
        if (isLocal()) {
            setNumReduceTasks(1);
            setJobPollingInterval(LOCAL_HADOOP_JOB_POLLING_INTERVAL);
        } else {
            setNumReduceTasks(BasePlatform.CLUSTER_REDUCER_COUNT);
        }
        
        hp.setMapSpeculativeExecution(false);
        hp.setReduceSpeculativeExecution(false);
        
        // We don't need all of the debug level output from Cascading
        setLogLevel(Level.SLF4J_INFO, "cascading");
        
        // Turn down noise from Hadoop & Cascading
        setLogLevel(Level.SLF4J_INFO, "org.apache.hadoop.conf.Configuration", "cascading.pipe.joiner.InnerJoin");
        
        // We get WARN logging for every trapped record, which can be many GBs if we have millions
        // of these. So crank it up to ERROR level.
        setLogLevel(Level.SLF4J_ERROR, "cascading.flow.stream.TrapHandler");
    }
    
    @Override
    public String getPlatformType() {
        return _platform.getPlatformType();
    }
    
    public static Tuple clone(Tuple t, FlowProcess flowProcess) {
        if (flowProcess instanceof LoggingFlowProcess) {
            // Need to unwrap to get at actual flow process.
            flowProcess = ((LoggingFlowProcess)flowProcess).getDelegate();
        }
        
        if (flowProcess instanceof HadoopFlowProcess) {
            // Hadoop will do a deep copy implicitly (via serialization), so we don't
            // have to do any copying ourselves, and thus the tuple isn't modified.
            return t;
        } else if (flowProcess instanceof NullFlowProcess) {
            // I'm guessing we don't need to copy Tuples when running a CascadingTestCase.
            return t;
        } else if (flowProcess instanceof LocalFlowProcess) {
            
            // TODO We don't really need to copy the Tuple if every value
            // inside is immutable.
            
            // Cascading local mode just does a shallow copy of the tuple, so we need
            // to clone the tuple, and then deep-clone any tuple fields that aren't scalars.
            // Currently we only handle Tuple & Date as an embedded object, or anything
            // that's Writable.
            
            Tuple result = t;
            
            for (int i = 0; i < result.size(); i++) {
                Object value = result.getObject(i);
                if ((value == null) || (value instanceof Boolean) || (value instanceof String) ||
                    (value instanceof Double) || (value instanceof Float) || 
                    (value instanceof Integer) || (value instanceof Long) || (value instanceof Short)) {
                    // All set, Cascading will do shallow copy
                } else {
                    // we need to clone the Tuple
                    if (result == t) {
                        result = new Tuple(t);
                    }
                    
                    result.set(i, cloneValue(value, i));
                }
            }
            
            return result;
        } else {
            throw new IllegalArgumentException("Unknown class of flow process: " + flowProcess.getClass().getName());
        }
    }
    
    private static Object cloneValue(Object value, int index) {
        if (value instanceof Tuple) {
            return new Tuple((Tuple)value);
        } else if (value instanceof Date) {
            return new Date(((Date)value).getTime());
        } else if (value instanceof Writable) {
            // FUTURE - add threadlocal variables that we reuse here, versus re-allocating constantly.
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ((Writable)value).write(new DataOutputStream(baos));
                Object result = value.getClass().newInstance();
                ((Writable)result).readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Error doing deep copy of Writable", e);
            }
        } else {
            // FUTURE - add support for Serializable types
            throw new RuntimeException(String.format("Field value of type %s at position %d can't be cloned, unknown type", value.getClass().getName(), index));
        }
    }

    @Override
    public String copySharedDirToLocal(FlowProcess flowProcess, String sharedDirName) {
        return _platform.copySharedDirToLocal(flowProcess, sharedDirName);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return _platform.getBooleanProperty(name);
    }

    @Override
    public File getDefaultLogDir() {
        return _platform.getDefaultLogDir();
    }

    @Override
    public int getIntProperty(String name) {
        return _platform.getIntProperty(name);
    }

    @Override
    public File getLogDir() {
        return _platform.getLogDir();
    }

    @Override
    public int getNumReduceTasks() throws Exception {
        return _platform.getNumReduceTasks();
    }

    @Override
    public String getProperty(String name) {
        return _platform.getProperty(name);
    }

    @Override
    public BasePath getTempDir() throws Exception {
        return _platform.getTempDir();
    }

    @Override
    public boolean isLocal() {
        return _platform.isLocal();
    }

    @Override
    public boolean isTextSchemeCompressable() {
        return _platform.isTextSchemeCompressable();
    }

    @Override
    public Scheme makeBinaryScheme(Fields fields) throws Exception {
        return _platform.makeBinaryScheme(fields);
    }

    @Override
    public FlowConnector makeFlowConnector() throws Exception {
        return _platform.makeFlowConnector();
    }

    @Override
    public FlowProcess makeFlowProcess() throws Exception {
        return _platform.makeFlowProcess();
    }

    @Override
    public Tap makePartitionTap(Tap parentTap, Partition partition) throws Exception {
        return _platform.makePartitionTap(parentTap, partition);
    }

    @Override
    public Tap makePartitionTap(Tap parentTap, Partition partition, SinkMode mode) throws Exception {
        return _platform.makePartitionTap(parentTap, partition, mode);
    }

    @Override
    public BasePath makePath(String path) throws Exception {
        return _platform.makePath(path);
    }

    @Override
    public BasePath makePath(BasePath parent, String subdir) throws Exception {
        return _platform.makePath(parent, subdir);
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path) throws Exception {
        return _platform.makeTap(scheme, path);
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path, SinkMode mode) throws Exception {
        return _platform.makeTap(scheme, path, mode);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Tap makeTemplateTap(Tap tap, String pattern, Fields fields) throws Exception {
        return _platform.makeTemplateTap(tap, pattern, fields);
    }

    @Override
    public Scheme makeTextScheme() throws Exception {
        return _platform.makeTextScheme();
    }

    @Override
    public Scheme makeTextScheme(boolean enableCompression) throws Exception {
        return _platform.makeTextScheme(enableCompression);
    }

    @Override
    public boolean rename(BasePath src, BasePath dst) throws Exception {
        return _platform.rename(src, dst);
    }

    @Override
    public void resetNumReduceTasks() throws Exception {
        _platform.resetNumReduceTasks();
    }

    @Override
    public void setFlowPriority(FlowPriority priority) throws Exception {
        _platform.setFlowPriority(priority);
    }

    @Override
    public void setJobPollingInterval(long interval) {
        _platform.setJobPollingInterval(interval);
    }

    @Override
    public void setLogDir(File logDir) {
        _platform.setLogDir(logDir);
    }

    @Override
    public void setLogLevel(Level level, String... packageNames) {
        _platform.setLogLevel(level, packageNames);
    }

    @Override
    public void setNumReduceTasks(int numReduceTasks) throws Exception {
        _platform.setNumReduceTasks(numReduceTasks);
    }

    @Override
    public void setProperty(String name, String value) {
        _platform.setProperty(name, value);
    }

    @Override
    public void setProperty(String name, int value) {
        _platform.setProperty(name, value);
    }

    @Override
    public void setProperty(String name, boolean value) {
        _platform.setProperty(name, value);
    }

    @Override
    public String shareLocalDir(String localDirName) {
        return _platform.shareLocalDir(localDirName);
    }

}
