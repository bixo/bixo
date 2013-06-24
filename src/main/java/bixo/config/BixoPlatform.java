/*
 * Copyright 2009-2013 Scale Unlimited
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

import java.io.File;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;

@SuppressWarnings({"rawtypes", "deprecation"})
public class BixoPlatform extends BasePlatform {

    private static final long CASCADING_LOCAL_JOB_POLLING_INTERVAL = 100;
    private static final long LOCAL_HADOOP_JOB_POLLING_INTERVAL = 100;
    
    private BasePlatform _platform;
    private JobConf _hadoopJobConf = null;

    
    public BixoPlatform(boolean local) throws Exception {
        super(BixoPlatform.class);
        if (local) {
            _platform = new LocalPlatform(BixoPlatform.class);
            setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        } else {
            _hadoopJobConf = new JobConf();
            HadoopPlatform hp = new HadoopPlatform(BixoPlatform.class, _hadoopJobConf);
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
        }
        
    }

    @Override
    public File getDefaultLogDir() {
        return _platform.getDefaultLogDir();
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
    public BasePath makePath(String path) throws Exception {
        return _platform.makePath(path);
    }

    @Override
    public BasePath makePath(BasePath parent, String subdir) throws Exception {
        return _platform.makePath(parent, subdir);
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path, SinkMode mode) throws Exception {
        return _platform.makeTap(scheme, path, mode);
    }

    @Override
    public Scheme makeTextScheme() throws Exception {
        return _platform.makeTextScheme();
    }

    @Override
    public Scheme makeTextScheme(boolean isEnableCompression) throws Exception {
        return _platform.makeTextScheme(isEnableCompression);
    }

    @Override
    public boolean rename(BasePath srcPath, BasePath destPath) throws Exception {
        return _platform.rename(srcPath, destPath);
    }

    @Override
    public void resetNumReduceTasks() throws Exception {
        _platform.resetNumReduceTasks();        
    }

    @Override
    public void setFlowPriority(FlowPriority flowPriority) throws Exception {
        _platform.setFlowPriority(flowPriority);
    }

    @Override
    public void setNumReduceTasks(int numReduceTasks) throws Exception {
        _platform.setNumReduceTasks(numReduceTasks);
    }

}
