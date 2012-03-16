/*
 * Copyright 2009-2012 Scale Unlimited
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
 */
package bixo.operations;

import java.util.Iterator;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import bixo.config.UserAgent;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.BaseFetcher;
import bixo.hadoop.FetchCounters;
import bixo.robots.BaseRobotsParser;
import bixo.robots.RobotUtils;
import bixo.utils.DiskQueue;
import bixo.utils.GroupingKey;
import bixo.utils.ThreadedExecutor;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;

/**
 * Filter out URLs by either domain (not popular enough) or if they're blocked by robots.txt
 *
 */

@SuppressWarnings("serial")
public class FilterAndScoreByUrlAndRobots extends BaseOperation<NullContext> implements Buffer<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(FilterAndScoreByUrlAndRobots.class);
	
    private static final long COMMAND_TIMEOUT = RobotUtils.getMaxFetchTime();
    private static final long TERMINATE_TIMEOUT = COMMAND_TIMEOUT;

    private static final int MAX_URLS_IN_MEMORY = 100;

    private BaseScoreGenerator _scorer;
	private BaseFetcher _fetcher;
	private BaseRobotsParser _parser;
	
    private transient ThreadedExecutor _executor;
    private transient LoggingFlowProcess _flowProcess;

    public FilterAndScoreByUrlAndRobots(UserAgent userAgent, int maxThreads, BaseRobotsParser parser, BaseScoreGenerator scorer) {
        super(ScoredUrlDatum.FIELDS);

        _scorer = scorer;
        _parser = parser;
        _fetcher = RobotUtils.createFetcher(userAgent, maxThreads);
    }

    public FilterAndScoreByUrlAndRobots(BaseFetcher fetcher, BaseRobotsParser parser, BaseScoreGenerator scorer) {
        // We're going to output a ScoredUrlDatum (what FetcherBuffer expects).
        super(ScoredUrlDatum.FIELDS);

        _scorer = scorer;
        _parser = parser;
        _fetcher = fetcher;
    }

    @Override
    public void prepare(FlowProcess flowProcess, cascading.operation.OperationCall<NullContext> operationCall) {
        _executor = new ThreadedExecutor(_fetcher.getMaxThreads(), COMMAND_TIMEOUT);
        
        // FUTURE KKr - use Cascading process vs creating our own, once it
        // supports logging in local mode, and a setStatus() call.
        _flowProcess = new LoggingFlowProcess((HadoopFlowProcess)flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
    }
    
    @Override
    public void cleanup(FlowProcess flowProcess, cascading.operation.OperationCall<NullContext> operationCall) {
        
        try {
            if (!_executor.terminate(TERMINATE_TIMEOUT)) {
                LOGGER.warn("Had to do a hard shutdown of robots fetching");
            }
        } catch (InterruptedException e) {
            // FUTURE What's the right thing to do here? E.g. do I need to worry about
            // losing URLs still to be processed?
            LOGGER.warn("Interrupted while waiting for termination");
            Thread.currentThread().interrupt();
        }
        
        _flowProcess.dumpCounters();
    }
    
	@Override
	public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
        TupleEntry group = bufferCall.getGroup();
        String protocolAndDomain = group.getString(0);

        DiskQueue<GroupedUrlDatum> urls = new DiskQueue<GroupedUrlDatum>(MAX_URLS_IN_MEMORY);
        Iterator<TupleEntry> values = bufferCall.getArgumentsIterator();
        while (values.hasNext()) {
            urls.add(new GroupedUrlDatum(new TupleEntry(values.next())));
        }
        
        try {
            Runnable doRobots = new ProcessRobotsTask(protocolAndDomain, _scorer, urls, _fetcher, _parser, bufferCall.getOutputCollector(), _flowProcess);
            _executor.execute(doRobots);
        } catch (RejectedExecutionException e) {
            // should never happen.
            LOGGER.error("Robots handling pool rejected our request for " + protocolAndDomain);
            _flowProcess.increment(FetchCounters.DOMAINS_REJECTED, 1);
            _flowProcess.increment(FetchCounters.URLS_REJECTED, urls.size());
            ProcessRobotsTask.emptyQueue(urls, GroupingKey.DEFERRED_GROUPING_KEY, bufferCall.getOutputCollector());
        } catch (Throwable t) {
           LOGGER.error("Caught an unexpected throwable - robots handling rejected our request for " + protocolAndDomain, t);
           _flowProcess.increment(FetchCounters.DOMAINS_REJECTED, 1);
           _flowProcess.increment(FetchCounters.URLS_REJECTED, urls.size());
           ProcessRobotsTask.emptyQueue(urls, GroupingKey.DEFERRED_GROUPING_KEY, bufferCall.getOutputCollector());
      } 
	}

	

}
