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
 */
package bixo.operations;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import bixo.config.UserAgent;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.RandomResponseHandler;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.fetcher.StringResponseHandler;
import bixo.fetcher.simulation.TestWebServer;
import bixo.hadoop.FetchCounters;
import bixo.utils.GroupingKey;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProcess.NullFlowProcess;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullContext;

import crawlercommons.robots.BaseRobotsParser;
import crawlercommons.robots.SimpleRobotRulesParser;


public class FilterAndScoreByUrlAndRobotsTest {
    private static final String CRLF = "\r\n";

    private static class MatchBlockedByRobotsKey extends ArgumentMatcher<Tuple> {

        @Override
        public boolean matches(Object argument) {
            ScoredUrlDatum datum = new ScoredUrlDatum((Tuple)argument);
            return (datum.getGroupKey().equals(GroupingKey.BLOCKED_GROUPING_KEY));
        }
    }
    
    private List<TupleEntry> getGroupedurlDatumList(String url) {
        List<TupleEntry> iterValues = new ArrayList<TupleEntry>();
        iterValues.add(new GroupedUrlDatum(url, url).getTupleEntry());
        return iterValues;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testUsingAllThreads() throws Exception {
        final int maxThreads = 10;
        
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, new UserAgent("testAgent", "crawler@domain.com", "http://www.domain.com"));
        BaseScoreGenerator scorer = new FixedScoreGenerator(1.0);
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        FilterAndScoreByUrlAndRobots op = new FilterAndScoreByUrlAndRobots(fetcher, parser, scorer);
        
        FlowProcess fp = Mockito.mock(NullFlowProcess.class);
        
        OperationCall<NullContext> oc = Mockito.mock(OperationCall.class);
        BufferCall<NullContext> bc = Mockito.mock(BufferCall.class);
        
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);
        
        Mockito.when(bc.getGroup()).thenReturn(new TupleEntry(new Tuple("http://localhost:8089")));
        Mockito.when(bc.getArgumentsIterator()).thenReturn(getGroupedurlDatumList("http://localhost:8089").iterator());
        Mockito.when(bc.getOutputCollector()).thenReturn(collector);
        
        TestWebServer server = null;
        
        try {
            server = new TestWebServer(new RandomResponseHandler(100, 1000), 8089);
            op.prepare(fp, oc);

            for (int i = 0; i < maxThreads; i++) {
                op.operate(fp, bc);
            }
            
            // Give threads a chance to run, as otherwise we might call verify() before one of the ProcessRobotsTask
            // threads has been started.
            op.flush(fp, oc);
            
            Mockito.verify(fp, Mockito.times(maxThreads)).increment(FetchCounters.DOMAINS_PROCESSING, 1);
        } finally {
            server.stop();
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testBlockedRobots() throws Exception {
        final int maxThreads = 1;
        
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, new UserAgent("testAgent", "crawler@domain.com", "http://www.domain.com"));
        BaseScoreGenerator scorer = new FixedScoreGenerator(1.0);
        BaseRobotsParser parser = new SimpleRobotRulesParser();
        FilterAndScoreByUrlAndRobots op = new FilterAndScoreByUrlAndRobots(fetcher, parser, scorer);
        
        FlowProcess fp = Mockito.mock(NullFlowProcess.class);
        
        OperationCall<NullContext> oc = Mockito.mock(OperationCall.class);
        BufferCall<NullContext> bc = Mockito.mock(BufferCall.class);
        
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);
        
        Mockito.when(bc.getGroup()).thenReturn(new TupleEntry(new Tuple("http://localhost:8089")));
        Mockito.when(bc.getArgumentsIterator()).thenReturn(getGroupedurlDatumList("http://localhost:8089").iterator());
        Mockito.when(bc.getOutputCollector()).thenReturn(collector);
        
        TestWebServer server = null;
        
        try {
            final String disallowAllRobots = "User-agent: *" + CRLF
            + "Disallow: /";

            server = new TestWebServer(new StringResponseHandler("text/plain", disallowAllRobots), 8089);
            op.prepare(fp, oc);

            for (int i = 0; i < maxThreads; i++) {
                op.operate(fp, bc);
            }
            
            // Give threads a chance to run, as otherwise we might call verify() before one of the ProcessRobotsTask
            // threads has been started.
            op.flush(fp, oc);
            
            Mockito.verify(collector).add(Mockito.argThat(new MatchBlockedByRobotsKey()));
        } finally {
            server.stop();
        }
    }

}
