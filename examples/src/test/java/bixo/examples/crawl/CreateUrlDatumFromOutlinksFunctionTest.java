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
package bixo.examples.crawl;


import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.urls.SimpleUrlNormalizer;
import bixo.urls.SimpleUrlValidator;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.Payload;

public class CreateUrlDatumFromOutlinksFunctionTest {


    @SuppressWarnings("unchecked")
    @Test
    public void testOperate() {
        CreateUrlDatumFromOutlinksFunction op = new CreateUrlDatumFromOutlinksFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator());
        HadoopFlowProcess fp = Mockito.mock(HadoopFlowProcess.class);
        Mockito.when(fp.getJobConf()).thenReturn(new JobConf());
        
        OperationCall<NullContext> oc = Mockito.mock(OperationCall.class);
        FunctionCall<NullContext> fc = Mockito.mock(FunctionCall.class);
        TupleEntryCollector collector = Mockito.mock(TupleEntryCollector.class);
        
        Outlink outlink1 =  new Outlink("http://bar.com/", "anchorText");
        Outlink outlinks[] = {outlink1};
        ParsedDatum datum = new ParsedDatum("http://foo.com/", 
                        "foo.com", "parsed text", "en", "title", outlinks, null);
        datum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, 0);
                        
        TupleEntry entry = new TupleEntry(ParsedDatum.FIELDS);
        entry.setTuple(new Tuple(datum.getTuple()));

        Mockito.when(fc.getArguments()).thenReturn(entry);
        Mockito.when(fc.getOutputCollector()).thenReturn(collector);
        
        op.prepare(fp, oc);
        op.operate(fp, fc);
        op.cleanup(fp, oc);

        Mockito.verify(collector).add(Mockito.argThat(new MatchUrlDatum()));
        Mockito.verifyNoMoreInteractions(collector);

    }
    
    private static class MatchUrlDatum extends ArgumentMatcher<Tuple> {

        @Override
        public boolean matches(Object argument) {
            UrlDatum datum = new UrlDatum(UrlDatum.FIELDS, (Tuple)argument);
            if (datum.getUrl().equals("http://bar.com/")) {
                Payload payload = datum.getPayload();
                return payload.get(CrawlDbDatum.LAST_STATUS_FIELD).equals(UrlStatus.UNFETCHED.name())
                                && payload.get(CrawlDbDatum.LAST_FETCHED_FIELD) == Long.valueOf(0);
            }
            return false;
        }
    }

}
