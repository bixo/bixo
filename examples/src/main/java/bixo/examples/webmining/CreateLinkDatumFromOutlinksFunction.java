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
package bixo.examples.webmining;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.NullContext;

import bixo.datum.Outlink;
import bixo.urls.SimpleUrlNormalizer;
import bixo.urls.SimpleUrlValidator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;


@SuppressWarnings("serial")
public class CreateLinkDatumFromOutlinksFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateLinkDatumFromOutlinksFunction.class);
    private transient SimpleUrlNormalizer _normalizer;
    private transient SimpleUrlValidator _validator;

    public CreateLinkDatumFromOutlinksFunction() {
        super(LinkDatum.FIELDS);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of outlink URLs");
        _normalizer = new SimpleUrlNormalizer();
        _validator = new SimpleUrlValidator();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending creation of outlink URLs");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        
        AnalyzedDatum datum = new AnalyzedDatum(funcCall.getArguments().getTuple());
        Outlink outlinks[] = datum.getOutlinks();

        TupleEntryCollector collector = funcCall.getOutputCollector();

        if (outlinks.length > 0) {
            float pageScore = datum.getPageScore();
            
            // Give each outlink 1/N th the page score.
            // Note : Ideally you would deal with duplicates and also ensure that the 
            // source url is excluded.
            float outlinkScore = pageScore/outlinks.length;
    
            for (Outlink outlink : outlinks) {
                String url = outlink.getToUrl().trim();
                url = url.replaceAll("[\n\r]", "");
                String normalizedUrl = _normalizer.normalize(url);
                if (_validator.isValid(normalizedUrl)) {
                    LinkDatum linkDatum = new LinkDatum(normalizedUrl, 0, outlinkScore);
                    collector.add(linkDatum.getTuple());
                }
            }
        }
    }
}
