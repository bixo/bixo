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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;


@SuppressWarnings("serial")
public class CreateResultsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateResultsFunction.class);

    public CreateResultsFunction() {
        super(new Fields("line"));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of PageResults output");
        super.prepare(process, operationCall);
        
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        
        AnalyzedDatum datum = new AnalyzedDatum(funcCall.getArguments().getTuple());

        TupleEntryCollector collector = funcCall.getOutputCollector();
        PageResult[] pageResults = datum.getPageResults();
        if (pageResults.length > 0) {
            for (PageResult pageResult : pageResults) {
                String outResult = String.format("%s\t%s\t%s", pageResult.getSourceUrl(), pageResult.getImageUrl(), pageResult.getDescription());
                collector.add(new Tuple(outResult));
            }
        }
    }
    
}
