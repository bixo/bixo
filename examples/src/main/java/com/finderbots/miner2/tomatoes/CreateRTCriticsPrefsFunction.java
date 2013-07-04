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
package com.finderbots.miner2.tomatoes;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.bixolabs.cascading.NullContext;
import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class CreateRTCriticsPrefsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateRTCriticsPrefsFunction.class);

    public CreateRTCriticsPrefsFunction() {
        super(new Fields("line"));
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of PageResults output");
        super.prepare(process, operationCall);
        
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        
        RTPageDatum datum = new RTPageDatum(funcCall.getArguments().getTuple());

        TupleEntryCollector collector = funcCall.getOutputCollector();
        MultiValuePreference[] preferences = datum.getPreferences();
        if (preferences.length > 0) {
            for (MultiValuePreference preference : preferences) {
                String outResult = String.format("%s,%s,%s", preference.getUserId(), preference.getActionId(), preference.getItemId());
                collector.add(new Tuple(outResult));
            }
        }//otherwise ignore
    }
    
}
