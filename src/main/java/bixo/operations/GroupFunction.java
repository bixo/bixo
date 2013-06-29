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

import org.apache.log4j.Logger;

import com.scaleunlimited.cascading.NullContext;


import bixo.config.BixoPlatform;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

@SuppressWarnings({ "serial" })
public class GroupFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(GroupFunction.class);

    private final BaseGroupGenerator _generator;

    public GroupFunction(BaseGroupGenerator generator) {
        super(GroupedUrlDatum.FIELDS);
        
        _generator = generator;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        String key;
        try {
            UrlDatum datum = new UrlDatum(funCall.getArguments());
            key = _generator.getGroupingKey(datum);
            GroupedUrlDatum result = new GroupedUrlDatum(datum, key);
            funCall.getOutputCollector().add(BixoPlatform.clone(result.getTuple(), process));
        } catch (Exception e) {
            // TODO KKr - don't lose the tuple (skipping support)
            LOGGER.error("Unexpected exception while grouping URL (probably badly formed)", e);
        }
    }
}
