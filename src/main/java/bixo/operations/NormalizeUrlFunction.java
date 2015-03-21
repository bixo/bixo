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

import com.scaleunlimited.cascading.NullContext;

import bixo.config.BixoPlatform;
import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlNormalizer;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;


@SuppressWarnings("serial")
public class NormalizeUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final BaseUrlNormalizer _normalizer;

    public NormalizeUrlFunction(BaseUrlNormalizer normalizer) {
        super(UrlDatum.FIELDS);
        
        _normalizer = normalizer;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        UrlDatum datum = new UrlDatum(funCall.getArguments());
        
        // Create copy, since we're setting a field, and the tuple is going to be unmodifiable.
        UrlDatum result = new UrlDatum(datum);
        result.setUrl(_normalizer.normalize(datum.getUrl()));
        funCall.getOutputCollector().add(BixoPlatform.clone(result.getTuple(), process));
    }
}
