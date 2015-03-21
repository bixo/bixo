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

import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.scaleunlimited.cascading.NullContext;

import bixo.config.BixoPlatform;
import bixo.datum.UrlDatum;
import bixo.hadoop.ImportCounters;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;


// TODO KKr - combine/resolve delta with UrlImporter
@SuppressWarnings("serial")
public class LoadUrlsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadUrlsFunction.class);

    private int _maxUrls;
    private int _numUrls;

    public LoadUrlsFunction(int maxUrls) {
        super(UrlDatum.FIELDS);

        _maxUrls = maxUrls;
        _numUrls = 0;
    }

    public LoadUrlsFunction() {
        this(Integer.MAX_VALUE);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        if (_numUrls >= _maxUrls) {
            process.increment(ImportCounters.URLS_FILTERED, 1);
            return;
        }
        
        String url = funcCall.getArguments().getString("line").trim();
        if ((url.length() == 0) || (url.startsWith("#"))) {
            return;
        }
        
        try {
            // Validate the URL
            new URL(url);
            
            UrlDatum urlDatum = new UrlDatum(url);
            funcCall.getOutputCollector().add(BixoPlatform.clone(urlDatum.getTuple(), process));
            
            _numUrls += 1;
            process.increment(ImportCounters.URLS_ACCEPTED, 1);
        } catch (MalformedURLException e) {
            LOGGER.warn("Invalid URL in input data file: " + url);
            process.increment(ImportCounters.URLS_REJECTED, 1);
        }
    }
}

