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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.NullContext;

import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlNormalizer;
import bixo.urls.BaseUrlValidator;
import bixo.urls.SimpleUrlNormalizer;
import bixo.urls.SimpleUrlValidator;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;


public class UrlImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(UrlImporter.class);

    @SuppressWarnings("serial")
    private static class CreateUrlFromTextFunction extends BaseOperation<NullContext> implements Function<NullContext> {
        private BaseUrlNormalizer _normalizer;
        private BaseUrlValidator _validator;

        public CreateUrlFromTextFunction(BaseUrlNormalizer normalizer, BaseUrlValidator validator) {
            super(CrawlDbDatum.FIELDS);
            _normalizer = normalizer;
            _validator = validator;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            if (urlAsString.length() > 0) {
                // Normalize and Validate the URL
                urlAsString = _normalizer.normalize(urlAsString);
                if (_validator.isValid(urlAsString)) {
                    CrawlDbDatum crawlDatum = new CrawlDbDatum(urlAsString, 0, 0, UrlStatus.UNFETCHED, 0);
                    funcCall.getOutputCollector().add(crawlDatum.getTuple());
                }
            } else {
                LOGGER.error("Malformed url in import list: " + urlAsString);
            }
        }
    }

    private BasePath _inputFilePath;
    private BasePath _destDirPath;
    private BasePlatform _platform;

    /**
     * UrlImporter is a simple class that can be used to import a list of urls into the
     * crawl db
     * 
     * @param inputFilePath : the input text file containing the urls to be imported
     * @param desDirPath : the destination path at which to create the crawl db.
     */
    public UrlImporter(BasePlatform platform, BasePath inputFilePath, BasePath desDirPath) {
        _platform = platform;
        _inputFilePath = inputFilePath;
        _destDirPath = desDirPath;
    }

    @SuppressWarnings("rawtypes")
    public void importUrls(boolean debug) throws Exception {


        try {
            Tap urlSource = _platform.makeTap(_platform.makeTextScheme(), _inputFilePath);
            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFromTextFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator()));

            Tap urlSink = _platform.makeTap(_platform.makeBinaryScheme(CrawlDbDatum.FIELDS), _destDirPath, SinkMode.REPLACE);

            FlowConnector flowConnector = _platform.makeFlowConnector();
            Flow flow = flowConnector.connect(urlSource, urlSink, importPipe);
            flow.complete();
        } catch (Exception e) {
            throw e;
        }
    }

}
