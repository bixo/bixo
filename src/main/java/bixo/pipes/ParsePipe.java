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
package bixo.pipes;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.BixoPlatform;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.parser.BaseParser;
import bixo.parser.ParserCounters;
import bixo.parser.SimpleParser;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;


@SuppressWarnings({"serial", "rawtypes"})
public class ParsePipe extends SubAssembly {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParsePipe.class);
    
    public static final String PARSE_PIPE_NAME = "parse_pipe";

    private static class ParseFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        private transient LoggingFlowProcess _flowProcess;
        private BaseParser _parser;

        public ParseFunction(BaseParser parser) {
            super(ParsedDatum.FIELDS);
            _parser = parser;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void prepare(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
        }

        @Override
        public boolean isSafe() {
            // We don't want to get called multiple times for the same tuple
            return false;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            FetchedDatum fetchedDatum = new FetchedDatum(functionCall.getArguments());
            
            try {
                ParsedDatum parseResult = _parser.parse(fetchedDatum);
                _flowProcess.increment(ParserCounters.DOCUMENTS_PARSED, 1);
                functionCall.getOutputCollector().add(BixoPlatform.clone(parseResult.getTuple(), flowProcess));
            } catch (Exception e) {
                LOGGER.warn("Error processing " + fetchedDatum.getUrl(), e);
                _flowProcess.increment(ParserCounters.DOCUMENTS_FAILED, 1);
                // TODO KKr - don't lose datums for documents that couldn't be parsed
            }
        }
        
        @Override
        public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            _flowProcess.dumpCounters();
            super.cleanup(flowProcess, operationCall);
        }
    }

    public ParsePipe(Pipe fetcherPipe) {
        this(fetcherPipe, new SimpleParser());
    }
    
    public ParsePipe(Pipe fetcherPipe, BaseParser parser) {
        super(fetcherPipe);
        Pipe parsePipe = new Pipe(PARSE_PIPE_NAME, fetcherPipe);

        ParseFunction parserFunction = new ParseFunction(parser);
        parsePipe = new Each(parsePipe, parserFunction, Fields.RESULTS);
        setTails(parsePipe);
    }

    public Pipe getTailPipe() {
        String[] pipeNames = getTailNames();
        if (pipeNames.length != 1) {
            throw new RuntimeException("Unexpected number of tail pipes!");
        }
        
        if (!pipeNames[0].equals(PARSE_PIPE_NAME)) {
            throw new RuntimeException("Unexpected name for tail pipe");
        }
        
        return getTails()[0];
    }

}
