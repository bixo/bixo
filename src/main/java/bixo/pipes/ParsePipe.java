/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.pipes;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.cascading.LoggingFlowReporter;
import bixo.cascading.NullContext;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.parser.IParser;
import bixo.parser.ParserCounters;
import bixo.parser.SimpleParser;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ParsePipe extends SubAssembly {
    private static final Logger LOGGER = Logger.getLogger(ParsePipe.class);
    
    public static final String PARSE_PIPE_NAME = "parse_pipe";

    private static class ParseFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        private transient BixoFlowProcess _flowProcess;
        private IParser _parser;
        private Fields _metaDataFields;

        public ParseFunction(IParser parser, Fields inMetaDataFields, Fields outMetaDataFields) {
            super(ParsedDatum.FIELDS.append(outMetaDataFields));
            _metaDataFields = inMetaDataFields;
            _parser = parser;
        }

        @Override
        public void prepare(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new BixoFlowProcess((HadoopFlowProcess)flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
        }

        @Override
        public boolean isSafe() {
            // We don't want to get called multiple times for the same tuple
            return false;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            FetchedDatum fetchedDatum = new FetchedDatum(arguments.getTuple(), _metaDataFields);
            
            try {
                ParsedDatum parseResult = _parser.parse(fetchedDatum);
                _flowProcess.increment(ParserCounters.DOCUMENTS_PARSED, 1);
                functionCall.getOutputCollector().add(parseResult.toTuple());
            } catch (Exception e) {
                LOGGER.warn("Error processing " + fetchedDatum.getBaseUrl(), e);
                _flowProcess.increment(ParserCounters.DOCUMENTS_FAILED, 1);
                // TODO KKr - don't lose datums for documents that couldn't be parsed
            }
        }
    }

    public ParsePipe(Pipe fetcherPipe) {
        this(fetcherPipe, new SimpleParser(), new Fields());
    }
    
    public ParsePipe(Pipe fetcherPipe, IParser parser) {
        this(fetcherPipe, parser, new Fields());
    }

    public ParsePipe(Pipe fetcherPipe, IParser parser, Fields metaDataFields) {
    	this(fetcherPipe, parser, metaDataFields, metaDataFields);
    }
    
    public ParsePipe(Pipe fetcherPipe, IParser parser, Fields inMetaDataFields, Fields outMetaDataFields) {
        Pipe parsePipe = new Pipe(PARSE_PIPE_NAME, fetcherPipe);

        ParseFunction parserFunction = new ParseFunction(parser, inMetaDataFields, outMetaDataFields);
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
