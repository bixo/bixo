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

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.NullContext;

import bixo.datum.ParsedDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


@SuppressWarnings({"serial", "rawtypes"})
public class CreateWritableSeqFileData extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateWritableSeqFileData.class);

    public CreateWritableSeqFileData() {
        super(new Fields(CrawlConfig.WRITABLE_SEQ_FILE_KEY_FN, CrawlConfig.WRITABLE_SEQ_FILE_VALUE_FN));
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation writable sequence file tuples");
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending creation writable sequence file tuples");
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
        ParsedDatum datum = new ParsedDatum(funcCall.getArguments());
        Text key = new Text(datum.getUrl());
        Text value = new Text(datum.getTitle() + '\n' + datum.getParsedText());
        Tuple keyVal = new Tuple(key, value);
        funcCall.getOutputCollector().add(keyVal);
    }
}
