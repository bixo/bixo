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
package bixo.examples.crawl;

import bixo.datum.ParsedDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.bixolabs.cascading.NullContext;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class WriteMahoutSequenceFileFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(UrlImporter.class);

    private String _languageName = "";
    private int _titleWeight = 1;


    public String getlanguageName() {
        return _languageName;
    }

    public void setlanguageName(String languageName) {
        _languageName = languageName;
    }

    public int getTitleWeight() {
        return _titleWeight;
    }

    public void setTitleWeight(int titleWeight) {
        _titleWeight = titleWeight;
    }

    public WriteMahoutSequenceFileFunction() {
        super(new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN));
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        ParsedDatum datum = new ParsedDatum(funCall.getArguments());
        String lang = datum.getLanguage();
        if( lang.equals("") || lang.contains(getlanguageName())){//unknown or specified lang
            Text key = new Text(datum.getUrl());
            String title = "";
            //the parsed title will be the first line of the the mahout sequence file
            for(int i = 1; i <= _titleWeight; i++){
                title += datum.getTitle();
                title += '\n';//make sure new line to separate title copies
            }
            // todo: this assumes there is never a '\n' in a page title except the ones we put there
            // otherwise the display code will not know how to combine them
            // sort of a hacky way to boost the title!
            Text value = new Text(title + '\n' + datum.getParsedText());
            Tuple mahoutKeyValue = new Tuple(key, value);
            funCall.getOutputCollector().add(mahoutKeyValue);
        } else {
            //this test seldom works because language detection in tika is weak, I think
            LOGGER.debug("Datum filtered because language = " + lang + '\n');
        }
    }
}
