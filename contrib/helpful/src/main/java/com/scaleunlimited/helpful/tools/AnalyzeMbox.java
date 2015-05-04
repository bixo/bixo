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
package com.scaleunlimited.helpful.tools;


import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.config.BixoPlatform;
import bixo.datum.FetchedDatum;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.helpful.operations.CalcMessageScoreBuffer;
import com.scaleunlimited.helpful.operations.FieldNames;
import com.scaleunlimited.helpful.operations.ParseEmailFunction;
import com.scaleunlimited.helpful.operations.SumScoresBuffer;

public class AnalyzeMbox {
	
	private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }



	@SuppressWarnings("rawtypes")
    public static void main(String[] args) {
		AnalyzeMboxOptions options = new AnalyzeMboxOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        String inputFileName = options.getInputFile();
        String outputDirName = options.getOutputDir();
        
        try {
            BixoPlatform platform = new BixoPlatform(AnalyzeMbox.class, options.getPlatformMode());
            // Create the input (source tap), which is just a sequence file reader. We assume
        	// that the file already has the results of splitting the mbox file into emails.
            BasePath inputPath = platform.makePath(inputFileName);
            inputPath.assertExists("input file");
            Tap sourceTap = platform.makeTap(platform.makeBinaryScheme(FetchedDatum.FIELDS), inputPath);
            
            Pipe pipe = new Pipe("Email Analyzer");
            pipe = new Each(pipe, new ParseEmailFunction());
            
            // We'll get output that has ANALYZED_EMAIL_FIELDS in it. We want to group by
            // the message-id field, and then do an aggregation on that of the scores.
            pipe = new GroupBy(pipe, new Fields(FieldNames.MESSAGE_ID));
            pipe = new Every(pipe, new CalcMessageScoreBuffer(), Fields.RESULTS);

            // Now we want to sum the scores for each user, which is another grouping/summing.
            pipe = new GroupBy(pipe, new Fields(FieldNames.EMAIL_ADDRESS));
            pipe = new Every(pipe, new SumScoresBuffer(), Fields.RESULTS);
            
            // Let's filter out anybody with an uninteresting score.
            ExpressionFilter filter = new ExpressionFilter(String.format("%s <= 0.0", FieldNames.SUMMED_SCORE), Double.class);
            pipe = new Each(pipe, filter);
            
            // And let's sort in reverse order (high to low score)
            pipe = new GroupBy(pipe, new Fields(FieldNames.SUMMED_SCORE), true);

            // Create the output (sink tap)
            Tap sinkTap = platform.makeTap(platform.makeTextScheme(), 
                            platform.makePath(outputDirName), SinkMode.REPLACE);
            
            // Finally we can run it.
            FlowConnector flowConnector = platform.makeFlowConnector();
            Flow flow = flowConnector.connect(sourceTap, sinkTap, pipe);
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running AnalyzeMbox: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

	}

}
