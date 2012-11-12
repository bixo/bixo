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
package com.finderbots.utilities;

import cascading.flow.Flow;
import cascading.flow.PlannerException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

@SuppressWarnings("deprecation")
public class ExportPinterestPrefsTool {

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }


    public static void main(String[] args) {
        ExportToolOptions options = new ExportToolOptions();
        CmdLineParser parser = new CmdLineParser(options);
        String outputDirName;

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        try {
            outputDirName = options.getOutputDir();
            JobConf conf = new JobConf();
            Path outputPath = new Path(outputDirName);
            Path crawlPath = new Path(options.getCrawlDir());
            FileSystem fs = outputPath.getFileSystem(conf);

            // get the urls of users, urls of followed people, make sure they are unique, create an index
            // and write the ids out as CSV file of prefs for mahout input.
            Flow exportPinterestPrefsWorkFlow = ExportPinterestPrefsWorkflow.createFlow(crawlPath, options);
            exportPinterestPrefsWorkFlow.complete();
        } catch (PlannerException e) {
            e.writeDOT("failed-flow.dot");
            System.err.println("PlannerException: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }


}
