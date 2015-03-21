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
package bixo.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;
import bixo.fetcher.BaseFetcher;
import bixo.operations.UrlLengthener;
import bixo.utils.ConfigUtils;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.NullSinkTap;


public class LengthenUrlsTool {

    private static String readInputLine() throws IOException {
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(isr);
        
        try {
            return br.readLine();
        } finally {
            // TODO KKr - will this actually close System.in?
            // Should I reuse this buffered reader? Check out password masking code.
            // br.close();
        }
    }

    /**
     * @param args - URL to fetch, or path to file of URLs
     */
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        try {
            String url = null;
            if (args.length == 0) {
                System.out.print("URL to lengthen: ");
                url = readInputLine();
                if (url.length() == 0) {
                    System.exit(0);
                }

                if (!url.startsWith("http://")) {
                    url = "http://" + url;
                }
            } else if (args.length != 1) {
                System.out.print("A single URL or filename parameter is allowed");
                System.exit(0);
            } else {
                url = args[0];
            }

            String filename;
            if (!url.startsWith("http://")) {
                // It's a path to a file of URLs
                filename = url;
            } else {
                // We have a URL that we need to write to a temp file.
                File tempFile = File.createTempFile("LengthenUrlsTool", "txt");
                filename = tempFile.getAbsolutePath();
                FileWriter fw = new FileWriter(tempFile);
                IOUtils.write(url, fw);
                fw.close();
            }

            System.setProperty("bixo.root.level", "TRACE");
            // Uncomment this to see the wire log for HttpClient
            // System.setProperty("bixo.http.level", "DEBUG");

            BaseFetcher fetcher = UrlLengthener.makeFetcher(10, ConfigUtils.BIXO_TOOL_AGENT);

            Pipe pipe = new Pipe("urls");
            pipe = new Each(pipe, new UrlLengthener(fetcher));
            pipe = new Each(pipe, new Debug());

            BixoPlatform platform = new BixoPlatform(LengthenUrlsTool.class, Platform.Local);
            BasePath filePath = platform.makePath(filename);
            TextLine textLineLocalScheme = new TextLine(new Fields("url"));
            Tap sourceTap = platform.makeTap(textLineLocalScheme, filePath, SinkMode.KEEP);
            SinkTap sinkTap = new NullSinkTap(new Fields("url"));
            
            FlowConnector flowConnector = platform.makeFlowConnector();
            Flow flow = flowConnector.connect(sourceTap, sinkTap, pipe);

            flow.complete();
        } catch (Exception e) {
            System.err.println("Exception running tool: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
