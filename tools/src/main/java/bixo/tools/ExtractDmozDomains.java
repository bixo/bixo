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
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractDmozDomains {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // <ExternalPage about="http://darkkaminari.net">
        Pattern aboutPattern = Pattern.compile(".*ExternalPage[ \t]+about[ \t]*=[ \t]*\"([^\"]+)\".*");
        Matcher aboutMatcher = aboutPattern.matcher("");

        //   <link r:resource="http://www.geocities.com/kaseychan17/index.html"/>
        Pattern resourcePattern = Pattern.compile(".*link[ \t]+r:resource[ \t]*=[ \t]*\"([^\"]+)\".*");
        Matcher resourceMatcher = resourcePattern.matcher("");

        HashSet<String> hostNames = new HashSet<String>(1000000);
        
        try {
            String inputFile = args[0];
            PrintStream outputStream;
            if (args.length == 2) {
                outputStream = new PrintStream(new File(args[1]));
            } else {
                outputStream = System.out;
            }

            InputStreamReader isr = new InputStreamReader(new FileInputStream(inputFile), "UTF-8");
            BufferedReader reader = new BufferedReader(isr);
            
            int numLines = 0;
            int numUrls = 0;
            
            String curLine;
            String urlString;
            while ((curLine = reader.readLine()) != null) {
                numLines += 1;
                if ((numLines % 100000) == 0) {
                    System.out.println(String.format(" %d/%d/%d", numLines, numUrls, hostNames.size()));
                } else if ((numLines % 1000) == 0) {
                    System.out.print('.');
                }

                urlString = null;
                if (aboutMatcher.reset(curLine).matches()) {
                    urlString = aboutMatcher.group(1);
                } else if (resourceMatcher.reset(curLine).matches()) {
                    urlString = resourceMatcher.group(1);
                }

                if (urlString != null) {
                    numUrls += 1;
                    
                    try {
                        URL url = new URL(URLDecoder.decode(urlString, "UTF-8"));
                        String hostName = url.getHost();
                        if (hostNames.add(hostName)) {
                            outputStream.println(hostName);
                        }
                    } catch (MalformedURLException e) {
                        System.err.println();
                        System.err.println("Invalid URL found in dmoz data: " + urlString);
                        System.err.println("Exception is " + e.getMessage());
                    } catch (IllegalArgumentException e) {
                        System.err.println();
                        System.err.println("Invalid URL found in dmoz data: " + urlString);
                        System.err.println("Exception is " + e.getMessage());
                    }
                }
            }
            
            System.out.println("Processed " + numLines + " lines of input");
            System.out.println("Found " + numUrls + " URLs");
            System.out.println("Found " + hostNames.size() + " unique host names");
        } catch (Exception e) {
            System.err.println("Exception while running tool: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
