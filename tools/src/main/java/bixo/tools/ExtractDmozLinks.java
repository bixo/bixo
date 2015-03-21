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
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractDmozLinks {

    public static void main(String[] args) {

        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream("src/main/resources/dmoz.properties"));
            String adultTopic = properties.getProperty("topics.adult");

            // <Topic r:id="Top/Adult/Arts/Animation/Anime/Fan_Works/Fan_Art">
            Pattern topicPattern = Pattern
                            .compile("[ \t]*<Topic[ \t]+r:id[ \t]*=[ \t]*\"([^\"]+)\">.*");
            Matcher topicMatcher = topicPattern.matcher("");

            // <link
            // r:resource="http://www.geocities.com/kaseychan17/index.html"/>
            Pattern resourcePattern = Pattern
                            .compile("[ \t]*<link(1|)[ \t]+r:resource[ \t]*=[ \t]*\"([^\"]+)\".*");
            Matcher resourceMatcher = resourcePattern.matcher("");

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

            boolean inGoodTopic = false;

            String curLine;
            String urlString;
            while ((curLine = reader.readLine()) != null) {
                numLines += 1;
                if ((numLines % 100000) == 0) {
                    System.out.println(String.format(" %d/%d", numLines, numUrls));
                } else if ((numLines % 1000) == 0) {
                    System.out.print('.');
                }

                boolean checkForTopic = !inGoodTopic;
                if (inGoodTopic) {
                    if (resourceMatcher.reset(curLine).matches()) {
                        urlString = resourceMatcher.group(2);
                        numUrls += 1;

                        try {
                            URL url = new URL(URLDecoder.decode(urlString, "UTF-8"));
                            outputStream.println(url.toExternalForm());
                        } catch (MalformedURLException e) {
                            System.err.println();
                            System.err.println("Invalid URL found in dmoz data: " + urlString);
                            System.err.println("Exception is " + e.getMessage());
                        } catch (IllegalArgumentException e) {
                            System.err.println();
                            System.err.println("Invalid URL found in dmoz data: " + urlString);
                            System.err.println("Exception is " + e.getMessage());
                        }
                    } else {
                        checkForTopic = true;
                    }
                }

                if (checkForTopic && (topicMatcher.reset(curLine).matches())) {
                    // Skip all adult links...we'll get some, but less this way.
                    inGoodTopic = !topicMatcher.group(1).contains(adultTopic);
                }
            }

            System.out.println("Processed " + numLines + " lines of input");
            System.out.println("Found " + numUrls + " URLs");
        } catch (Exception e) {
            System.err.println("Exception while running tool: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
