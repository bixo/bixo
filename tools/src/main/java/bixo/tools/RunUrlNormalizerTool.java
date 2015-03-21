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

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;

import bixo.urls.BaseUrlNormalizer;
import bixo.urls.SimpleUrlNormalizer;

public class RunUrlNormalizerTool {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String curUrl = null;
        
        try {
            List<String> lines = FileUtils.readLines(new File(args[0]));

            BaseUrlNormalizer urlNormalizer = new SimpleUrlNormalizer();
            for (String url : lines) {
                curUrl = url;
                String normalized = urlNormalizer.normalize(curUrl);
                if (!normalized.equalsIgnoreCase(curUrl)) {
                    System.out.println(curUrl + " ==> " + normalized);
                }
            }
        } catch (Throwable t) {
            System.err.println("Exception while processing URLs: " + t.getMessage());
            System.err.println("Current url: " + curUrl);
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
