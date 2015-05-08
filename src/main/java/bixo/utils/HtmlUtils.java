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
package bixo.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HtmlUtils {

    // <META NAME="ROBOTS" CONTENT="xxx"> 
    private static final Pattern META_ROBOTS_PATTERN = Pattern
    .compile("(?is)<meta\\s+name\\s*=\\s*['\\\"]\\s*robots['\\\"]\\s+content\\s*=\\s*['\\\"]([^'\"]+)['\\\"]");

    // <META HTTP-EQUIV="PRAGMA" CONTENT="NO-CACHE">
    private static final Pattern META_PRAGMA_PATTERN = Pattern
    .compile("(?is)<meta\\s+http-equiv\\s*=\\s*['\\\"]\\s*pragma['\\\"]\\s+content\\s*=\\s*['\\\"]no-cache['\\\"]");

    // <META HTTP-EQUIV="CACHE-CONTROL" CONTENT="XXX">
    private static final Pattern META_CACHE_CONTROL_PATTERN = Pattern
    .compile("(?is)<meta\\s+http-equiv\\s*=\\s*['\\\"]\\s*cache-control['\\\"]\\s+content\\s*=\\s*['\\\"]([^'\"]+)['\\\"]");

    // <META HTTP-EQUIV="CONTENT-TYPE" CONTENT="XXX">
    private static final Pattern META_CONTENT_TYPE_PATTERN = Pattern
    .compile("(?is)<meta\\s+http-equiv\\s*=\\s*['\\\"]\\s*content-type['\\\"]\\s+content\\s*=\\s*['\\\"]([^'\"]+)['\\\"]");

    // <META HTTP-EQUIV="CONTENT-LANGUAGE" CONTENT="XXX">
    private static final Pattern META_CONTENT_LANGUAGE_PATTERN = Pattern
    .compile("(?is)<meta\\s+http-equiv\\s*=\\s*['\\\"]\\s*content-language['\\\"]\\s+content\\s*=\\s*['\\\"]([^'\"]+)['\\\"]");

    // <META NAME="DC.LANGUAGE" CONTENT="XXX">
    private static final Pattern META_DC_LANGUAGE_PATTERN = Pattern
    .compile("(?is)<meta\\s+name\\s*=\\s*['\\\"]\\s*dc.language['\\\"]\\s+content\\s*=\\s*['\\\"]([^'\"]+)['\\\"]");

    private HtmlUtils() {
        // Enforce class isn't instantiated
    }
    
    public static boolean hasNoArchiveMetaTags(String htmlText) {
        Matcher m = META_ROBOTS_PATTERN.matcher(htmlText);
        if (m.find()) {
            String[] directives = m.group(1).toLowerCase().split(",");
            for (String directive : directives) {
                if (directive.equals("none") || directive.equals("noarchive")) {
                    return true;
                }
            }
        }

        m = META_PRAGMA_PATTERN.matcher(htmlText);
        if (m.find()) {
            return true;
        }
        
        m = META_CACHE_CONTROL_PATTERN.matcher(htmlText);
        if (m.find()) {
            String[] directives = m.group(1).toLowerCase().split(",");
            for (String directive : directives) {
                if (directive.equals("no-cache") || directive.equals("no-store") || directive.equals("private")) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    public static boolean hasNoFollowMetaTags(String htmlText) {
        Matcher m = META_ROBOTS_PATTERN.matcher(htmlText);
        if (m.find()) {
            String[] directives = m.group(1).toLowerCase().split(",");
            for (String directive : directives) {
                if (directive.equals("none") || directive.equals("nofollow")) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    public static boolean hasOnlyNonEnglishMetaTags(String htmlText) {
        Matcher m = META_CONTENT_TYPE_PATTERN.matcher(htmlText);
        if (m.find()) {
            String[] directives = m.group(1).toLowerCase().split(";");
            for (String directive : directives) {
                directive = directive.trim();
                if  (   (directive.equals("charset=gb2312"))
                    ||  (directive.equals("charset=gbk"))
                    ||  (directive.equals("charset=gb18030"))
                    ||  (directive.equals("charset=windows-1251"))
                    ||  (directive.equals("charset=iso-2022-jp"))
                    ||  (directive.equals("charset=euc-jp"))
                    ||  (directive.equals("charset=euc-kr"))) {
                    return true;
                }
            }
        }
        m = META_CONTENT_LANGUAGE_PATTERN.matcher(htmlText);
        if (m.find()) {
            String[] directives = m.group(1).toLowerCase().split(",");
            for (String directive : directives) {
                if (directive.trim().startsWith("en")) {
                    return false;
                }
            }
            return true;
        }
        m = META_DC_LANGUAGE_PATTERN.matcher(htmlText);
        if (m.find()) {
            String[] directives = m.group(1).toLowerCase().split(";");
            for (String directive : directives) {
                if (directive.trim().startsWith("en")) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
