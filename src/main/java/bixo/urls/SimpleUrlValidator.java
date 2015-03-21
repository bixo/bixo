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
package bixo.urls;

import java.net.URI;
import java.net.URL;
import java.util.regex.Pattern;

@SuppressWarnings("serial")
public class SimpleUrlValidator extends BaseUrlValidator {
    private static final Pattern HTTP_PATTERN = Pattern.compile("^(http|https):");

    @Override
    public boolean isValid(String urlString) {
        if (!HTTP_PATTERN.matcher(urlString).find()) {
            return false;
        }

        try {
            URL url = new URL(urlString);
            String hostname = url.getHost();
            if ((hostname == null) || (hostname.length() == 0)) {
                return false;
            }
            
            URI uri = new URI(urlString);
            hostname = uri.getHost();
            if ((hostname == null) || (hostname.length() == 0)) {
                return false;
            }
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
