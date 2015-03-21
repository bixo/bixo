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

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseUrlNormalizer implements Serializable {

    /**
     * Convert <url> into a normalized format, where unimportant differences between
     * two URLs have been removed.
     * 
     * @param url - URL to normalize. Might not be valid, e.g. missing a protocol
     * @return - normalized URL. Still might not be valid, if input URL (for example)
     *           uses an unknown protocol and thus no checks can be done.
     */
    public abstract String normalize(String url);
}
