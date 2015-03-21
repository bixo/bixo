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
package bixo.datum;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings({ "serial", "rawtypes" })
public class UrlAndMetadata implements Comparable, Serializable {

    private String _url;
    private Map<String, Comparable> _metadata;
    
    public UrlAndMetadata(String url, Map<String, Comparable> metadata) {
        _url = url;
        _metadata = metadata;
    }

    
    public String getUrl() {
        return _url;
    }


    public void setUrl(String url) {
        _url = url;
    }


    public Map<String, Comparable> getMetadata() {
        return _metadata;
    }


    public void setMetadata(Map<String, Comparable> metadata) {
        _metadata = metadata;
    }


    @Override
    public int compareTo(Object o) {
        UrlAndMetadata other = (UrlAndMetadata)o;
        
        int result = _url.compareTo(other._url);
        if (result == 0) {
            // TODO KKr - use Metadata type, with its compareTo support
        }
        
        return result;
    }
    
    
}
