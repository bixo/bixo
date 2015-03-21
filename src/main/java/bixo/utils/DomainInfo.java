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

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class DomainInfo {

    private static final String TESTING_DOMAIN_PREFIX = "bixo-test-domain-";
    
    // Pay no attention to this cheesy hack - special case handling of
    // certain domains so we can test without trying to resolve domain names.
    private static final Pattern TESTING_DOMAIN_PATTERN = Pattern.compile(TESTING_DOMAIN_PREFIX + "\\d+\\.com");

    public static String makeTestDomain(int index) {
        return TESTING_DOMAIN_PREFIX + index + ".com";
    }
    
    private String _protocolAndDomain;
    private String _domain;
    private String _hostAddress;

    public DomainInfo(String protocolAndDomain) throws UnknownHostException, MalformedURLException, URISyntaxException {
        _protocolAndDomain = protocolAndDomain;
        URL url = new URL(protocolAndDomain);

        // Since URI class is stricter than URL when validating, let's use the URL result to try to create a URI
        new URI(url.getProtocol(), null, url.getHost(), url.getPort(), url.getPath(), url.getQuery(), null);
        
        _domain = url.getHost();
        
        if (TESTING_DOMAIN_PATTERN.matcher(_domain).matches()) {
            _hostAddress = _domain;
        } else {
            _hostAddress = InetAddress.getByName(_domain).getHostAddress();
        }
    }

    public String getProtocolAndDomain() {
        return _protocolAndDomain;
    }

    public String getDomain() {
        return _domain;
    }

    public String getHostAddress() {
        return _hostAddress;
    }
    
    public boolean isValidHostAddress() {
        // FUTURE - fill out full set of special IP addresses. Though
        // for local server testing, 127.0.0.1 is actually valid.
        return !_hostAddress.equals("0.0.0.0");
    }
    
}
