package bixo.fetcher;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.http.HttpVersion;

import bixo.fetcher.beans.FetcherPolicy;

public class TryKeepAlive {
    private static final String[] APACHE_URLS = {
        "http://camel.apache.org/components.html",
        "http://camel.apache.org/http.html",
        "http://commons.apache.org/",
        "http://commons.apache.org/collections/",
        "http://commons.apache.org/components.html",
        "http://commons.apache.org/lang/",
        "http://gump.zones.apache.org/gump/test/httpcomponents/index.html",
        "http://hc.apache.org/",
        "http://hc.apache.org/commons-httpclient-lessons.html",
        "http://hc.apache.org/httpclient-3.x/",
        "http://hc.apache.org/httpcomponents-core/httpcore-nio/apidocs/org/apache/http/nio/package-summary.html",
        "http://hc.apache.org/httpcomponents-core/httpcore/apidocs/org/apache/http/io/package-summary.html",
        "http://hc.apache.org/httpcomponents-core/httpcore/apidocs/org/apache/http/package-summary.html",
        "http://hc.apache.org/httpcomponents-core/httpcore/apidocs/org/apache/http/package-use.html",
        "http://hc.apache.org/httpcomponents-core/httpcore/apidocs/org/apache/http/params/package-summary.html",
        "http://hc.apache.org/httpcomponents-core/httpcore/apidocs/org/apache/http/util/package-use.html",
        "http://issues.apache.org/activemq/browse/SMXCOMP-57",
        "http://issues.apache.org/activemq/browse/SM-372",
        "http://issues.apache.org/activemq/browse/SM-427",
        "http://jakarta.apache.org/jmeter/usermanual/component_reference.html",
        "http://myfaces.apache.org/tomahawk/index.html",
        "http://servicemix.apache.org/",
        "http://servicemix.apache.org/deployment-of-a-service-assembly-in-openesb.html",
        "http://servicemix.apache.org/embedding.html",
        "http://servicemix.apache.org/http.html",
        "http://servicemix.apache.org/servicemix-http-new-endpoints.html",
        "http://servicemix.apache.org/servicemix-http.html",
        "http://tapestry.apache.org/",
        "http://wiki.apache.org/HttpComponents/HttpComponents",
        "http://xml.apache.org/commons/components/external/",
        "http://xml.apache.org/commons/components/resolver/",
        "http://xml.apache.org/commons/components/resolver/resolver-article.html",
    };
    
    private static final String[] TRANSPAC_URLS = {
                    "http://www.transpac.com/",
                    "http://www.transpac.com/aboutus.html",
                    "http://www.transpac.com/contact.html",
                    "http://www.transpac.com/kkresume.html",
                    "http://www.transpac.com/csresume.html",
                    "http://www.transpac.com/projects.html",
                    "http://www.transpac.com/schmed"
    };

    private static class UrlWithHost implements Comparable<UrlWithHost> {
        private String _url;
        private String _host;
        
        public UrlWithHost(String url, String host) {
            _url = url;
            _host = host;
        }

        public String getUrl() {
            return _url;
        }

        public String getHost() {
            return _host;
        }

        @Override
        public int compareTo(UrlWithHost o) {
            return _url.compareTo(o._url);
        }
    }
    
    private static UrlWithHost convertUrlToIP(String url) throws MalformedURLException, UnknownHostException {
        URL realUrl = new URL(url);
        String host = realUrl.getHost();
        String hostIP = InetAddress.getByName(host).getHostAddress();
        URL ipUrl = new URL("http", hostIP, realUrl.getFile());
        return new UrlWithHost(ipUrl.toString(), host);
    }
    
    private static long tryNoKeepaliveHttp10(String[] urls) {
        long startTime = System.currentTimeMillis();
        for (String uri : urls) {
            HttpClientFactory factory = new HttpClientFactory(1, HttpVersion.HTTP_1_0, new FetcherPolicy());
            IHttpFetcher fetcher = factory.newHttpFetcher();
            fetcher.get(uri);
        }
        long stopTime = System.currentTimeMillis();
        return stopTime - startTime;
    }
    
    private static long tryHttp10(String[] urls) {
        HttpClientFactory factory = new HttpClientFactory(1, HttpVersion.HTTP_1_0, new FetcherPolicy());

        long startTime = System.currentTimeMillis();
        for (String uri : urls) {
            IHttpFetcher fetcher = factory.newHttpFetcher();
            fetcher.get(uri);
        }
        long stopTime = System.currentTimeMillis();
        return stopTime - startTime;
    }
    
    private static long tryHttp11(String[] urls) {
        HttpClientFactory factory = new HttpClientFactory(10, HttpVersion.HTTP_1_1, new FetcherPolicy());
        long startTime = System.currentTimeMillis();
        IHttpFetcher fetcher = factory.newHttpFetcher();
        for (String uri : urls) {
            fetcher.get(uri);
        }
        long stopTime = System.currentTimeMillis();
        return stopTime - startTime;
    }
    
    private static long tryByIPHttp11(String[] urls) throws MalformedURLException, UnknownHostException {
        ArrayList<UrlWithHost> ipUrls = new ArrayList<UrlWithHost>();
        for (String url : urls) {
            ipUrls.add(convertUrlToIP(url));
        }

        HttpClientFactory factory = new HttpClientFactory(10, HttpVersion.HTTP_1_1, new FetcherPolicy());
        long startTime = System.currentTimeMillis();
        IHttpFetcher fetcher = factory.newHttpFetcher();
        for (UrlWithHost url : ipUrls) {
            fetcher.get(url.getUrl(), url.getHost());
        }
        long stopTime = System.currentTimeMillis();
        return stopTime - startTime;
    }
    
    private static long trySortedByIPHttp11(String[] urls) throws MalformedURLException, UnknownHostException {
        ArrayList<UrlWithHost> ipUrls = new ArrayList<UrlWithHost>();
        
        for (String url : urls) {
            ipUrls.add(convertUrlToIP(url));
        }
        
        Collections.sort(ipUrls);
        
        HttpClientFactory factory = new HttpClientFactory(10, HttpVersion.HTTP_1_1, new FetcherPolicy());
        long startTime = System.currentTimeMillis();
        IHttpFetcher fetcher = factory.newHttpFetcher();
        for (UrlWithHost url : ipUrls) {
            fetcher.get(url.getUrl(), url.getHost());
        }
        long stopTime = System.currentTimeMillis();
        return stopTime - startTime;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        
        try {
            System.out.println("Http 1.0 no keep-alive: " + tryNoKeepaliveHttp10(APACHE_URLS) + "ms");
            System.out.println("Http 1.0 elapsed time: " + tryHttp10(APACHE_URLS) + "ms");
            System.out.println("Http 1.1 elapsed time: " + tryHttp11(APACHE_URLS) + "ms");
            System.out.println("Http 1.1 by IP elapsed time: " + tryByIPHttp11(APACHE_URLS) + "ms");
            System.out.println("Http 1.1 by sorted IP elapsed time: " + trySortedByIPHttp11(APACHE_URLS) + "ms");
        } catch (Throwable t) {
            System.err.println("Exception: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
