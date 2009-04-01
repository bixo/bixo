package bixo;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import junit.framework.TestCase;

public class DomainNamesTest extends TestCase {
	public final void testIPv4() throws MalformedURLException {
		assertEquals("1.2.3.4", DomainNames.getPLD("1.2.3.4"));
		
		URL url = new URL("http://1.2.3.4:8080/a/b/c?q=1");
		assertEquals("1.2.3.4", DomainNames.getPLD(url));
	}
	
	public final void testIPv6() throws MalformedURLException, UnknownHostException {
		InetAddress inet = InetAddress.getByName("1080:0:0:0:8:800:200c:417a");
		URL url = new URL("http", inet.getHostAddress(), 8080, "a/b/c");
		assertEquals("[1080:0:0:0:8:800:200c:417a]", DomainNames.getPLD(url));
	}
	
	public final void testStandardDomains() throws MalformedURLException {
		assertEquals("xxx.com", DomainNames.getPLD("xxx.com"));
		assertEquals("xxx.com", DomainNames.getPLD("www.xxx.com"));
		assertEquals("xxx.com", DomainNames.getPLD("www.zzz.xxx.com"));
		assertEquals("xxx.com", DomainNames.getPLD(new URL("https://www.zzz.xxx.com:9000/a/b?c=d")));
	}
	
	public final void testBizDomains() {
		assertEquals("xxx.biz", DomainNames.getPLD("xxx.biz"));
		assertEquals("xxx.biz", DomainNames.getPLD("www.xxx.biz"));
	}

	// Japan (and uk) have shortened gTLDs before the country code.
	public final void testJapaneseDomains() {
		assertEquals("xxx.co.jp", DomainNames.getPLD("xxx.co.jp"));
		assertEquals("xxx.co.jp", DomainNames.getPLD("www.xxx.co.jp"));
		assertEquals("xxx.ne.jp", DomainNames.getPLD("www.xxx.ne.jp"));
	}
	
	// In Germany you can have xxx.de.com
	public final void testGermanDomains() {
		assertEquals("xxx.de.com", DomainNames.getPLD("xxx.de.com"));
		assertEquals("xxx.de.com", DomainNames.getPLD("www.xxx.de.com"));
	}
	
	// Typical international domains look like xxx.com.it
	public final void testItalianDomains() {
		assertEquals("xxx.com.it", DomainNames.getPLD("xxx.com.it"));
		assertEquals("xxx.com.it", DomainNames.getPLD("www.xxx.com.it"));
	}
	
	
}
