package bixo.utils;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

public class UrlUtils {
	private static final Logger LOGGER = Logger.getLogger(UrlUtils.class);
	
	public static String makeUrl(URL baseUrl, String relativeUrl) throws MalformedURLException {
		// Peel off cases of URLs that aren't actually URLs, or at least don't have protocols
		// that the Java URL class knows about.
		if (relativeUrl.startsWith("javascript:") || relativeUrl.startsWith("mailto:") || relativeUrl.startsWith("about:")) {
			return relativeUrl;
		}
		
		// We need to handle one special case, where the relativeUrl is just
		// a query string (like "?pid=1"), and the baseUrl doesn't end with
		// a '/'. In that case, the URL class removes the last portion of
		// the path, which we don't want.
		
		try {
			if (!relativeUrl.startsWith("?") || (baseUrl.getPath().length() == 0) || baseUrl.getPath().endsWith("/")) {
				return new URL(baseUrl, relativeUrl).toExternalForm();
			} else {
				// for the <file> param, use the base path (which won't include the query string) plus
				// the relative query string.
				return new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(), baseUrl.getPath() + relativeUrl).toExternalForm();
			}
		} catch (MalformedURLException e) {
			// we can get things like "mail:xxx" (versus mailto:) in href attributes.
			LOGGER.warn("Invalid relativeUrl parameter: " + relativeUrl);
			return relativeUrl;
		}
	}
}
