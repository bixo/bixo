package bixo.utils;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlUtils {

	public static URL makeUrl(URL baseUrl, String relativeUrl) throws MalformedURLException {
		// We need to handle one special case, where the relativeUrl is just
		// a query string (like "?pid=1"), and the baseUrl doesn't end with
		// a '/'. In that case, the URL class removes the last portion of
		// the path, which we don't want.
		
		if (!relativeUrl.startsWith("?") || (baseUrl.getPath().length() == 0) || baseUrl.getPath().endsWith("/")) {
			return new URL(baseUrl, relativeUrl);
		}

		// for the <file> param, use the base path (which won't include the query string) plus
		// the relative query string.
		return new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(), baseUrl.getPath() + relativeUrl);
	}
}
