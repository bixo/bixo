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
