package bixo.urldb;

import java.net.URI;
import java.util.regex.Pattern;

@SuppressWarnings("serial")
public class SimpleUrlValidator implements IUrlValidator {
    private static final Pattern NOT_HTTP_PATTERN = Pattern.compile("^(javascript:|mailto:|ftp:)");

    @Override
    public boolean validate(String url) {
        if (NOT_HTTP_PATTERN.matcher(url).find()) {
            return false;
        }

        try {
            URI uri = new URI(url);
            return uri.getHost() != null;
        } catch (Exception e) {
            return false;
        }
    }

}
