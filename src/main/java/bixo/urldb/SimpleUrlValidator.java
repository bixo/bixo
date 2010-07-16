package bixo.urldb;

import java.net.URI;
import java.net.URL;
import java.util.regex.Pattern;

@SuppressWarnings("serial")
public class SimpleUrlValidator implements IUrlValidator {
    private static final Pattern HTTP_PATTERN = Pattern.compile("^(http|https):");

    @Override
    public boolean validate(String urlString) {
        if (!HTTP_PATTERN.matcher(urlString).find()) {
            return false;
        }

        try {
            new URL(urlString);
            URI uri = new URI(urlString);
            return uri.getHost() != null;
        } catch (Exception e) {
            return false;
        }
    }

}
