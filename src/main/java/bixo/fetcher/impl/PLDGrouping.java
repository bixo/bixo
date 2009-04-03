package bixo.fetcher.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import bixo.fetcher.GroupingKeyGenerator;
import bixo.items.UrlItem;
import bixo.utils.DomainNames;

public class PLDGrouping implements GroupingKeyGenerator {

    @Override
    public String getGroupingKey(UrlItem urlItem) throws IOException {
        try {
            return DomainNames.getPLD(new URL(urlItem.getUrl()));
        } catch (MalformedURLException e) {
            throw new IOException("Unable to parse url string into URL object.", e);
        }
    }

}
