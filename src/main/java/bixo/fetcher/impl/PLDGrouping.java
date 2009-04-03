package bixo.fetcher.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import bixo.fetcher.GroupingKeyGenerator;
import bixo.items.UrlItem;
import bixo.utils.DomainNames;

public class PLDGrouping implements GroupingKeyGenerator {

    private DomainNames _domainNames;

    public PLDGrouping() {
        _domainNames = new DomainNames();
    }

    @Override
    public String getGroupingKey(UrlItem urlItem) throws IOException {
        try {
            return _domainNames.getPLD(new URL(urlItem.getUrl()));
        } catch (MalformedURLException e) {
            throw new IOException("Unable to parse url string into URL object.", e);
        }
    }

}
