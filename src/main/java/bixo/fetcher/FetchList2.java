package bixo.fetcher;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import bixo.datum.ScoredUrlDatum;
import bixo.utils.DomainNames;

public class FetchList2 {
    private static final Logger LOGGER = Logger.getLogger(FetchList2.class);
    
    private String _domain;
    private List<ScoredUrlDatum> _urls;
    
    
    public FetchList2(List<ScoredUrlDatum> urls) {
        _urls = urls;
        
        if (urls.size() > 0) {
            _domain = DomainNames.safeGetHost(urls.get(0).getUrl());
        } else {
            LOGGER.warn("Empty URL list passed to FetchList");
            _domain = "<empty list>";
        }
    }

    public List<ScoredUrlDatum> getUrls() {
        return _urls;
    }
    
    public ScoredUrlDatum get(int index) {
        return _urls.get(index);
    }

    public Iterator<ScoredUrlDatum> iterator() {
        return _urls.iterator();
    }

    public int size() {
        return _urls.size();
    }

    public String getDomain() {
        return _domain;
    }

}
