package bixo.datum;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings({ "unchecked", "serial" })
public class UrlAndMetadata implements Comparable, Serializable {

    private String _url;
    private Map<String, Comparable> _metadata;
    
    public UrlAndMetadata(String url, Map<String, Comparable> metadata) {
        _url = url;
        _metadata = metadata;
    }

    
    public String getUrl() {
        return _url;
    }


    public void setUrl(String url) {
        _url = url;
    }


    public Map<String, Comparable> getMetadata() {
        return _metadata;
    }


    public void setMetadata(Map<String, Comparable> metadata) {
        _metadata = metadata;
    }


    @Override
    public int compareTo(Object o) {
        UrlAndMetadata other = (UrlAndMetadata)o;
        
        int result = _url.compareTo(other._url);
        if (result == 0) {
            // TODO KKr - use Metadata type, with its compareTo support
        }
        
        return result;
    }
    
    
}
