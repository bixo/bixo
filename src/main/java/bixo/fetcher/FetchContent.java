package bixo.fetcher;

public class FetchContent {
    private String _baseUrl;
    private String _fetchedUrl;
    private long _fetchTime;
    
    private byte[] _content;
    private String _contentType;
    
    public FetchContent(String baseUrl, String fetchedUrl, long fetchTime, byte[] content, String contentType) {
        _baseUrl = baseUrl;
        _fetchedUrl = fetchedUrl;
        _fetchTime = fetchTime;
        _content = content;
        _contentType = contentType;
    }

    public String getBaseUrl() {
        return _baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        _baseUrl = baseUrl;
    }

    public String getFetchedUrl() {
        return _fetchedUrl;
    }

    public void setFetchedUrl(String fetchedUrl) {
        _fetchedUrl = fetchedUrl;
    }

    public long getFetchTime() {
        return _fetchTime;
    }

    public void setFetchTime(long fetchTime) {
        _fetchTime = fetchTime;
    }

    public byte[] getContent() {
        return _content;
    }

    public void setContent(byte[] content) {
        _content = content;
    }

    public String getContentType() {
        return _contentType;
    }

    public void setContentType(String contentType) {
        _contentType = contentType;
    }
    
    
}
