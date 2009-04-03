package bixo.fetcher;

public class FetchResult {
    private FetchStatusCode _statusCode;
    private FetchContent _content;
    
    public FetchResult(FetchStatusCode statusCode, FetchContent content) {
        _statusCode = statusCode;
        _content = content;
    }

    public FetchStatusCode getStatusCode() {
        return _statusCode;
    }

    public FetchContent getContent() {
        return _content;
    }
    
    public String toString() {
        int size = _content.getContent() == null ? 0 : _content.getContent().length;
        return String.format("%s (status code %d, size %d)", _content.getFetchedUrl(), _statusCode.getCode(), size);
    }
}
