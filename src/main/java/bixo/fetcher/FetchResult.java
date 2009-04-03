package bixo.fetcher;

public class FetchResult {
    private FetchStatus _status;
    private FetchContent _content;
    
    public FetchResult(FetchStatus status, FetchContent content) {
        _status = status;
        _content = content;
    }

    public FetchStatus getStatus() {
        return _status;
    }

    public FetchContent getContent() {
        return _content;
    }
    
    public String toString() {
        int size = _content.getContent() == null ? 0 : _content.getContent().length;
        return String.format("%s (status %d, size %d)", _content.getFetchedUrl(), _status.getCode(), size);
    }
}
