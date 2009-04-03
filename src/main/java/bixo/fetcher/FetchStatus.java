package bixo.fetcher;

public class FetchStatus {
    // TODO KKr - define enum type (FetchStatusCode?) and use that here, versus raw int
    private int _code;

    public FetchStatus(int code) {
        _code = code;
    }

    public int getCode() {
        return _code;
    }

    public void setCode(int code) {
        _code = code;
    }
    
}
