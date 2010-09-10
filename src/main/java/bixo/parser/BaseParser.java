package bixo.parser;

import java.io.Serializable;

import bixo.config.ParserPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;

@SuppressWarnings("serial")
public abstract class BaseParser implements Serializable {

    private ParserPolicy _policy;
    
    public BaseParser(ParserPolicy policy) {
        _policy = policy;
    }

    public ParserPolicy getParserPolicy() {
        return _policy;
    }

    public abstract ParsedDatum parse(FetchedDatum fetchedDatum) throws Exception;

}
