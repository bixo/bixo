package bixo.parser;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;

@SuppressWarnings("serial")
public class FakeParser implements IParser {

    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) {
        ParsedDatum parsedDatum = new ParsedDatum("url", "127.0.0.1", "someParsedText", "en", "title", new Outlink[0], null);
        parsedDatum.setPayload(fetchedDatum.getPayload());
        return parsedDatum;
    }
}
