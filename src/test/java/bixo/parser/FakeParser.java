package bixo.parser;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;

@SuppressWarnings("serial")
public class FakeParser implements IParser {

    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) {

        return new ParsedDatum("url", "someParsedText", new Outlink[0], fetchedDatum.getMetaDataMap());
    }


}
