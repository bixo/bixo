package bixo.parser;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;

@SuppressWarnings("serial")
public class FakeParserFactory implements IParserFactory {

    @Override
    public IParser newParser() {
        return new IParser() {

            @Override
            public ParsedDatum parse(FetchedDatum fetchedDatum) {

                return new ParsedDatum("url", "someParsedText", new Outlink[0], fetchedDatum.getMetaDataMap());
            }

        };
    }

}
