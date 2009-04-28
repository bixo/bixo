package bixo.parser;

import bixo.tuple.FetchedDatum;
import bixo.tuple.ParsedDatum;

public interface IParser {

    ParsedDatum parse(FetchedDatum fetchedDatum);

}
