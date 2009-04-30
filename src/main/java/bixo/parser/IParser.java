package bixo.parser;

import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;

public interface IParser {

    ParsedDatum parse(FetchedDatum fetchedDatum);

}
