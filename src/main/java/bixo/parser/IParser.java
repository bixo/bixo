package bixo.parser;

import java.io.Serializable;

import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;

public interface IParser extends Serializable {

    ParsedDatum parse(FetchedDatum fetchedDatum) throws Exception;

}
