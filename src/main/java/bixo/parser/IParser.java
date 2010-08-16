package bixo.parser;

import java.io.Serializable;

import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;

public interface IParser extends Serializable {

    // TODO - When this is an abstract class, add constructor/get/set for ParserPolicy
    
    public ParsedDatum parse(FetchedDatum fetchedDatum) throws Exception;

}
