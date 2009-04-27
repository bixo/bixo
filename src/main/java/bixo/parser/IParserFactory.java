package bixo.parser;

import java.io.Serializable;

public interface IParserFactory extends Serializable {

    IParser newParser();

}
