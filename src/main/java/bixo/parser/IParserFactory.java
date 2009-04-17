package bixo.parser;

import java.io.Serializable;

import bixo.fetcher.IHttpFetcher;

public interface IParserFactory extends Serializable {

    IParser newParser();

}
