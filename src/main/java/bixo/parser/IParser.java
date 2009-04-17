package bixo.parser;

import bixo.tuple.FetchContentTuple;
import bixo.tuple.ParseResultTuple;

public interface IParser {

    ParseResultTuple parse(FetchContentTuple contentTuple);

}
