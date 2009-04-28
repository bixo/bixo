package bixo.parser.html;

import bixo.parser.IParser;
import bixo.parser.IParserFactory;

@SuppressWarnings("serial")
public class HtmlParserFactory implements IParserFactory {

    @Override
    public IParser newParser() {
        return new HtmlParser();
    }
}
