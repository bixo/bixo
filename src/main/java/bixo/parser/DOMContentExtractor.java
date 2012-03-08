package bixo.parser;

import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class DOMContentExtractor extends BaseContentExtractor {

    private SAXReader _saxReader;
    
    public DOMContentExtractor(SAXReader saxReader) {
        _saxReader = saxReader;
    }

    @Override
    public String getContent() {
        // TODO Auto-generated method stub
        return null;
    }

    
}
