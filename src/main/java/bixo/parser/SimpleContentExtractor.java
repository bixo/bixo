package bixo.parser;

@SuppressWarnings("serial")
public class SimpleContentExtractor extends BaseContentExtractor {
    private StringBuilder _content = new StringBuilder();

    @Override
    public void reset() {
        super.reset();
        _content.setLength(0);
    }
    
    @Override
    public void addContent(char[] ch, int start, int length) {
        _content.append(ch, start, length);
    }

    @Override
    public void addContent(char ch) {
        _content.append(ch);
    }

    @Override
    public String getContent() {
        return _content.toString();
    }
}