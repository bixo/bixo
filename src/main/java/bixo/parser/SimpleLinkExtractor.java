package bixo.parser;

import java.util.ArrayList;
import java.util.List;

import bixo.datum.Outlink;

@SuppressWarnings("serial")
public class SimpleLinkExtractor extends BaseLinkExtractor {
    private List<Outlink> _outlinks = new ArrayList<Outlink>();

    @Override
    public void reset() {
        super.reset();
        _outlinks.clear();
    }
    

    @Override
    public void addLink(Outlink link) {
        _outlinks.add(link);
    }

    @Override
    public Outlink[] getLinks() {
        return _outlinks.toArray(new Outlink[_outlinks.size()]);
    }
}