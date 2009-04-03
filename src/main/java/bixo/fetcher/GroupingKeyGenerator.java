package bixo.fetcher;

import java.io.IOException;
import java.io.Serializable;

import bixo.items.UrlItem;

public interface GroupingKeyGenerator extends Serializable{
    public String getGroupingKey(UrlItem urlItem) throws IOException;
}
