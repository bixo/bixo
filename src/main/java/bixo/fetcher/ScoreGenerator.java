package bixo.fetcher;

import java.io.Serializable;

import bixo.items.UrlItem;

public interface ScoreGenerator extends Serializable{

    double generateScore(UrlItem urlItem);

}
