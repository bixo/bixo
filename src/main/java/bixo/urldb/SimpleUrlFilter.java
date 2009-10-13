package bixo.urldb;

import java.net.MalformedURLException;
import java.net.URL;

import bixo.datum.UrlDatum;

/**
 * Simple UrlFilter that just makes sure the URL is considered to be
 * "valid" by the rather simple rules encoded in the java.net URL class.
 */
@SuppressWarnings("serial")
public class SimpleUrlFilter implements IUrlFilter {

	@Override
	public boolean isRemove(UrlDatum datum) {
        try {
            new URL(datum.getUrl());
            return false;
        } catch (MalformedURLException e) {
            return true;
        }
	}

}
