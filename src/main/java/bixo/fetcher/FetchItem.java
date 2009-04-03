/**
 * 
 */
package bixo.fetcher;

import java.net.URL;

class FetchItem implements Comparable<FetchItem> {
	public URL _url;
	public float _score;
	
	public FetchItem(URL url, float score) {
		_score = score;
		_url = url;
	}

	
	public URL getUrl() {
		return _url;
	}


	public float getScore() {
		return _score;
	}


	public int compareTo(final FetchItem o) {
		// Sort in reverse order, such that higher scores are first.
		if (_score > o._score) {
			return -1;
		} else if (_score < o._score) {
			return 1;
		} else {
			return 0;
		}
	}
}