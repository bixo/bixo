package com.scaleunlimited.helpful.pipes;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

import com.scaleunlimited.helpful.operations.ParseModMboxPageFunction;

@SuppressWarnings("serial")
public class ModMboxPipe extends SubAssembly {

	public ModMboxPipe(Pipe fetchedDatumProvider) {
		Pipe parsePipe = new Pipe("mod_mbox page parser", fetchedDatumProvider);
		
		// The fetchedDatumProvider will pass us a stream of FetchedDatum tuples. For each,
		// we want to parse the HTML and extract the actual mbox file URLs, which we'll
		// pass on as UrlDatum tuples.
		parsePipe = new Each(parsePipe, new ParseModMboxPageFunction());
		
		setTails(parsePipe);
	}
}
