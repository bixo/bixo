/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
