/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.operations;

import bixo.datum.UrlDatum;
import bixo.urldb.IUrlFilter;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class TextUrlParser extends BaseOperation<String> implements Function<String> {
    // Default implicit name for field returned by Cascading TextLine scheme that contains the
    // actual text from a line in a file.
    private static final String LINE = "line";
    
    private IUrlFilter[] _urlFilters = new IUrlFilter[0];

    public TextUrlParser() {
        super(UrlDatum.FIELDS);
    }

    public TextUrlParser(IUrlFilter... urlFilters) {
        super(UrlDatum.FIELDS);
        
        if (urlFilters != null) {
            _urlFilters = urlFilters;
        }
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<String> call) {
        TupleEntry entry = call.getArguments();
        UrlDatum datum = new UrlDatum(entry.getString(LINE));

        for (IUrlFilter filter : _urlFilters) {
            if (filter.isRemove(datum)) {
            	return;
            }
        }
        
        call.getOutputCollector().add(datum.toTuple());
    }
}
