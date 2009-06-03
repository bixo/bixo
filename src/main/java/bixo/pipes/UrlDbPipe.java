package bixo.pipes;

import bixo.datum.UrlDatum;
import bixo.operations.LastUpdated;
import bixo.operations.UrlFilterFunction;
import bixo.urldb.IUrlFilter;
import cascading.operation.Aggregator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class UrlDbPipe extends SubAssembly {

    // TODO sg: should we have more than one filter and filter?
    public UrlDbPipe(Pipe pipe, IUrlFilter urlFilter, Fields metaDataFields) {
        Pipe urlDbPipe = new Pipe("urlDb_pipe", pipe);

        Fields urlDatumFields = UrlDatum.FIELDS.append(metaDataFields);
        urlDbPipe = new Each(urlDbPipe, new UrlFilterFunction(urlDatumFields, UrlDatum.URL_FIELD, urlFilter));

        // we want the url with the latest update.
        urlDbPipe = new GroupBy(urlDbPipe, new Fields(UrlDatum.URL_FIELD));
        Aggregator<Tuple> last = new LastUpdated(UrlDatum.FIELDS);
        urlDbPipe = new Every(urlDbPipe, last, Fields.RESULTS);

        setTails(urlDbPipe);
    }

}
