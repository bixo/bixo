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
package bixo.parser;

import bixo.config.ParserPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;

@SuppressWarnings("serial")
public class FakeParser extends BaseParser {

    public FakeParser() {
        super(new ParserPolicy());
    }

    @Override
    public ParsedDatum parse(FetchedDatum fetchedDatum) {
        ParsedDatum parsedDatum = new ParsedDatum("url", "127.0.0.1", "someParsedText", "en", "title", new Outlink[0], null);
        parsedDatum.setPayload(fetchedDatum.getPayload());
        return parsedDatum;
    }

}
