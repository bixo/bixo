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
package com.scaleunlimited.helpful.operations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.mbox.MboxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.scaleunlimited.cascading.NullContext;

import bixo.datum.FetchedDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


@SuppressWarnings({"serial", "rawtypes"})
public class ParseEmailFunction extends BaseOperation<NullContext> implements Function<NullContext> {
	private static final Logger LOGGER = LoggerFactory.getLogger(ParseEmailFunction.class);
	
	private static final Fields OUTPUT_FIELDS = new Fields(FieldNames.MESSAGE_ID, FieldNames.EMAIL_ADDRESS, FieldNames.EMAIL_NAME, FieldNames.SCORE);

	private static final Pattern THANKS_AT_END_PATTERN = Pattern.compile("\rthank[a-zA-Z ]{1,40}(|,|\\.|!)\r", Pattern.CASE_INSENSITIVE);
	private static final Pattern FULL_EMAIL_ADDRESS_PATTERN = Pattern.compile("(.*)<(.+@.+)>");
	private static final Pattern SIMPLE_EMAIL_ADDRESS_PATTERN = Pattern.compile("(.+@.+)");

	private static final Map<String, String> EMAIL_ALIASES = new HashMap<String, String>() {{
		put("stack@archive.org", "stack@duboce.net");
		put("saint.ack@gmail.com", "stack@duboce.net");

		put("ted.dunning@gmail.com", "tdunning@veoh.com");

		put("jason.hadoop@gmail.com", "jason.hadoop@gmail.com");

		put("oom@yahoo-inc.com", "omalley@apache.org");
		put("owen.omalley@gmail.com", "omalley@apache.org");
		put("owen@yahoo-inc.com", "omalley@apache.org");

		put("tom.e.white@gmail.com", "tom@cloudera.com");

		put("ak@cs.washington.edu", "aaron@cloudera.com");

		put("lohit_bv@yahoo.com", "lohit.vijayarenu@yahoo.com");

		put("acm@yahoo-inc.com", "arunc@yahoo-inc.com");

		put("alexloddengaard@gmail.com", "alex@cloudera.com");
		put("alexlod@google.com", "alex@cloudera.com");
	}};
	
	private static class EmailRater {
		private Pattern _pattern;
		private double _score;

		public EmailRater(String pattern, double score) {
			_pattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			_score = score;
		}

		public Pattern getPattern() {
			return _pattern;
		}

		public double getScore() {
			return _score;
		}
	}
	
	private static final EmailRater[] EMAIL_RATERS = {
		new EmailRater("worship the ground you walk on", 100.0),
		new EmailRater("owe you a beer", 50.0),
		new EmailRater("thank", 5.0)		
	};
	

	// These aren't easily serializable, so make them transient and do lazy
	// creation from the operate() call.
	private transient MboxParser _parser;
	private transient ContentHandler _handler;
	private transient StringBuilder _content;
	
	public ParseEmailFunction() {
		super(OUTPUT_FIELDS);
	}

	private synchronized void init() {
		if (_parser == null) {
			_parser = new MboxParser();
		}
		
		if (_content == null) {
			_content = new StringBuilder();
		}
		
		if (_handler == null) {
			_handler = new DefaultHandler() {
				private boolean inParagraph = false;
				private boolean inQuotes = false;

				@Override
				public void startDocument() throws SAXException {
					super.startDocument();

					inParagraph = false;
					inQuotes = false;
					_content.setLength(0);
				}

				@Override
				public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
					if (localName.equalsIgnoreCase("p")) {
						inParagraph = true;
					} else if (localName.equalsIgnoreCase("q")) {
						inQuotes = true;
					} else if (localName.equalsIgnoreCase("br")) {
						_content.append('\r');
					}
				}

				@Override
				public void endElement(String uri, String localName, String name) throws SAXException {
					if (localName.equalsIgnoreCase("p")) {
						inParagraph = false;
					} else if (localName.equalsIgnoreCase("q")) {
						inQuotes = false;
					}
				}

				@Override
				public void characters(char[] ch, int start, int length) throws SAXException {
					if (inParagraph && !inQuotes) {
						// We have text we want to process.
						_content.append(ch, start, length);
					}
				}
			};
		}
	}
	
	@Override
	public void operate(FlowProcess process, FunctionCall<NullContext> functionCall) {
		init();
		
		// On input we have a FetchedDatum that holds a single email.
        TupleEntry arguments = functionCall.getArguments();
        FetchedDatum fetchedDatum = new FetchedDatum(arguments.getTuple());
        
        // Now, if the FetchedDatum mime-type is application/mbox, we want to parse it and
        // output the results
        if (fetchedDatum.getContentType().equals("application/mbox")) {
        	Metadata metadata = new Metadata();
        	ParseContext context = new ParseContext();
        	InputStream is = new ByteArrayInputStream(fetchedDatum.getContentBytes());
        	
        	try {
        		_parser.parse(is, _handler, metadata, context);

        		// _content now has all of the body text, and metadata has the header info.
        		String messageId = metadata.get(Metadata.IDENTIFIER);
        		String emailAddress = metadata.get(Metadata.CREATOR);

        		if (emailAddress == null) {
        			LOGGER.warn("No email address for message: " + messageId);
        			return;
        		}

        		String address = null;
        		String name = null;

        		Matcher addressMatcher = FULL_EMAIL_ADDRESS_PATTERN.matcher(emailAddress);
        		if (addressMatcher.matches()) {
        			name = addressMatcher.group(1);
        			address = addressMatcher.group(2);
        		} else {
        			addressMatcher = SIMPLE_EMAIL_ADDRESS_PATTERN.matcher(emailAddress);
        			if (addressMatcher.matches()) {
        				address = addressMatcher.group(1);
        			} else {
        				LOGGER.warn("Email address has invalid format: " + emailAddress);
        				return;
        			}
        		}

        		// Now we might need to remain the address, if this user has aliases.
        		if (EMAIL_ALIASES.containsKey(address)) {
        			address = EMAIL_ALIASES.get(address);
        		}

        		Tuple tuple = new Tuple(messageId, address, name, 0.0);
        		functionCall.getOutputCollector().add(tuple);

        		String replyId = metadata.get(Metadata.RELATION);

        		if (replyId != null) {
        			double score = analyzeReply(_content.toString());
        			if (score > 0.0) {
        				tuple = new Tuple(replyId, null, null, score);
        				functionCall.getOutputCollector().add(tuple);
        			}
        		}
        	} catch (Exception e) {
				LOGGER.error("Exception parsing email message", e);
			}
        }
	}

	/**
	 * Calculate the "thanks" score for <msg>
	 * 
	 * @param msg - body of email message
	 * @return - score
	 */
	private double analyzeReply(String msg) {
		int endOfSearch = msg.length();
		Matcher endingThanksMatcher = THANKS_AT_END_PATTERN.matcher(msg);
		if (endingThanksMatcher.find()) {
			endOfSearch = endingThanksMatcher.start();
		}
		
		for (EmailRater rater : EMAIL_RATERS) {
			Matcher ratingMatch = rater.getPattern().matcher(msg);
			if (ratingMatch.find() && (ratingMatch.start() < endOfSearch)) {
				return rater.getScore();
			}
		}
		
		return 0.0;
	}
}
