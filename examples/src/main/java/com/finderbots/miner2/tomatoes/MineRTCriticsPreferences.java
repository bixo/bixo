/*
 * Copyright 2009-2012 Scale Unlimited
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
package com.finderbots.miner2.tomatoes;

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.parser.DOMParser;
import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;
import com.bixolabs.cascading.NullContext;
import com.finderbots.miner2.RegexUrlStringFilter;
import com.finderbots.miner2.SimpleBodyContentHandler;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("serial")
public class MineRTCriticsPreferences extends DOMParser {

    private static final Logger LOGGER = Logger.getLogger(MineRTCriticsPreferences.class);

    private static final Pattern IMG_SUFFIX_EXCLUSION_PATTERN =
        Pattern.compile("(?i)\\.(gif|jpg|jpeg|bmp|png|ico)$");

    private static final int MAX_WORDS_PER_PHRASE = 2;

    private transient RTPageDatum _result;
    private RegexUrlStringFilter _urlsToMineFilter;// if not null then url must match a pattern to include before being analyzed

    public MineRTCriticsPreferences(RegexUrlStringFilter urlsToMineFilter) {
        super(RTPageDatum.FIELDS);
        _urlsToMineFilter = urlsToMineFilter;
    }
    
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);

        _result = new RTPageDatum("", "", "", "", new MultiValuePreference[0]);
    }

    // This get each page. It will check to see if it is a page to mine, then extract data
    // depending on what type of page it is. /critic/ pages will be minded for preferences, /m/ pages
    // will get mined for url, itemId, and poster image URL
    // These are all stored in the datum and will be written to different files by another pipe.
    @Override
    protected void process(ParsedDatum datum, Document doc, TupleEntryCollector collector) throws Exception {
        LOGGER.debug(this.getClass().toString() + " Got datum for url: " + datum.getUrl());
        if (_urlsToMineFilter == null || !_urlsToMineFilter.isRemove(datum.getUrl().toString()) ){
            // currently mines all pages so the fields in the tuple/datum must
            // ALL be set every time. Either set for prefs OR media pages, not both
            // todo: split into two datum types and mine separately?
            SimpleBodyContentHandler bodyContentHandler = new SimpleBodyContentHandler();
            SAXWriter writer = new SAXWriter(bodyContentHandler);
            writer.write(doc);

            // Mine that data.
            String url = datum.getUrl();
            _result.setUrl(url);

            if(url.contains("/critic/")){// mining a critic page
                _result.setItemId("");
                _result.setItemName("");
                _result.setPosterImageUrl("");
                MultiValuePreference[] prefs = minePrefs(url, doc);
                _result.setPrefs(prefs);
            } else if (url.contains("/m/")){//mining a media page
                _result.setItemId(mineItemId(url));
                _result.setItemName(mineItemName(doc));
                _result.setPosterImageUrl(minePosterImageUrl(doc));
                _result.setPrefs(new MultiValuePreference[0]);
            } else {// not a page to mine, should be filtered out so throw an exception?
                //throw new Exception("Got a page that should not be mined: "+url);
                LOGGER.info("URLs to mine not working, getting urls that we don't mine like: "+url);
            }

            collector.add(_result.getTuple());
        }
    }

    @Override
    protected void handleException(ParsedDatum datum, Exception e, TupleEntryCollector collector) {
        // We'll just log it here, though normally we'd want to rethrow the exception, and
        // have our workflow set up to trap it.
        LOGGER.error("Exception parsing/processing " + datum.getUrl(), e);

    }

    // This is called on a rottentomatoes.com/m/ page where the url has the item id
    // rottentomatoes.com/m/the_good_the_bad_and_the_ugly/
    private String mineItemId(String url){
        String itemId;
        Pattern itemIdPattern = Pattern.compile("(.*com/m/)([^/]*)(/.*)");
        try {// first get the critic userId
            Matcher itemIdMatcher = itemIdPattern.matcher(url);
            if( itemIdMatcher.find() ){
                itemId = itemIdMatcher.group(2).trim();
            } else {
                throw new IllegalStateException("Can't find action ID on page: "+url);
            }
        } catch(IllegalStateException e){
            LOGGER.warn("bad media ID in the URL");
            throw e;
        }
        return itemId;
    }

    // This is called on a rottentomatoes.com/m/ page where the url has the item id
    // rottentomatoes.com/m/the_good_the_bad_and_the_ugly/ the id is in the url but
    // we have to get the full title out of the body
    private String mineItemName(Document doc){
        List<Node> titleNodes = getNodes(doc, "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"movie_title\", \" \" ))]//span");
        Node titleNode = titleNodes.get(0);
        if(titleNode == null ){
            LOGGER.warn("Can't locate media title in page: "+_result.getUrl());
            throw new IllegalStateException() ;
        }
        String title = titleNode.getText();
        if(title == null || title.isEmpty()){
            LOGGER.warn("Can't locate media title in page: "+_result.getUrl());
            throw new IllegalStateException() ;
        }
        return title;
    }


    // This is called on a rottentomatoes.com/m/ page where the url has the item id
    // rottentomatoes.com/m/the_good_the_bad_and_the_ugly/ the id is in the url but
    // we have to get the full title out of the body
    private String minePosterImageUrl(Document doc){
        List<Node> posterNodes = getNodes(doc, "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"trailer_play_action_button\", \" \" ))]//img");
        Node posterNode = posterNodes.get(0);
        if(posterNode == null ){
            LOGGER.warn("Can't locate media poster image in page: "+_result.getUrl());
            throw new IllegalStateException() ;
        }
        String posterURL = getAttributeFromNode(posterNode, "src");// get the src property of the img tag
        if(posterURL == null || posterURL.isEmpty()){
            LOGGER.warn("Can't locate media title in page: "+_result.getUrl());
            throw new IllegalStateException() ;
        }
        return posterURL;
    }

    private MultiValuePreference[] minePrefs(String url, Document doc) throws IllegalStateException {
        ArrayList<MultiValuePreference> prefList = new ArrayList<MultiValuePreference>();
        /* RT critic page will be mined for review actions, fresh or rotten, and user and item ids
         * all of which come from the rows of a table, except the critic user name from the
         * url
        <tr>
            <td>
                <span class="icon tiny fresh" title=""></span>
            </td>
            <td>
			    <span class="tMeterIcon tiny">
				<span title="Fresh" class="icon tiny fresh"></span><span class="tMeterScore">88%</span>
				</span>
            </td>
            <td><a target="_top" href="/m/unmarried_woman/" class="">An Unmarried Woman</a> (1978)</td>
            <td class="lastCol">"<a href="/click/author-16/reviews.php?rid=2150520&amp;cats=&amp;genreid=&amp;switches=&amp;letter=&amp;sortby=&amp;page=1">
                <strong>There are scenes in An Unmarried Woman so well written and acted that our laughter is unsettling, the laughter of exact recognition. </strong></a>"
                    —
<a href="/source-165/"><em>Film Comment Magazine</em></a>
                <div>Posted Jun 28, 2013</div>
            </td>
        </tr>
       */

        //todo: put the xpaths in a param file if xpaths keyed by url-regex is enough for a nice DSL
        List<Node> cellNodes = getNodes(doc, "//tr//td");//this should get all cells in the table
        //look in the first column for a class name = "rotten" or "fresh"
        //in the second is the tomato meter, the third has the item url, which contains an id for the media item

        Pattern actionPattern = Pattern.compile("(.*tiny[\\w]*)([^\"]*)(\" title=\".*)", Pattern.DOTALL);//looking through many lines
        //used on the url to get critic ID
        Pattern userIdPattern = Pattern.compile("(.*/critic/)(.*)(/.*)");
        //used on //tr nodes, there will be an <a> tag with /m/xxx/ where xxx is the movie ID
        //todo: may want to get href from particular <a> tag but should work for now
        Pattern itemIdPattern = Pattern.compile("(.*com/m/)([^/]*)(/.*)", Pattern.DOTALL);//looking through many lines

        String userId;
        try {// first get the critic userId
            Matcher userIdMatcher = userIdPattern.matcher(url);
            if( userIdMatcher.find() ){
                userId = userIdMatcher.group(2);
            } else {
                throw new IllegalStateException("Bad critic ID in the URL: "+url);
            }
            //userId = userIdMatcher.group(2);
        } catch(IllegalStateException e){
            LOGGER.warn("Bad critic ID in the URL: "+url);
            throw e;
        }
        for( int rowIndex = 0; rowIndex < cellNodes.size(); rowIndex += 4) {// now for each row grab the itemId and preference value
            //get the part between /critic/ and the next slash, that is the unique critic name in rotten tomatoes
            String criticReviewCellXML = cellNodes.get(rowIndex).asXML();
            String mediaDescriptionXML = cellNodes.get(rowIndex+2).asXML();
            try{

                Matcher actionMatcher = actionPattern.matcher(criticReviewCellXML);
                String actionId;
                if( actionMatcher.find() ){
                    actionId = actionMatcher.group(2).trim();
                } else {
                    LOGGER.info("Can't find action ID on page: "+url);
                    continue;
                }

                String itemId;
                Matcher itemIdMatcher = itemIdPattern.matcher(mediaDescriptionXML);
                if(itemIdMatcher.find()){
                    itemId = itemIdMatcher.group(2).trim();
                } else {
                    LOGGER.info("Can't find item ID on page: "+url);
                    continue;
                }


                MultiValuePreference pref = new MultiValuePreference(userId, actionId, itemId);

                prefList.add(pref);
            } catch (IllegalStateException e) {
                //just ignore a bad row
                LOGGER.warn("Exception during a regex matching the preference from a table row of the critic page. It will be ignored but beware something is wrong");
                throw e;
            }
        }

        return prefList.toArray(new MultiValuePreference[prefList.size()]);
    }

    private RTMediaURLResult[] extractImages(String sourceUrl, Document doc, Outlink[] outlinks) {
        ArrayList<RTMediaURLResult> pageResults = new ArrayList<RTMediaURLResult>();
        // Find if we have image links that may have extracted as an Outlink
        for (Outlink outlink : outlinks) {
            String outlinkUrl = outlink.getToUrl();
            if (isImgSuffix(outlinkUrl)) {
                // TODO Maybe set description to any words found in image name? Change '-' and '_' to spaces?
                RTMediaURLResult result = new RTMediaURLResult(sourceUrl, outlinkUrl, "");
                pageResults.add(result);

            }
        }
        // Next extract all img
        List<Node> imgNodes = getNodes(doc, "//img");
        for (Node node : imgNodes) {
            String src = getAttributeFromNode(node, "src");
            String alt = getAttributeFromNode(node, "alt");
            RTMediaURLResult result = new RTMediaURLResult(sourceUrl, src, alt);
            pageResults.add(result);
        }

        return pageResults.toArray(new RTMediaURLResult[pageResults.size()]);
    }

    private String getAttributeFromNode(Node node, String attribute) {
        String attributeValue = null;
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            Element e = (Element)node;
            attributeValue = e.attributeValue(attribute);
        }
        return  (attributeValue == null ? "" : attributeValue);
    }

    private String getTextFromNode(Node node) {
        String attributeValue = null;
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            Element e = (Element)node;
            attributeValue = e.getTextTrim();
        }
        return  (attributeValue == null ? "" : attributeValue);
    }

    /**
     * Utility routine to get back a list of nodes from the HTML page document,
     * which match the provided XPath expression.
     * 
     * @param xPath expression to match
     * @return array of matching nodes, or an empty array if nothing matches
     */
    @SuppressWarnings("unchecked")
    private List<Node> getNodes(Node node, String xPath) {
        List<Node> result = node.selectNodes(xPath);
        if (result == null) {
            result = new ArrayList<Node>();
        }
        
        return result;
    }
    
    private static boolean isImgSuffix(String url) {
        Matcher m = IMG_SUFFIX_EXCLUSION_PATTERN.matcher(url);
       if (m.find()) {
           return true;
       }
       return false;
    }

}

