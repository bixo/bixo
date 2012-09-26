package bixo.examples.crawl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/11/12
 * Time: 10:45 AM
 * To change this template use File | Settings | File Templates.
 */

abstract class ClusterConfiguration { //abstract class must be overriden for each cluster config
    private Path docFiles;
    private Path simFiles;
    private Path dictionaryFiles;
    private Path vectorFiles;
    private Path docIndexFiles;
    protected Path clusterFiles;
    private Path docToClusterMapFiles;
    private HashMap<String, String> MY_MAP;
    private Boolean removePatentsChildren;

    //force the extending class to create the hashmap of config options
    abstract HashMap<String, String> createMap();

    ClusterConfiguration(){
        MY_MAP = createMap();
        docFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("DOC_FILES"));
        simFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("ROW_SIMILARITY_MATRIX_FILES"));
        dictionaryFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("DICTIONARY_FILES"));
        vectorFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("NAMED_VECTOR_FILES"));
        docIndexFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("DOC_TO_ROW_INDEX_FILES"));
        clusterFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("CLUSTER_FILES"));
        docToClusterMapFiles = new Path(this.get("INPUT_ROOT_DIR"), this.get("DOC_TO_CLUSTER_MAP_FILES"));
        removePatentsChildren = true;
    }

    public String vectorNameToURL(String name){
        return name;//do nothing? may want to make abstract
    }

    protected String get(String key){
        return MY_MAP.get(key);
    }

    public Path getDocFiles() {
        return docFiles;
    }

    public Path getSimFiles() {
        return simFiles;
    }

    public Path getDictionaryFiles() {
        return dictionaryFiles;
    }

    public Path getVectorFiles() {
        return vectorFiles;
    }

    public Path getDocIndexFiles() {
        return docIndexFiles;
    }

    public Path getClusterFiles()   throws IOException  {
        return clusterFiles;
    }

    public Path getDocToClusterMapFiles() {
        return docToClusterMapFiles;
    }

    public void setRemovePatentsChildren(Boolean flag) {
        removePatentsChildren = flag;
    }

    public Boolean getRemovePatentsChildren() {
        return removePatentsChildren;
    }

    public String extractNameFromBody(String body){
        return body;
    }

    public String extractSnippetFromBody(String body){
        return body;
    }

    public String cleanupBody(String body) {
        return body;
    }

    public DBObject createRootOfClusterTree( DBCollection clusters ){
        String newJSONParent = "{'cluster_id' : 'root:new cluster', 'name' : 'new cluster', 'parent_ids' : [	], 'child_ids' : [	],	'term_weight_pairs' : [  ], 'clustered_doc_descriptors' : [	] }";
        DBObject parent = (DBObject) JSON.parse(newJSONParent);
        clusters.insert(parent);
        return clusters.findOne(new BasicDBObject("name", "new cluster"));
    }
}

class  ReutersClusterConfiguration extends ClusterConfiguration {

    HashMap<String, String> createMap() {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("INPUT_ROOT_DIR", "r");
        result.put("PARENT_CLUSTER_NAME", "reuters");
        result.put("DOC_FILES", "reuters-seqfiles/chunk-0");
        result.put("FINDERBOTS_PATH_TO_REUTERS_TXT_FILES", "http://finderbots.com/r"); // append to domain to get to reuters text files
        result.put("DOC_TO_ROW_INDEX_FILES", "reuters-matrix/docIndex");
        result.put("ROW_SIMILARITY_MATRIX_FILES", "reuters-similarity/part-r-00000");
        result.put("NAMED_VECTOR_FILES", "reuters-vectors/tfidf-vectors/part-r-00000");
        result.put("DICTIONARY_FILES", "reuters-vectors/dictionary.file-0");
        result.put("CLUSTER_FILES", "reuters-kmeans-clusters/clusters-3-final/part-r-00000");
        result.put("DOC_TO_CLUSTER_MAP_FILES", "reuters-kmeans-clusters/clusteredPoints/part-m-00000");
        return result;
    }

    public String vectorNameToURL(String name){
        Path textFile = new Path(name);
        name = textFile.getName();
        textFile = new Path(this.get("FINDERBOTS_PATH_TO_REUTERS_TXT_FILES"), name);
        return name = textFile.toString();
    }

    //todo should put this stuff in a doc filtering class probably
    public String extractNameFromBody(String body){
        //get the third line for name
        String newBody = body.split("\n",4)[2];
        return WordUtils.capitalize(newBody.toLowerCase());
    }

    public String extractSnippetFromBody(String body){
        //get the third line for name
        //remove the first three lines and return the first 200 chars of what is left.
        int length = body.length();
        int newLength = 200;//number of characters in the snippet
        if( length < newLength ){
            newLength = length;
        }
        return body.substring(0, newLength);
    }

    public String cleanupBody(String body) {
        //return rest of body after splitting out date, blank line, name, blank line
        String[] newBody = body.split("\n",5);
        if ( newBody.length > 4 ) {
            return newBody[4];
        } else {
            return "";
        }
    }
}

class  TextClusterConfiguration extends ClusterConfiguration {

    HashMap<String, String> createMap() {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("INPUT_ROOT_DIR", "b2");
        result.put("PARENT_CLUSTER_NAME", "belief");
        result.put("DOC_FILES", "bixo-seqfiles/chunk-0");
        result.put("FINDERBOTS_PATH_TO_BELIEF_TXT_FILES", "http://finderbots.com/belief"); // append to domain to get to text files to put in iframe--ugly I know...
        result.put("DOC_TO_ROW_INDEX_FILES", "bixo-matrix/docIndex");
        result.put("ROW_SIMILARITY_MATRIX_FILES", "bixo-similarity/part-r-00000");
        result.put("NAMED_VECTOR_FILES", "bixo-vectors/tfidf-vectors/part-r-00000");
        result.put("DICTIONARY_FILES", "bixo-vectors/dictionary.file-0");
        result.put("CLUSTER_FILES", "bixo-kmeans-clusters/clusters-2-final/part-r-00000");
        result.put("DOC_TO_CLUSTER_MAP_FILES", "bixo-kmeans-clusters/clusteredPoints/part-m-00000");
        return result;
    }

    public String vectorNameToURL(String name){
        Path textFile = new Path(name);
        name = textFile.getName();
        textFile = new Path(this.get("FINDERBOTS_PATH_TO_BELIEF_TXT_FILES"), name);
        return name = textFile.toString();
    }

    //todo should put this stuff in a doc filtering class probably
    public String extractNameFromBody(String body){
        //get the third line for name
        return "Belief";
    }

    public String extractSnippetFromBody(String body){
        return body;//these are small files
    }

    public String cleanupBody(String body) {
        return body;//do nothing
    }
}

class NutchClusterConfiguration extends ClusterConfiguration {
    String currentURL = new String();

    HashMap<String, String> createMap() {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("INPUT_ROOT_DIR", "n");
        result.put("PARENT_CLUSTER_NAME", "nutch");
        result.put("DOC_FILES", "nutch-seqfiles/chunk-0");
        result.put("DOC_TO_ROW_INDEX_FILES", "nutch-matrix/docIndex");
        result.put("ROW_SIMILARITY_MATRIX_FILES", "nutch-similarity/part-r-00000");
        result.put("NAMED_VECTOR_FILES", "nutch-vectors/tfidf-vectors/part-r-00000");
        result.put("DICTIONARY_FILES", "nutch-vectors/dictionary.file-0");
        result.put("CLUSTER_FILES", "nutch-kmeans-clusters/clusters-2-final/part-r-00000");
        result.put("DOC_TO_CLUSTER_MAP_FILES", "nutch-kmeans-clusters/clusteredPoints/part-m-00000");
        return result;
    }

    public String vectorNameToURL(String name){
        currentURL = name;
        return name;
    }

    //todo should put this stuff in a doc filtering class probably
    public String extractNameFromBody(String body){
        //todo: Hmm, we lost the page name from nutch? Must find... Looks like the page "title" tag is the first few words
        int length = body.length();
        int newLength = 20;//todo: number of characters to use for the title, is there a better way?
        if( length < newLength ){
            newLength = length;
        }
        return body.substring(0, newLength);
    }

    public String extractSnippetFromBody(String body){
        //todo: when using the nutch parsed_text dump there is a LOT of boilerplate in the text so the body and snippet are often
        //exactly the same for many pages on a site. Must fix this for finderbots, not all that important for the analysis
        int length = body.length();
        int newLength = 300;//number of characters in the snippet
        if( length < newLength ){
            newLength = length;
        }
        return body.substring(0, newLength);
    }

    public String cleanupBody(String body) {
        //return rest of body after splitting out date, blank line, name, blank line
        return body;
    }
}

class BixoClusterConfiguration extends ClusterConfiguration {
    String currentURL = new String();
    Configuration conf;
    int titleWeight = 3;//this assume the title is in triplicate at the start of the page parsed text
    //created by the ExportTool

    HashMap<String, String> createMap() {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("INPUT_ROOT_DIR", "b");
        result.put("PARENT_CLUSTER_NAME", "crawl");
        result.put("DOC_FILES", "seqfiles");
        result.put("DOC_TO_ROW_INDEX_FILES", "doc-matrix/docIndex");
        result.put("ROW_SIMILARITY_MATRIX_FILES", "similarity/part-r-00000");
        result.put("NAMED_VECTOR_FILES", "vectors/tfidf-vectors/part-r-00000");
        result.put("DICTIONARY_FILES", "vectors/dictionary.file-0");
        result.put("CLUSTER_FILES", "clusters");//place to find clusters
        result.put("DOC_TO_CLUSTER_MAP_FILES", "clusters/clusteredPoints/part-m-0");
        return result;
    }

    public int getTitleWeight(){
        return titleWeight;
    }

    public void setTitleWeight(int tw){
        titleWeight = tw;
    }

    BixoClusterConfiguration(Configuration conf){
        super();
        this.conf = conf;
    }

    public String vectorNameToURL(String name){
        currentURL = name;
        return name;
    }

    //todo should put this stuff in a doc filtering class probably
    public String extractNameFromBody(String body){
        //titleWeight number of lines at the beginning are the Title duplicated for TFIDF weighting, the rest is content
        int end = 0;
        int start = 0;
        for( int i = 1; i <= titleWeight; i++){
            start = end;
            end = body.indexOf('\n', end);//get end of Title line titleWeight times
            end++;
        }
        String title;
        if( end > start ){//all is well
            title = body.substring(start, end - 1);//remove the \n
        } else {//can't find Title so ...
            title = "No title";
        }
        return title;
    }

    public String extractSnippetFromBody(String body){
        //titleWeight number of lines at the beginning are the Title duplicated for TFIDF weighting, the rest is content
        int end = 0;
        int start = 0;
        for( int i = 1; i <= titleWeight; i++){
            start = end;
            end = body.indexOf('\n', end);//get end of Title line titleWeight times
            end++;
        }
        start = end + 1;//start past the \n
        String snippet = "No snippet";
        Integer length = body.length();
        Integer newLength = 300 + end;//number of characters in the snippet might drop one if .indexOf() returned -1
        if( start != 0 && end != -1  ){//make sure we skipped the title successfully
            if( length < newLength ){
                newLength = length;
            }
            snippet = body.substring(start, newLength);
        }
        return snippet;
    }

    public String cleanupBody(String body) {
        //return rest of body after splitting out date, blank line, name, blank line
        //titleWeight number of lines at the beginning are the Title duplicated for TFIDF weighting, the rest is content
        int end = 0;
        int start = 0;
        for( int i = 1; i <= titleWeight; i++){
            start = end;
            end = body.indexOf('\n', end);//get end of Title line titleWeight times
            end++;
        }
        start = end + 1;
        end = body.length()-1;
        if( start > end || start < 0 ){
            body = "";
        } else {
            body = body.substring(start, body.length()-1 );
        }
        return body;
    }

    public DBObject createRootOfClusterTree( DBCollection clusters ){
        String newJSONParent = "{'cluster_id' : 'root:crawl', 'name' : 'crawl', 'parent_ids' : [	], 'child_ids' : [	],	'term_weight_pairs' : [  ], 'clustered_doc_descriptors' : [	] }";
        DBObject parent = (DBObject) JSON.parse(newJSONParent);
        clusters.insert(parent);
        return clusters.findOne(new BasicDBObject("name", "crawl"));
    }

    public Path getClusterFiles() throws IOException {//points to a dir with another dir that has "final" in it's name
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] stats = fs.listStatus(clusterFiles);
        int fileNum = 0;
        ArrayList<Path> docFilesPaths = new ArrayList<Path>();
        Path finalClustersPath = null;
        for( FileStatus s : stats ){
            if(s.getPath().toString().contains("-final")){//clusters-n-final etc.
                finalClustersPath = s.getPath();
            } //otherwise its iteration clusters
        }
        docFilesPaths.clear();
        stats = fs.listStatus(finalClustersPath);
        for( FileStatus s : stats ){
            if(s.getPath().toString().contains("art-")){//part-r-00000 etc.
                return s.getPath();
            } //otherwise its _SUCCCESS or some such file
        }
        // better have returned or we hav an error
        throw new IOException();
    }


}