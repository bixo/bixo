package com.finderbots.utilities;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/4/12
 * Time: 11:55 AM
 * on a local instance run this from the "big-data" directory
 * on a hadoop cluster run from anywhere. it assumes paths relative
 * to /user/your-user-name/
 * so INPUT_ROOT_DIR/DOCS_FILES will be
 *    hdfs://user/pat/r/reuters-seqfiles/chunk-0
 * or file://.../r/reuters-seqfiles/chunk-0
 * todo: make this work with directories full of files, it now works by pointing to files update, works with directory in the case of input doc sequence files, easy peasy. Not sure if this is needed anywhere else?
 *
 * For the batch of docs the mahout file is a sequence
 * of key=docid, value=text pairs. This will write them into mongo as is, allowing Mongo to
 * create objectIds
 * The collections will be called DOC_COLLECTION_NAME each record will look like:
 *      { "_id" : ObjectId("4f661fa191717066b8000002"), "runId" : "/-tmp/reut2-000.sgm-0.txt",
 *      "body" : "26-FEB-1987 15:01:01.79  BAH..."}
 *
 */


public class ExportDocsMahout2Mongo  {

    private Mongo m;//where to put connection
    private DB db;//where to put current mongo db in use
    private DBCollection tmpIndex;//doc name to doc id dictionary, destroyed and created at each export
    private DBCollection tmpDict;//tokenID to token string dictionary, destroyed and created at each export
    private DBCollection docs;//all document stuff is accumulated here in the db it is named DOC_COLLECTION_NAME
    private DBCollection clusters; //clusters and docs they contain, named CLUSTER_COLLECTION_NAME
    private static final String DOC_COLLECTION_NAME = "amped_pages";
    private static final String CLUSTER_COLLECTION_NAME = "topics";
    private static final String DICTIONARY_COLLECTION_NAME = "tmp_dict";//name of tmpDict in mongo
    private static final String DOC_INDEX_COLLECTION_NAME = "tmp_doc_index";//name of tmpDict in mongo
    private ClusterConfiguration currentClusterConf;
    private static int lastNumberOfSimDocs = 0;


    private DBCollection exportDocIndex(Configuration conf, Path docIndex ) throws Exception {
        tmpIndex = db.getCollection( DOC_INDEX_COLLECTION_NAME );
        tmpIndex.drop();
        tmpIndex.ensureIndex(new BasicDBObject("key",1));//index the integer keys

        long count = 0;
        System.out.println("Exporting key-to-url temp table to Mongo"+'\n');
        try {
            SequenceFileIterator<IntWritable, Text> iterator = new SequenceFileIterator<IntWritable, Text>(docIndex, true, conf);
            while (iterator.hasNext()) {
                // export the docIndex to Mongo and create an index for integer keys
                Pair<IntWritable, Text> record = iterator.next();
                int key = record.getFirst().get();
                String url = record.getSecond().toString();

                BasicDBObject rec = new BasicDBObject("key", key);
                rec.put("url", currentClusterConf.vectorNameToURL(url));

                tmpIndex.insert(rec);
                count++;
            }
        } finally {
            //nothing here?
        }
        System.out.println("Number of processed docs: " + String.valueOf(count) + '\n');
        return tmpIndex;
    }

    private DBCollection exportDict(Configuration conf, Path dictionaryFiles ) throws Exception {
        tmpDict = db.getCollection(DICTIONARY_COLLECTION_NAME);
        tmpDict.drop();
        tmpDict.ensureIndex(new BasicDBObject("key",1));//index the integer keys

        long count = 0;
        System.out.println("Exporting key-to-token temp table to Mongo"+'\n');
        try {
            SequenceFileIterator<Text, IntWritable> iterator = new SequenceFileIterator<Text,IntWritable>(dictionaryFiles, true, conf);
            while (iterator.hasNext()) {
                // export the tmpDict to Mongo and create an index for integer keys
                Pair<Text, IntWritable> record = iterator.next();
                int key = record.getSecond().get();
                String token = record.getFirst().toString();

                BasicDBObject rec = new BasicDBObject("key", key);
                rec.put("token", token );

                tmpDict.insert(rec);
                count++;
            }
        } finally {
            //nothing here?
        }
        System.out.println("Number of processed token: " + String.valueOf(count) + '\n');
        return tmpDict;
    }




    private String getNameFromID(int docId, DBCollection docIndex){
        DBObject record = docIndex.findOne(new BasicDBObject("key", docId));
        return record.get("url").toString() ;
    }

    private int exportSimilarDocs(Configuration conf, Path simFiles, Path docIndex,  DBCollection docs) throws Exception {
        // for each doc in the similarity matrix  add the similar docs to the docs record
        // use the key stored in the docIndex to access the doc to store similar docs with

        try {
            // export the docIndex to Mongo and create an temp index for integer keys -> urls
            tmpIndex =  exportDocIndex(conf, docIndex);
            // for each record in the similarity matrix store its similar docs with the keyed doc
            // the simFiles will be of the form:
            // Key: 6: Value: /-tmp/reut2-000.sgm-103.txt:{7485:0.3034959858247971,8364:0.33986303372405957,10137:0.3034959858247971,10959:0.24222222380915243,11150:0.32380875508814805,13547:0.22931431366917934,15902:0.32380875508814805,16566:0.25558835351247133,16728:0.2239231191777753,18303:0.5136590715900357}
            // Key int is not needed, Value: name of doc, vector of pairs of docids, distances
            SequenceFileIterator<IntWritable, VectorWritable> iterator = new SequenceFileIterator<IntWritable, VectorWritable>(simFiles, true, conf);
            System.out.println("Path to similarity matrix: " + simFiles.toString() + '\n');
            System.out.println("Key class: " + iterator.getKeyClass().toString() + '\n');
            System.out.println("Value Class: " + iterator.getValueClass().toString() + '\n');
            long count = 0;

            while (iterator.hasNext()) {
                Pair<IntWritable, VectorWritable> record = iterator.next();
                int key = record.getFirst().get();// rowId = docId of seed doc, attach the simDocs to this doc
                Vector value = (Vector)record.getSecond().get();
                //String url = currentClusterConf.vectorNameToURL(vector.getName());
                //Vector value = vector.getDelegate();
                Iterator<Vector.Element> vi = value.iterateNonZero();//a very sparse vector so don't waist time on empty elements
                int vcount = 0;
                int docId = 0;
                ArrayList simDocs = new ArrayList();
                while (vi.hasNext() ){
                    Vector.Element ve = vi.next();
                    docId = ve.index();
                    double distance = ve.get();
                    //now look up the docId in the docIndex and put that in mongo with the
                    //distance
                    // store on element in docs indexed by name
                    // "similar_pages" : [ 	{ 	"_id" : ObjectId("4f7201d79171701581000007"), 	"url" : "http://occamsmachete.com/ml/2011/12/29/data-sources/", 	"name" : "Data Sources", 	"distance" : 0.7 },... ]
                    simDocs.add( getNameFromID(docId, tmpIndex) );
                    simDocs.add( distance );
                    vcount++;
                }//todo: don't insert the identical doc, i.e. with the same url actually the -ess param to rowsimilarity should do that???
                //get the doc referenced by name
                String url = getNameFromID(key, tmpIndex);
                DBObject doc = docs.findOne( new BasicDBObject("url", url) );
                if(doc != null){
                    doc.put("similar_pages", simDocs);
                    docs.findAndModify(new BasicDBObject("url", url),
                            new BasicDBObject(),
                            new BasicDBObject("sort", 1),
                            false,
                            doc,
                            false,
                            true);
                    if( vcount != lastNumberOfSimDocs ){
                        lastNumberOfSimDocs = vcount;
                    }
                } else {
                    System.out.println("Doc not in index for url: "+url+'\n');
                }
                count++;
            }
            System.out.println("Number of seed docs processed (with which similar docs are associated): " + String.valueOf(count) + '\n');
        } finally {
            //nothing here?
        }

        return 0;
    }

    private int exportTermVectors(Configuration conf, Path dictionaryFiles, Path vectorFiles,  DBCollection docs) throws Exception {
        // for each doc add the tokens and weights for term vectors
        // put the dictionary in a tempDict collection to use for looking up tokens for vector weights

        try {
            // export the tmpDict to Mongo and create an temp index for integer keys -> tokens
            exportDict (conf, dictionaryFiles);

            // for each named vector create a token->weight vector and store it in the doc
            SequenceFileIterator<Text, VectorWritable> iterator = new SequenceFileIterator<Text, VectorWritable>(vectorFiles, true, conf);
            System.out.println("Path to Named Vectors (make sure they are named): " + vectorFiles.toString() + '\n');
            System.out.println("Key class: " + iterator.getKeyClass().toString() + '\n');
            System.out.println("Value Class: " + iterator.getValueClass().toString() + '\n');
            long count = 0;

            while (iterator.hasNext()) { //for each vector, construct token->weight array and insert in the doc named in vector
                Pair<Text, VectorWritable> record = iterator.next();
                String url = currentClusterConf.vectorNameToURL(record.getFirst().toString());
                Vector vector = record.getSecond().get();
                ArrayList termVector = getTermVector(vector, tmpDict);
                //get the doc referenced by name
                DBObject doc = docs.findOne( new BasicDBObject("url", url) );
                if( doc != null ){
                    doc.put( "term_weight_pairs", termVector );
                    docs.findAndModify(new BasicDBObject("url", url),
                            new BasicDBObject(),
                            new BasicDBObject("sort", 1),
                            false,
                            doc,
                            false,
                            true);
                    count++;
                } else {//null doc?????
                    System.out.println("doc not found in dictionary: " + url + '\n');

                }
            }
            System.out.println("Number of docs for which term vectors are created: " + String.valueOf(count) + '\n');
        } finally {
            //nothing here?
        }

        return 0;
    }

    public static ArrayList getTermVector( Vector v, DBCollection dictionary ){
        ArrayList termVector = new ArrayList();
        Iterator<Vector.Element> vi = v.iterateNonZero();//a very sparse vector so don't waist time on empty elements
        while (vi.hasNext() ){
            Vector.Element ve = vi.next();
            int tokenId = ve.index();
            double weight = ve.get();
            DBObject tokenRecord = dictionary.findOne(new BasicDBObject("key", tokenId));
            if( tokenRecord != null ){
                termVector.add( tokenRecord.get("token"));
                termVector.add( weight );
            } else {
                System.out.println("Warning: Vector has token not found in dictionary" + '\n');

            }
        }
        return termVector;
    }

    private int exportDocs(Configuration conf, Path docFiles, DBCollection docs) throws Exception {
        // the internal mahout generated docIds are only unique
        // for a given run so use the named vector (-nv) option to create file paths for keys
        // then the code below will make the paths unique over different runs so they can be used as
        // external keys and will be used to overwrite data with each export

        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] stats = fs.listStatus(docFiles);
            int fileNum = 0;
            ArrayList<Path> docFilesPaths = new ArrayList<Path>();
            for( FileStatus s : stats ){
                if(
                   (s.getPath().toString().contains("art-")) ||
                   (s.getPath().toString().contains("chunk"))){//part-00000 etc.
                    docFilesPaths.add(s.getPath());
                } //otherwise its _SUCCCESS or some such file
            }
            //Path[] docFilesList = new Path[docFilesPaths.size()];
            Path[] docFilesList = docFilesPaths.toArray(new Path[docFilesPaths.size()]);

            SequenceFileDirIterator<?, ?> iterator = new SequenceFileDirIterator<Writable, Writable>(docFilesList, true, conf);
            System.out.println("Path to input docs: " + docFiles.toString() + '\n');
            System.out.println("Key class: Text\n");
            System.out.println("Value Class: Text\n");
            long count = 0;

            // make sure url is indexed and unique

            docs.ensureIndex(new BasicDBObject("url", 1));//create index if not there
            BasicDBObject sort = new BasicDBObject("sort", 1);
            BasicDBObject fields = new BasicDBObject("fields", 0);// todo: is this correct? seems to work below

            while (iterator.hasNext()) {
                Pair<?, ?> record = iterator.next();
                String key = record.getFirst().toString();
                String str = record.getSecond().toString();

                BasicDBObject doc = new BasicDBObject();
                doc.put("url", currentClusterConf.vectorNameToURL(key));
                String newBody = currentClusterConf.cleanupBody(str);
                doc.put("name", currentClusterConf.extractNameFromBody(str));
                doc.put("body", newBody );
                doc.put("snippet", currentClusterConf.extractSnippetFromBody(str));
                ArrayList tmpArray = new ArrayList();
                doc.put("topic_ids", tmpArray);//add an empty array of Topics it belongs to

                BasicDBObject query = new BasicDBObject("url", currentClusterConf.vectorNameToURL(key));

                // this will find and modify or insert if not present
                docs.findAndModify(query, fields, sort, false, doc, false, true);

                count++;
            }
            System.out.println("Number of docs exported: " + String.valueOf(count) + '\n');
        } finally {
            //nothing here?
        }
        return 0;
    }

    ObjectId createNewClusterTree( String grandParentName, String parentName ){
        BasicDBObject grandParentQuery = new BasicDBObject("cluster_id", grandParentName);
        DBObject grandParent = clusters.findOne(grandParentQuery);
        BasicDBObject parentQuery = new BasicDBObject("cluster_id", parentName );
        DBObject parent = clusters.findOne(parentQuery);
        if( grandParent == null ){
            // create a grandparent
            String newJSONGrandParent = "{'cluster_id' : 'root', 'name' : 'root', 'parent_ids' : [	], 'child_ids' : [	],	'term_weight_pairs' : [  ], 'clustered_doc_descriptors' : [	] }";
            grandParent = (DBObject)JSON.parse(newJSONGrandParent);
            clusters.insert(grandParent);

        }
        ObjectId gpId = (ObjectId)clusters.findOne(grandParentQuery).get("_id");
        ObjectId pId;

        if(parent != null){//parent already exists so empty the child_ids array
            ((BasicDBList)parent.get("child_ids")).clear();
            pId = (ObjectId)parent.get("_id");
            clusters.findAndModify( new BasicDBObject("_id", pId), new BasicDBObject(), new BasicDBObject(),false, parent, false, true );
        } else {
            //create a new parent cluster and insert it into the root
            parent = currentClusterConf.createRootOfClusterTree(clusters);
        }
        pId = (ObjectId)parent.get("_id");

        Boolean parentAlreadyInGrandparent = false;
        BasicDBList parentSibs = (BasicDBList) grandParent.get("child_ids");
        if(parentSibs != null && !parentSibs.isEmpty()){
            // has children so get the one named "parentName"
            Iterator parentSibsIterator = parentSibs.iterator();
            while(parentSibsIterator.hasNext()){
                ObjectId parentSibId = (ObjectId)parentSibsIterator.next();
                if(parentSibId == pId){// have a gp and the parent is already in the list of children
                    parentAlreadyInGrandparent = true;
                    break;//stop looking but have to empty the parents children.
                }
            }
        }
        if(!parentAlreadyInGrandparent){// add it
            parentSibs.add(pId);
            clusters.findAndModify( new BasicDBObject("_id", gpId), new BasicDBObject(), new BasicDBObject(),false, grandParent, false, true );
        }
        return pId;
    }

    private void exportClusters(Configuration conf) throws Exception {
        try{
            //for each cluster create a name, write out the centroid, add all docs to the cluster and add the cluster to each doc
            ObjectId parentId = createNewClusterTree("root", "root:crawl");
            TopicMap tm = new TopicMap(conf, currentClusterConf, clusters, docs, tmpDict, parentId, db);
            tm.readClustersFromMahout();
            tm.writeClustersToMongo();
            //now add the centroid vector to the clusters collection.
        } finally {
            //nothing??
        }
    }

    public int run(String[] args) throws Exception {


        // create path to the input sequence file
        Configuration conf = new Configuration();

        // Set up connection to MongoDB destination for export
        m = new Mongo( "127.0.0.1" , 27017 );//for some reason localhost does not connect on laptop
        db = m.getDB("finder_bots_com_development");//careful this is the name of live finderbots db
        docs = db.getCollection(DOC_COLLECTION_NAME);//where all the doc related data will accumulate
        clusters = db.getCollection( CLUSTER_COLLECTION_NAME );//where all the doc related data will accumulate
        tmpDict = db.getCollection(DICTIONARY_COLLECTION_NAME);//a table mapping term id to term string
        //currentClusterConf = new BixoClusterConfiguration(conf);
        currentClusterConf = new BixoClusterConfiguration(conf);

        try {
            // export the document specific stuff, as seen on amped_pages in FinderBots.com
            exportDocs( conf,  currentClusterConf.getDocFiles(),  docs );
            exportSimilarDocs(conf,  currentClusterConf.getSimFiles(),  currentClusterConf.getDocIndexFiles(), docs);
            exportTermVectors(conf,  currentClusterConf.getDictionaryFiles(),  currentClusterConf.getVectorFiles(), docs);

            // export the cluster specific stuff seen on topic pages in FinderBots.com
            currentClusterConf.setRemovePatentsChildren(true);
            exportClusters(conf);
        } finally {
            //nothing here?
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        new ExportDocsMahout2Mongo().run(args);
    }

    /* internal classes */
}