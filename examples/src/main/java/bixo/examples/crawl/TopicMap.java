package bixo.examples.crawl;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.NamedVector;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/4/12
 * Time: 11:55 AM
 * Creates a structure to mimic the Topic in finderbots, then will write it to Mongo
 * { "_id" : ObjectId("4f7c8d7466775a1ba30c8916"), "name" : "reuters: topic #15", "clusterID" : "reuters:21477", "parent" : ObjectId("4f7c88723ea0e7fad212194e"), "amped_pages" : [ 	{ 	"url" : "http://finderbots.com/r/reut2-000.sgm-103.txt","weight" : 1, 	"distance" : 0.9698336270321658 }, 	{ 	"url" : "http://finderbots.com/r/reut2-000.sgm-107.txt","weight" : 1, 	"distance" : 0.988130577490163 }, 	{ 	"url" : "http://finderbots.com/r/reut2-000.sgm-110.txt","weight" : 1, 	"distance" : 0.9771974485436884 } ] }
 *
 */
class TopicMap  {
    Configuration conf;
    DBCollection clusters;
    DBCollection docs;
    DBCollection dictionary;
    DB db;
    ClusterConfiguration clusterConf;
    ObjectId parentId;

    static final int NUMBER_OF_CLUSTER_DOCS_SAVED = 30;//save no more than 30 of the closest per cluster



    class Topic { //pretty closely corresponds to the mongo doc structure and what ruby expects
        private String name;
        //protected String cluster_id; //used as the key for topic hashmap so not needed here
        private ArrayList<ObjectId> parent_ids;
        private ArrayList<ObjectId> child_ids;
        private ArrayList<HashMap<String, Object>> topClusteredDocs;
        private ArrayList centroidTermWeights; //list of pairs, term, weight
        private ObjectId oId = null;
        private org.apache.mahout.math.Vector centroid;

        public Topic(){
            parent_ids = new ArrayList<ObjectId>();
            child_ids = new ArrayList<ObjectId>();
            topClusteredDocs = new ArrayList<HashMap<String, Object>>();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public org.apache.mahout.math.Vector getCentroid() {
            return centroid;
        }

        public void setCentroid(org.apache.mahout.math.Vector centroid) {
            this.centroid = centroid;
        }

        public ObjectId getParent() {// todo assumes array of only one element, one parent
            if(this.parent_ids.isEmpty()){
                String state = "fail";
            }
            return this.parent_ids.isEmpty() ? null : this.parent_ids.get(0);
        }

        public void setParent(ObjectId parent) {
            if(parent_ids.isEmpty()){
                this.parent_ids.add(parent);
            } else {
                this.parent_ids.set(0, parent);
            }
        }
        public ArrayList getParents() {
            return parent_ids;
        }

        public void setParents(ArrayList parents) {
            this.parent_ids = parents;
        }

        public ArrayList getChildren() {
            return child_ids;
        }

        public void setChildren(ArrayList children) {
            this.child_ids = children;
        }

        public ArrayList<HashMap<String, Object>> getTopClusteredDocs() {
            return topClusteredDocs;
        }

        public void setTopClusteredDocs(ArrayList<HashMap<String, Object>> topClusteredDocs) {
            this.topClusteredDocs = topClusteredDocs;
        }

        public ArrayList getCentroidTermWeights() {
            return centroidTermWeights;
        }

        public void setCentroidTermWeights(ArrayList centroidTermWeights) {
            this.centroidTermWeights = centroidTermWeights;
        }

        public void setObjectId(ObjectId oId){
            this.oId = oId;
        }

        public ObjectId getObjectId(){
            return this.oId;
        }
    }

    HashMap<String, Topic> topics;//the key = the cluster_id

    //this data mimics the cluster mongo 'doc'/record with one entry per cluster
    //quads are url, weight, distance, ObjectId of the page in mongo

    TopicMap(Configuration conf, ClusterConfiguration clusterConf, DBCollection clusters, DBCollection docs, DBCollection dictionary, ObjectId parentId, DB db){
        this.conf = conf;
        this.clusterConf = clusterConf;
        this.dictionary = dictionary;
        this.clusters = clusters;
        this.docs = docs;
        this.db = db;
        this.parentId = parentId;
    }

    private void init() throws Exception {
        try {
            topics = new HashMap<String, Topic>();
            clusters.ensureIndex("cluster_id");
            clusters.ensureIndex("name");
            clusters.ensureIndex("parent_ids.0");// does this work? it should make the remove faster
            docs.ensureIndex("url");
            BasicDBObject parentQuery = new BasicDBObject("_id", parentId);
            DBObject parentCluster = clusters.findOne(parentQuery);

            if(clusterConf.getRemovePatentsChildren()){
                //remove all clusters with the same parent, could be 'root'
                //this assumes the parent cluster exists! or will throw an exception.
                BasicDBObject removeQuery = new BasicDBObject("parent_ids.0", parentId);//get all clusters that have parent_ids[0] = parentId
                clusters.remove(removeQuery);//remove all clusters whose first parent_id is the new cluster parent
                // since they are now obsolete.  Note: todo the parent_ids array will have only one entry for now, it will change semantics if this assumption changes
                //now remove the clusters associated with the parent so the parent will start with a blank child_id array
                ArrayList blankChildren = new ArrayList();//empty child list
                parentCluster.put("child_ids", blankChildren);
                clusters.findAndModify(parentQuery, new BasicDBObject(), new BasicDBObject("sort", 1),  false, parentCluster, true, true);
            }
        } finally {
            //nothing?
        }
    }

    public void writeClustersToDocs() throws Exception {
        try {
            SequenceFileIterator<IntWritable, WeightedVectorWritable> iterator = new SequenceFileIterator<IntWritable, WeightedVectorWritable>(clusterConf.getDocToClusterMapFiles(), true, conf);
            int numClusteredDocs = 0;

            while (iterator.hasNext()) {// for each clustered doc
                Pair<IntWritable, WeightedVectorWritable> record = iterator.next();
                // first store the cluster
                // store the cluster id, inclusion amount, distance, vector name, ignore the vector value since it's used elsewhere.
                // as you iterate, add the values to the cluster id record
                numClusteredDocs++;
                //get the cluster_id for key to HashMap
                String clusterId = String.valueOf(record.getFirst().get());
                //calculate the distance of the clustered doc to the cluster centroid
                // TODO: this is pretty heavy weight, there used to be a distance stored with the doc vector
                CosineDistanceMeasure dm = new CosineDistanceMeasure();
                WeightedVectorWritable clusteredDoc = record.getSecond();
                double dist = dm.distance(topics.get(clusterId).getCentroid(), clusteredDoc.getVector());
                if(!Double.isNaN(dist) && !Double.isInfinite(dist)){
                    String rawName = ((NamedVector)( clusteredDoc.getVector())).getName();
                    String url = clusterConf.vectorNameToURL(rawName);
                    DBObject cd = docs.findOne(new BasicDBObject("url", url));
                    if( cd != null ){
                        Topic curTop = topics.get(clusterId);
                        ObjectId topId = curTop.getObjectId();
                        BasicDBList curClusterList = new BasicDBList();
                        //((BasicDBList)clusteredDoc.get("cluster_ids")).add(topics.get(clusterId).getObjectId());
                        curClusterList.add(topId);
                        cd.put("topic_ids", curClusterList );
                        docs.findAndModify(new BasicDBObject("url", url), new BasicDBObject(), new BasicDBObject(), false, cd, false, true);
                    } else {//can't find the doc
                        System.out.println("Unable to find doc with url: " + url + '\n');

                    }
                }
            }

            System.out.println("Number of docs with cluster_ids assigned: " + String.valueOf(numClusteredDocs) + '\n');

        } finally {
            //nothing?
        }
    }

    public void readClustersFromMahout() throws Exception {
        try {
            init();

            this.getClusterCentroids();
            SequenceFileIterator<IntWritable, WeightedVectorWritable> iterator = new SequenceFileIterator<IntWritable, WeightedVectorWritable>(clusterConf.getDocToClusterMapFiles(), true, conf);
            int numClusteredDocs = 0;

            while (iterator.hasNext()) {// for each clustered doc
                Pair<IntWritable, WeightedVectorWritable> record = iterator.next();
                // first store the cluster
                // store the cluster id, inclusion amount, distance, vector name, ignore the vector value since it's used elsewhere.
                // as you iterate, add the values to the cluster id record
                numClusteredDocs++;
                //get the cluster_id for key to HashMap
                String clusterId = String.valueOf(record.getFirst().get());
                WeightedVectorWritable clusteredDoc = record.getSecond();
                /* double pdf = clusteredDoc.getWeight();
                double dist;
                //pdf = 1/(1+distance) and as of the Jeff Eastman patch the "distance" property is really the pdf
                //so calulate the distance from it thus dist = (1/pdf)-1
                if( pdf != 0 ) {
                    dist = (1.0/pdf)-1;
                } else {//if the pdf = 0 there is no likelihood the vector is part of the cluster
                    dist = 1;//as far away as you can get
                }
                */

                //calculate the distance of the clustered doc to the cluster centroid
                // TODO: this is pretty heavy weight, there used to be a distance stored with the doc vector
                CosineDistanceMeasure dm = new CosineDistanceMeasure();
                double dist = dm.distance(topics.get(clusterId).getCentroid(), clusteredDoc.getVector());

                if(!Double.isNaN(dist) && !Double.isInfinite(dist)){
                    String rawName = ((NamedVector)( clusteredDoc.getVector())).getName();
                    if( topics.get(clusterId) == null ){
                        topics.put(clusterId, new Topic());
                    }
                    topics.get(clusterId).setName(clusterConf.get("PARENT_CLUSTER_NAME")+": topic #"+Integer.toString(numClusteredDocs));
                    topics.get(clusterId).setParent(parentId);//parentId points to parent of all these clusters
                    HashMap<String, Object> currentDoc = new HashMap<String, Object>() ;//array of quads for each doc, only will contain the ones closest to the cluster center.
                    currentDoc.put("url", clusterConf.vectorNameToURL(rawName));
                    currentDoc.put("weight", clusteredDoc.getWeight());
                    currentDoc.put("distance", dist);
                    // store the doc id when storing this data to mongo
                    ArrayList topicDocs = topics.get(clusterId).getTopClusteredDocs();
                    if( topicDocs == null ){ //new array
                        topicDocs = new ArrayList();
                        topics.get(clusterId).setTopClusteredDocs(topicDocs);
                    }
                    topicDocs.add(currentDoc);
                    //now drop the largest since large numbers mean further away
                    if( topicDocs.size() > NUMBER_OF_CLUSTER_DOCS_SAVED ){
                        ListIterator it = topicDocs.listIterator();
                        HashMap<String, Object> largest;
                        if( it.hasNext()){
                            largest = (HashMap)it.next();
                            int i = 0;
                            while( it.hasNext() ){//keep updating the list with smallest distance
                                HashMap current = (HashMap)it.next();
                                double largestDist = Double.valueOf(largest.get("distance").toString());
                                double curDist = Double.valueOf(current.get("distance").toString());
                                if(largestDist < curDist ){
                                    largest = current;
                                }
                            }
                            topicDocs.remove(largest);
                        }
                    }
                    topics.get(clusterId).setTopClusteredDocs(topicDocs);

                }
                //get the cluster centroid
            }
            System.out.println("Number of docs assigned to clusters: " + String.valueOf(numClusteredDocs) + '\n');

            //this.removeDanglingDocs();
            this.sortDocsByDistance();
            Boolean stopHere = true;
        } catch (Exception e){
            throw e;
        }
    }
/*
* Key class: class org.apache.hadoop.io.Text Value Class: class org.apache.mahout.clustering.kmeans.Cluster
Key: VL-21536: Value: VL-21536{n=222 c=[0:0.004, 23:0.002,
* */
    public void getClusterCentroids() throws Exception {
        SequenceFileIterator<IntWritable, ClusterWritable> iterator = new SequenceFileIterator<IntWritable, ClusterWritable>(clusterConf.getClusterFiles(), true, conf);
        int clusterNumber = 0;
        while (iterator.hasNext()) {// for each clustered doc
            Pair<IntWritable, ClusterWritable> record = iterator.next();
            // the cursed Text here is "VL-blah" with blah being the cluster id string, why they appended a VL- is anyone's guess
            String clusterId = Integer.toString(record.getSecond().getValue().getId());
            if( topics.get(clusterId) == null ){
                topics.put(clusterId, new Topic());
                //we have a matching cluster now generate the term vector, sort, grab the top part of the vector and store it
                org.apache.mahout.math.Vector centroid = record.getSecond().getValue().getCenter();
                topics.get(clusterId).setCentroid(centroid);
                // sort it
                //System.out.println("Creating centroid terms and weight for cluster #" + Integer.toString(++clusterNumber) + '\n');
                ArrayList termVector = ExportDocsMahout2Mongo.getTermVector(centroid, dictionary);
                topics.get(clusterId).setCentroidTermWeights(termVector);
            }
        }
    }

    public void sortDocsByDistance() {
        // each cluster has a doc collection of the nearest clustered docs, sort them from closest to furthest
        // for cosine this is the smallest number is better so 0 is identical so smaller is better
        Iterator topicsDocsIterator = topics.entrySet().iterator();
        while( topicsDocsIterator.hasNext() ){
            //all we care about here is the topicDocs array
            Map.Entry pairs = (Map.Entry)topicsDocsIterator.next();
            ArrayList current = ((Topic)pairs.getValue()).getTopClusteredDocs();
            // check each entry to make sure there is a doc in the db or remove the docs from the topClusteredDocs since it's useless

            Collections.sort(current, new DistanceComparator());

        }
    }

    public class DistanceComparator implements Comparator<HashMap> {
        @Override
        public int compare(HashMap o1, HashMap o2) {//smaller to larger for cosine distance, 0=identical, 1 = far away
            return Double.valueOf(o1.get("distance").toString()).compareTo(Double.valueOf(o2.get("distance").toString()));
        }
    }

    public void removeDanglingDocs() {
        // check each entry to make sure there is a doc in the db or remove the docs from the topClusteredDocs since it's useless
        Iterator topicsDocsIterator = topics.entrySet().iterator();
        while( topicsDocsIterator.hasNext() ){
            //all we care about here is the topicDocs array
            Map.Entry pairs = (Map.Entry)topicsDocsIterator.next();
            ArrayList currentClusteredDocs = ((Topic)pairs.getValue()).getTopClusteredDocs();
            //Iterator docIterator = currentClusteredDocs.iterator();

            //while(docIterator.hasNext()){
            for(int i = 0; i<currentClusteredDocs.size(); i++){// don't use and iterator because even single threaded you'll
                //get a ConcurrentModificationException

                HashMap currentClusteredDoc = (HashMap)currentClusteredDocs.get(i);
                DBObject ampedDoc = docs.findOne( new BasicDBObject("url", currentClusteredDoc.get("url")));
                if( ampedDoc == null ){
                    currentClusteredDocs.remove(currentClusteredDoc);
                    //docIterator.remove(); // to avoid ConcurrentModificationException??? doesn't work though
                }
            }
        }
    }


    public DBCollection writeClustersToMongo() throws Exception {
        //for each cluster in the list
        //put the AmpedPage id in the topicDocs structure
        //write the cluster description
        int numClusters = 0;
        Iterator topicsIterator = topics.entrySet().iterator();
        while (topicsIterator.hasNext()) {
            //all we care about here is the topicDocs array
            Map.Entry pairs = (Map.Entry) topicsIterator.next();
            BasicDBObject dbTopic = new BasicDBObject();
            BasicDBObject topicQuery = new BasicDBObject();
            Topic currentTopic = (Topic) pairs.getValue();
            if (currentTopic.getParent() != null) { //got read in properly, if not it's a malformed cluster
                // look at the read code, may not have any docs or distance to doc is NaN or some such
                dbTopic.put("cluster_id", (String) pairs.getKey());
                topicQuery.put("cluster_id", (String) pairs.getKey());
                dbTopic.put("name", currentTopic.getName());
                BasicDBList currentParents = new BasicDBList();
                currentParents.add(currentTopic.getParent());
                dbTopic.put("parent_ids", currentParents);//does this work?
                ArrayList currentClusteredDocs = ((Topic) pairs.getValue()).getTopClusteredDocs();
                // insert the topic to get its id created
                DBObject insertedCluster = clusters.findAndModify(topicQuery, new BasicDBObject(), new BasicDBObject("sort", 1), false, dbTopic, true, true);
                //save the ObjectId
                currentTopic.setObjectId((ObjectId)insertedCluster.get("_id"));
                // add the topic id to its parent's child_ids
                BasicDBObject parentQuery = new BasicDBObject("_id", currentTopic.getParent());
                DBObject parentOfInsertedCluster = clusters.findOne(parentQuery);
                ArrayList parentsChildren = (ArrayList) parentOfInsertedCluster.get("child_ids");
                if (parentsChildren != null) {
                    if (!parentsChildren.contains(insertedCluster.get("_id"))) {
                        parentsChildren.add(insertedCluster.get("_id"));
                    }
                }
                // now write the parent with added child
                clusters.findAndModify(parentQuery, new BasicDBObject(), new BasicDBObject("sort", 1), false, parentOfInsertedCluster, true, true);

                for (int i = 0; i < currentClusteredDocs.size(); i++) {// don't use and iterator because even single threaded you'll
                    HashMap currentClusteredDoc = (HashMap) currentClusteredDocs.get(i);
                    DBObject ampedPage = docs.findOne(new BasicDBObject("url", currentClusteredDoc.get("url")));
                    BasicDBObject pageQuery = new BasicDBObject("url", ampedPage.get("url"));
                    if (ampedPage == null) {
                        currentClusteredDocs.remove(currentClusteredDoc);
                        //docIterator.remove(); // to avoid ConcurrentModificationException??? doesn't work though
                    } else {// put the amped_page id in the topic and put the topic id in the amped_page
                        currentClusteredDoc.put("amped_page_id", ampedPage.get("_id"));
                        BasicDBList clustersThisPageBelongsTo = (BasicDBList) ampedPage.get("topic_ids");
                        if(!clustersThisPageBelongsTo.contains(insertedCluster.get("_id"))){
                            clustersThisPageBelongsTo.add( insertedCluster.get("_id"));
                        }
                        docs.findAndModify(pageQuery, new BasicDBObject(), new BasicDBObject("sort", 1), false, ampedPage, true, true);

                    }
                }
                dbTopic.put("term_weight_pairs", currentTopic.getCentroidTermWeights());
                dbTopic.put("clustered_doc_descriptors", currentClusteredDocs);
                DBObject currentRecord = clusters.findAndModify(topicQuery, new BasicDBObject(), new BasicDBObject("sort", 1), false, dbTopic, true, true);
                //currentTopic.setObjectId((ObjectId)currentRecord.get("_id"));
                numClusters++;
            }
        }
        writeClustersToDocs();
        System.out.println("Number of clusters processed: " + String.valueOf(numClusters) + '\n');

        return clusters;
    }

}