package bixo.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.utils.FsUtils;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class SimpleStatusTool {
	private static final Logger LOGGER = Logger.getLogger(SimpleStatusTool.class);
	
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

	public static void main(String[] args) {
        SimpleStatusToolOptions options = new SimpleStatusToolOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        String crawlDirName = options.getCrawlDir();

        try {
        	JobConf conf = new JobConf();
        	Path crawlDirPath = new Path(crawlDirName);
        	FileSystem fs = crawlDirPath.getFileSystem(conf);

        	if (!fs.exists(crawlDirPath)) {
    			System.err.println("Prior crawl output directory does not exist: " + crawlDirName);
    			System.exit(-1);
        	}
        	
        	// Skip Hadoop/Cascading DEBUG messages.
        	Logger.getRootLogger().setLevel(Level.INFO);
        	
        	int prevLoop = -1;
        	Path curDirPath = null;
        	while ((curDirPath = FsUtils.findNextLoopDir(fs, crawlDirPath, prevLoop)) != null) {
        		String curDirName = curDirPath.toUri().toString();
        		LOGGER.info("");
        		LOGGER.info("================================================================");
        		LOGGER.info("Processing " + curDirName);
        		LOGGER.info("================================================================");
        		
        		int curLoop = FsUtils.extractLoopNumber(curDirPath);
        		if (curLoop != prevLoop + 1) {
        			LOGGER.warn(String.format("Missing directories between %d and %d", prevLoop, curLoop));
        		}
        		
        		prevLoop = curLoop;
        		
        		// Process the content, status and urls in curPath
                Tap statusTap = new Hfs(new TextLine(), curDirName + "/status");
                
                TupleEntryIterator iter = statusTap.openForRead(conf);
                
                UrlStatus[] statusValues = UrlStatus.values();
                int[] statusCounts = new int[statusValues.length];
                int totalEntries = 0;
                while (iter.hasNext()) {
                    TupleEntry entry = iter.next();
                    totalEntries += 1;

                    // http://www.mcafee.com/apps/campaigns/survey.asp?mktg=vRn1KQ2oV7cJXdVj0Xudyks4CQ%3d%3d	SKIPPED_BY_SCORE	null	null	1255201363035
                    // <url>  <status>  <headers>  <exception>  <time>
                    String statusLine = entry.getString("line");
                    String[] pieces = statusLine.split("\t");
                    UrlStatus status = UrlStatus.valueOf(pieces[1]);
                    statusCounts[status.ordinal()] += 1;
                }
                
                
                for (int i = 0; i < statusCounts.length; i++) {
                	if (statusCounts[i] != 0) {
                		LOGGER.info(String.format("Status %s: %d", statusValues[i].toString(), statusCounts[i]));
                	}
                }
                LOGGER.info("Total status: " + totalEntries);
                LOGGER.info("");
                
                Fields metaDataFields = new Fields();
                Tap urlTap = new Hfs(new SequenceFile(UrlDatum.FIELDS), curDirName + "/urls");
                iter = urlTap.openForRead(conf);
                totalEntries = 0;
                int fetchedUrls = 0;
                int unfetchedUrls = 0;
                
                while (iter.hasNext()) {
                    TupleEntry entry = iter.next();
                    totalEntries += 1;
                    
                    UrlDatum datum = new UrlDatum(entry.getTuple(), metaDataFields);
                    if (datum.getLastFetched() == 0) {
                    	unfetchedUrls += 1;
                    } else {
                    	fetchedUrls += 1;
                    }
                }
                
                LOGGER.info(String.format("%d fetched URLs", fetchedUrls));
                LOGGER.info(String.format("%d unfetched URLs", unfetchedUrls));
                LOGGER.info("Total URLs: " + totalEntries);
                LOGGER.info("");
        	}
        } catch (Throwable t) {
        	LOGGER.error("Exception running tool", t);
            System.exit(-1);
        }
	}

}
