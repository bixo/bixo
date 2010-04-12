package bixo.tools.sitecrawler;

import java.io.IOException;

import org.apache.log4j.Logger;

import bixo.datum.UrlDatum;
import bixo.hadoop.HadoopUtils;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.tap.Tap;

public class BixoJDBCTapFactory {

    private static final Logger LOGGER = Logger.getLogger(BixoJDBCTapFactory.class);
    private static final String JDBC_URL = "jdbc:hsqldb:hsql:mem:sitecrawler";
    private static final String JDBC_DRIVER = "org.hsqldb.jdbcDriver";
    private static final String[] _urlsSinkColumnNames = {"url", "lastFetched", "lastUpdated", "lastStatus", "crawlDepth"};
    private static final String[] _urlsSinkColumnDefs = {"VARCHAR(255)", "BIGINT", "BIGINT", "VARCHAR(32)", "INTEGER"};

    public static Tap createUrlsSourceJDBCTap() {
        String[] primaryKeys = {"url"};
        return createUrlsTap(primaryKeys);

    }

    // Similar to Urls Source Tap except that it doesn't have a primary key - by doing this
    // we 'fool' JDBCTap into thinking that the source and sink url taps aren't the same.
    public static Tap createUrlsSinkJDBCTap() {
        String[] primaryKeys = {};
        return createUrlsTap(primaryKeys);
    }

    public static Tap createUrlsSink2JDBCTap() {
        String[] primaryKeys = {"lastFetched"};
        return createUrlsTap(primaryKeys);
    }

    private static Tap createUrlsTap(String[] primaryKeys) {
        verifyRunEnvironment();
        
        String url = JDBC_URL; 
        String driver = JDBC_DRIVER;
        String tableName = "urls";

        TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys );
        Tap urlsTap = new JDBCTap( url, driver, tableDesc, new JDBCScheme( UrlDatum.FIELDS.append(MetaData.FIELDS), _urlsSinkColumnNames));

        return urlsTap;
  
    }
    
    private static void verifyRunEnvironment() {
        boolean canRun = false;
        try {
            canRun = HadoopUtils.isJobLocal(HadoopUtils.getDefaultJobConf());
        } catch (IOException e) {
            LOGGER.info("Unable to get default job conf: " + e);
        }
        if (!canRun) {
            throw new RuntimeException("The in-memory hsql db jdbc tap can only be used when running in local mode");
        }
    }
}
