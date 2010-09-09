package bixo.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.hsqldb.Server;

import bixo.datum.UrlDatum;
import bixo.hadoop.HadoopUtils;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.tap.Tap;

public class JDBCTapFactory {

    private static final Logger LOGGER = Logger.getLogger(JDBCTapFactory.class);
    private static final String JDBC_URL_PREFIX = "jdbc:hsqldb:";
    private static final String DB_NAME = "sitecrawler";
    private static final String IN_MEM_DB = "mem:" + DB_NAME;
    private static final String PERSISTENT_DB_PREFIX = "file:";
    
    private static final String JDBC_SERVER_URL_PREFIX = JDBC_URL_PREFIX + "hsql://";
    private static final String JDBC_SERVER_SUFFIX = "/" + DB_NAME + ";shutdown=true";
        
    private static final String JDBC_DRIVER = "org.hsqldb.jdbcDriver";
    private static final String[] _urlsSinkColumnNames = {"url", "lastFetched", "lastUpdated", "lastStatus", "crawlDepth"};
    private static final String[] _urlsSinkColumnDefs = {"VARCHAR(255)", "BIGINT", "BIGINT", "VARCHAR(32)", "INTEGER"};

    private static String _jdbcUrl;
    private static Server _server;
    
    public static Tap createUrlsSourceJDBCTap(String dbLocation) {
        String[] primaryKeys = {"url"};
        return createUrlsTap(primaryKeys, dbLocation);

    }

    // Similar to Urls Source Tap except that it doesn't have a primary key - by doing this
    // we 'fool' JDBCTap into thinking that the source and sink url taps aren't the same.
    public static Tap createUrlsSinkJDBCTap(String dbLocation) {
        String[] primaryKeys = {};
        return createUrlsTap(primaryKeys, dbLocation);
    }

    public static Tap createUrlsSink2JDBCTap(String dbLocation) {
        String[] primaryKeys = {"lastFetched"};
        return createUrlsTap(primaryKeys, dbLocation);
    }

    private static Tap createUrlsTap(String[] primaryKeys, String dbLocation) {
        initRunEnvironment(dbLocation);
        
        String driver = JDBC_DRIVER;
        String tableName = "urls";

        TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys );
        Tap urlsTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( CrawlDbDatum.FIELDS, _urlsSinkColumnNames));

        return urlsTap;
  
    }
    
    private static void initRunEnvironment(String dbLocation) {
        if (_jdbcUrl == null) {
            JobConf jobConf;
            try {
                jobConf = HadoopUtils.getDefaultJobConf();
            } catch (IOException e) {
                throw new RuntimeException("Unable to get default job conf: " + e);
            }
            String db = IN_MEM_DB;
            if (dbLocation != null) {
                String separator = "";
                if (!dbLocation.endsWith("/")) {
                    separator = "/";
                }
                db = PERSISTENT_DB_PREFIX + dbLocation + separator + DB_NAME;
            }
            if (HadoopUtils.isJobLocal(jobConf)) {
                _jdbcUrl = JDBC_URL_PREFIX + db;
            } else {

                if (_server == null) {
                    try {
                        InetAddress addr = InetAddress.getLocalHost();
                        String hostAddress = addr.getHostAddress();
                        _jdbcUrl = JDBC_SERVER_URL_PREFIX + hostAddress + JDBC_SERVER_SUFFIX;
                    } catch (UnknownHostException e) {
                        throw new RuntimeException("Unable to get host address: " + e);
                    }
                    String serverProps = "database.0=" + db;
                    _server = new Server();
                    _server.putPropertiesFromString(serverProps);
                    _server.setDatabaseName(0, "sitecrawler");
                    _server.setLogWriter(null);
                    _server.setErrWriter(null);
                    _server.start();
                }
                LOGGER.info("Using hsqldb in server mode");
            }
        }
    }

    public static void shutdown() {
        if (_server != null) {
            _server.shutdown();
        }
    }
}
