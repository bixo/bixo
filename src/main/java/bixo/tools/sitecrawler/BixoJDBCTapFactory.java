package bixo.tools.sitecrawler;

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

public class BixoJDBCTapFactory {

    private static final Logger LOGGER = Logger.getLogger(BixoJDBCTapFactory.class);
    private static final String IN_MEM_JDBC_URL = "jdbc:hsqldb:mem:sitecrawler";
   private static final String JDBC_DRIVER = "org.hsqldb.jdbcDriver";
    private static final String[] _urlsSinkColumnNames = {"url", "lastFetched", "lastUpdated", "lastStatus", "crawlDepth"};
    private static final String[] _urlsSinkColumnDefs = {"VARCHAR(255)", "BIGINT", "BIGINT", "VARCHAR(32)", "INTEGER"};

    private static String _jdbcUrl;
    private static Server _server;
    
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
        initRunEnvironment();
        
        String driver = JDBC_DRIVER;
        String tableName = "urls";

        TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys );
        Tap urlsTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( UrlDatum.FIELDS.append(MetaData.FIELDS), _urlsSinkColumnNames));

        return urlsTap;
  
    }
    
    private static void initRunEnvironment() {
        if (_jdbcUrl == null) {
            JobConf jobConf;
            try {
                jobConf = HadoopUtils.getDefaultJobConf();
            } catch (IOException e) {
                throw new RuntimeException("Unable to get default job conf: " + e);
            }
            if (HadoopUtils.isJobLocal(jobConf)) {
                _jdbcUrl = IN_MEM_JDBC_URL;
            } else {

                if (_server == null) {
                    try {
                        InetAddress addr = InetAddress.getLocalHost();
                        String hostAddress = addr.getHostAddress();
                        _jdbcUrl = "jdbc:hsqldb:hsql://" + hostAddress + "/sitecrawler;shutdown=true";
                    } catch (UnknownHostException e) {
                        throw new RuntimeException("Unable to get host address: " + e);
                    }
                    String serverProps = "database.0=mem:sitecrawler";
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
