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
package bixo.examples.crawl;

//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
//import org.hsqldb.Server;
//
//import cascading.jdbc.JDBCScheme;
//import cascading.jdbc.JDBCTap;
//import cascading.jdbc.TableDesc;
//import cascading.tap.Tap;
//
//import com.scaleunlimited.cascading.BasePlatform;


@SuppressWarnings("rawtypes")
public class JDBCTapFactory {

    /*
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
    
    public static Tap createUrlsSourceJDBCTap(BasePlatform platform, String dbLocation) {
        String[] primaryKeys = {"url"};
        return createUrlsTap(primaryKeys, platform, dbLocation);

    }

    // Similar to Urls Source Tap except that it doesn't have a primary key - by doing this
    // we 'fool' JDBCTap into thinking that the source and sink url taps aren't the same.
    public static Tap createUrlsSinkJDBCTap(BasePlatform platform, String dbLocation) {
        String[] primaryKeys = {};
        return createUrlsTap(primaryKeys, platform, dbLocation);
    }

    public static Tap createUrlsSink2JDBCTap(BasePlatform platform, String dbLocation) {
        String[] primaryKeys = {"lastFetched"};
        return createUrlsTap(primaryKeys, platform, dbLocation);
    }

    private static Tap createUrlsTap(String[] primaryKeys, BasePlatform platform, String dbLocation) {
        initRunEnvironment(platform, dbLocation);
        
        String driver = JDBC_DRIVER;
        String tableName = "urls";

        TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys );
        Tap urlsTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( CrawlDbDatum.FIELDS, _urlsSinkColumnNames));

        return urlsTap;
  
    }
    
    private static void initRunEnvironment(BasePlatform platform, String dbLocation) {
        if (_jdbcUrl == null) {
            
            String db = IN_MEM_DB;
            if (dbLocation != null) {
                String separator = "";
                if (!dbLocation.endsWith("/")) {
                    separator = "/";
                }
                db = PERSISTENT_DB_PREFIX + dbLocation + separator + DB_NAME;
            }
            
            if (platform.isLocal()) {
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
    */
}
