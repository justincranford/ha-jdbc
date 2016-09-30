package justin;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import net.sf.hajdbc.DatabaseClusterListener;
import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.SynchronizationListener;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.balancer.simple.SimpleBalancerFactory;
import net.sf.hajdbc.cache.lazy.LazyDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.h2.H2DialectFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.durability.none.NoDurabilityFactory;
import net.sf.hajdbc.lock.semaphore.SemaphoreLockManagerFactory;
import net.sf.hajdbc.sql.DataSourceDatabase;
import net.sf.hajdbc.sql.DataSourceDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.DatabaseClusterImpl;
import net.sf.hajdbc.sql.InvocationHandler;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.sql.TransactionModeEnum;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;
import net.sf.hajdbc.state.sqlite.SQLiteStateManagerFactory;
import net.sf.hajdbc.sync.FullSynchronizationStrategy;
import net.sf.hajdbc.sync.PassiveSynchronizationStrategy;
import net.sf.hajdbc.util.concurrent.cron.CronExpression;

/**
 * JUnit test to demonstrate H2 embedded database cluster in HA-JDBC 3.0.4-SNAPSHOT. Dependencies:
 *  - Oracle JDK 8u102 x64
 *  - ha-jdbc-3.0.4-snapshot.jar
 *  - junit-4.11.jar
 *  - h2-1.3.176.jar
 *  - commons-dbcp-1.4.jar
 *  - commons-pool-1.6.jar
 * @author justin.cranford
 */
@SuppressWarnings({"unused","null","unchecked", "static-method", "hiding"})
public class TestHAJDBCDemo2 {
	private static final String CLUSTER_NAME                = "clustername";
	private static final String SCHEMA_NAME                 = "schemaname";
	private static final String NETWORK_DATA_DB_HOSTNAME    = "localhost";
	// org.h2.jdbc.JdbcSQLException: A file path that is implicitly relative to the current working directory is not allowed in the database URL "jdbc:h2:/tnp/db/state/clustername". Use an absolute path, ~/name, ./name, or the baseDir setting instead. [90011-181]
	private static final String EMBEDDED_STATE_DB_DIRECTORY = "R:/tnp/db/state";
	private static final String EMBEDDED_DATA_DB_DIRECTORY  = "R:/tnp/db/data";

	private static final boolean DELETE_STATE_DB = true;
	private static final boolean DELETE_DATA_DB  = true;
	private static final boolean INIT_JGROUPS = false;
	private static final boolean USE_PROXY_DATA_SOURCE = true;

	private static final int DB_CONN_TIMEOUT = 60;
	private static final int DB_READ_TIMEOUT = 45;

	private static final String STATE_DB_JDBC_URL_H2       = "jdbc:h2:"     + EMBEDDED_STATE_DB_DIRECTORY + "/{0}";				// {0} will be replaced by "clustername"
	private static final String STATE_DB_JDBC_URL_HSQLDB   = "jdbc:hsqldb:" + EMBEDDED_STATE_DB_DIRECTORY + "/{0}";				// {0} will be replaced by "clustername"
	private static final String STATE_DB_JDBC_URL_DERBY    = "jdbc:derby:"  + EMBEDDED_STATE_DB_DIRECTORY + "/{0};create=true";	// {0} will be replaced by "clustername"

	private static final String DATA_DB_JDBC_URL_H2        = "jdbc:h2:"     + EMBEDDED_DATA_DB_DIRECTORY + "/" + SCHEMA_NAME;
	private static final String DATA_DB_JDBC_URL_HSQLDB    = "jdbc:hsqldb:" + EMBEDDED_DATA_DB_DIRECTORY + "/" + SCHEMA_NAME;
	private static final String DATA_DB_JDBC_URL_DERBY     = "jdbc:derby:"  + EMBEDDED_DATA_DB_DIRECTORY + "/" + SCHEMA_NAME + ";create=true";
	private static final String DATA_DB_JDBC_URL_MYSQL     = "jdbc:mysql://HOSTNAME:3306/" + SCHEMA_NAME;
	private static final String DATA_DB_JDBC_URL_ORACLE    = "jdbc:oracle:thin:@HOSTNAME:1521:" + SCHEMA_NAME;
	private static final String DATA_DB_JDBC_URL_SQLSERVER = "jdbc:sqlserver://HOSTNAME\\INSTANCENAME:1433;databaseName=" + SCHEMA_NAME + ";";
	private static final String DATA_DB_JDBC_URL_DB2       = "jdbc:db2://HOSTNAME:50000/" + SCHEMA_NAME + ":retrieveMessagesFromServerOnGetMessage=true;";
	private static final String DATA_DB_JDBC_URL_POSTGRES  = "jdbc:postgresql://HOSTNAME:5432/" + SCHEMA_NAME;

	private static final String JDBC_DRIVER_H2         = "org.h2.Driver";
	private static final String JDBC_DRIVER_HSQLDB     = "org.hsqldb.jdbcDriver";
	private static final String JDBC_DRIVER_DERBY      = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String JDBC_DRIVER_MYSQL_JDBC = "com.mysql.jdbc.Driver";
	private static final String JDBC_DRIVER_ORACLE     = "oracle.jdbc.OracleDriver";
	private static final String JDBC_DRIVER_SQLSERVER  = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String JDBC_DRIVER_DB2        = "com.ibm.db2.jcc.DB2Driver";
	private static final String JDBC_DRIVER_POSTGRESQL = "org.postgresql.Driver";

	private static final String DB_VALIDATION_QUERY_H2        = "SELECT 1";
	private static final String DB_VALIDATION_QUERY_HSQLDB    = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS";
	private static final String DB_VALIDATION_QUERY_DERBY     = "VALUES 1";
	private static final String DB_VALIDATION_QUERY_MYSQL     = "/* ping */ SELECT 1";
	private static final String DB_VALIDATION_QUERY_ORACLE    = "SELECT 1 FROM DUAL";
	private static final String DB_VALIDATION_QUERY_SQLSERVER = "SELECT 1";
	private static final String DB_VALIDATION_QUERY_DB2       = "SELECT 1 FROM SYSIBM.SYSDUMMY1";
	private static final String DB_VALIDATION_QUERY_POSTGRES  = "SELECT 1";
	private static final String DB_VALIDATION_QUERY_INGRES    = "SELECT 1";
	private static final String DB_VALIDATION_QUERY_FIREBIRD  = "SELECT 1 FROM rdb$database";

	private static final String DB_CONNECTION_PROPERTIES_H2        = null;
	private static final String DB_CONNECTION_PROPERTIES_HSQLDB    = null;
	private static final String DB_CONNECTION_PROPERTIES_DERBY     = null;	// create=true
	private static final String DB_CONNECTION_PROPERTIES_MYSQL     = "connectTimeout=" + DB_CONN_TIMEOUT + "000;socketTimeout=" + DB_READ_TIMEOUT + "000;useUnicode=true;characterSetResults=utf8;cacheServerConfiguration=true;alwaysSendSetIsolation=false;useLocalSessionState=true;useLocalTransactionState=true;rewriteBatchedStatements=true;useHostsInPrivileges=false;enableQueryTimeouts=true;maintainTimeStats=false;maxAllowedPacket=-1;";
	private static final String DB_CONNECTION_PROPERTIES_ORACLE    = "SetBigStringTryClob=true;oracle.net.CONNECT_TIMEOUT=" + DB_CONN_TIMEOUT + "000;oracle.jdbc.ReadTimeout=" + DB_READ_TIMEOUT + "000;";
	private static final String DB_CONNECTION_PROPERTIES_SQLSERVER = "loginTimeout=" + DB_CONN_TIMEOUT + ";";
	private static final String DB_CONNECTION_PROPERTIES_DB2       = "loginTimeout=" + DB_CONN_TIMEOUT + ";blockingReadConnectionTimeout=" + DB_READ_TIMEOUT + ";";
	private static final String DB_CONNECTION_PROPERTIES_POSTGRES  = "currentSchema=" + SCHEMA_NAME + ";loginTimeout=" + DB_CONN_TIMEOUT + ";connectionTimeout=" + DB_CONN_TIMEOUT + ";socketTimeout=" + DB_READ_TIMEOUT + ";tcpKeepAlive=true;hostRecheckSeconds=0;loglevel=0;";

	public static final Pattern JDBC_URL_H2        = Pattern.compile("^(jdbc:h2:([^;]*?)\\/([^;]+?))$");						// jdbc:h2://HOMEDIR/SCHEMANAME
	public static final Pattern JDBC_URL_HSQLDB    = Pattern.compile("^(jdbc:hsqldb:([^;]*?)\\/([^;]+?))$");					// jdbc:hsqldb://HOMEDIR/SCHEMANAME
	public static final Pattern JDBC_URL_DERBY     = Pattern.compile("^(jdbc:derby:([^;]*?)\\/([^;]+?)(;.*?))$");				// jdbc:derby://HOMEDIR/SCHEMANAME;create=true
	public static final Pattern JDBC_URL_MYSQL     = Pattern.compile("^(jdbc:mysql:\\/\\/)(.+?)(:\\d+)?(\\/.+)*$");				// jdbc:mysql://HOSTNAME:3306/SCHEMANAME
	public static final Pattern JDBC_URL_ORACLE    = Pattern.compile("^(jdbc:oracle:thin:@)(.+?)(:\\d+)?(:.+)*$");				// jdbc:oracle:thin:@HOSTNAME:1521:SCHEMANAME
	public static final Pattern JDBC_URL_SQLSERVER = Pattern.compile("^(jdbc:sqlserver:\\/\\/)(.+?)(\\\\.+?)?(:\\d+)?(;.+)*$");	// jdbc:sqlserver://HOSTNAME\INSTANCENAME:1433;databaseName=SCHEMANAME;
	public static final Pattern JDBC_URL_DB2       = Pattern.compile("^(jdbc:db2:\\/\\/)(.+?)(:\\d+)?(\\/.+)*$");				// jdbc:db2://HOSTNAME:50000/SCHEMANAME:retrieveMessagesFromServerOnGetMessage=true;
	public static final Pattern JDBC_URL_POSTGRES  = Pattern.compile("^(jdbc:postgresql:\\/\\/)(.+?)(:\\d+)?(\\/.+)*$");		// jdbc:postgresql://HOSTNAME:5432/SCHEMANAME

	@BeforeClass
	@AfterClass
	public static void cleanup() throws Exception {
		if (DELETE_STATE_DB) {
			System.out.println("Deleted " + EMBEDDED_STATE_DB_DIRECTORY + ": " + delete(EMBEDDED_STATE_DB_DIRECTORY));
			System.out.println("Deleted " + EMBEDDED_DATA_DB_DIRECTORY  + ": " + delete(EMBEDDED_DATA_DB_DIRECTORY));
		}
	}

	@Test
	public void test() throws Exception {
		final long start = System.currentTimeMillis();
		try {
//			String[] DATA_DB_JDBC_URLS = new String[]{DATA_DB_JDBC_URL_MYSQL.replace("HOSTNAME",NETWORK_DATA_DB_HOSTNAME) + "1", DATA_DB_JDBC_URL_MYSQL.replace("HOSTNAME",NETWORK_DATA_DB_HOSTNAME) + "2"};
			final String[] DATA_DB_JDBC_URLS = new String[]{DATA_DB_JDBC_URL_H2 + "1", DATA_DB_JDBC_URL_H2 + "2"};
			final int repeatConnections = 3;
			final String stateDbUsername = "sa";
			final String stateDbPassword = null;
			final String dataDbUsername = "root";
			final String dataDbPassword = "rootpwd";
			this.test(JDBC_DRIVER_H2, STATE_DB_JDBC_URL_H2, JDBC_DRIVER_H2, DATA_DB_JDBC_URLS, DB_VALIDATION_QUERY_H2, DB_CONNECTION_PROPERTIES_H2, stateDbUsername, stateDbPassword, dataDbUsername, dataDbPassword, repeatConnections);
		} finally {
			System.out.println("Execution time (testConfigDataSource) = " + (System.currentTimeMillis() - start) + "msec");
		}
	}

	private void test(String stateDatabaseJdbcDriver, String stateDatabaseJdbcUrl, String dataDatabaseJdbcDriver, String[] dataDatabaseJdbcUrls, String validationQuery, String connectionProperties, String stateDbUsername, String stateDbPassword, String dataDbUsername, String dataDbPassword, int numConnections) throws Exception {
//		System.setProperty(StateManager.CLEAR_LOCAL_STATE, Boolean.toString(true));	// ha-jdbc.state.clear=true
		SQLStateManagerFactory stateManagerFactory = null;
		try {
			stateManagerFactory = new SQLStateManagerFactory();
			stateManagerFactory.setUrlPattern(stateDatabaseJdbcUrl);	// override internal default JDBC URL pointing under user directory
			if (null != stateDbUsername) {
				stateManagerFactory.setUser(stateDbUsername);	// optional
			}
			if (null != stateDbPassword) {
				stateManagerFactory.setPassword(stateDbPassword);	// optional
			}
		} catch(Throwable e) {
			throw new Exception("Failed to config state db", e);
		}

		JGroupsCommandDispatcherFactory commandDispatcherFactory = null;
		if (INIT_JGROUPS) {
			System.setProperty("jgroups.bind_address", NETWORK_DATA_DB_HOSTNAME);
			System.setProperty("jgroups.udp.mcast_port", "7900");
			commandDispatcherFactory = new JGroupsCommandDispatcherFactory();
			commandDispatcherFactory.setTimeout(3000);
			commandDispatcherFactory.setStack("udp.xml");
		}

		CronExpression cronExpression = null;
		try {
			cronExpression = new CronExpression("0 0 0 1 1 ? 1999");	// Don't party like it's 1999!
		} catch(Exception e) {
			throw new Exception("Failed to init cron", e);
		}

		final HashMap<String,SynchronizationStrategy> synchronizationStrategyMap = new HashMap<String,SynchronizationStrategy>();
//		synchronizationStrategyMap.put("dump-restore", new DumpRestoreSynchronizationStrategy());		// ideal for many differences (MySQL and PostgreSQL only)
		synchronizationStrategyMap.put("full",         new FullSynchronizationStrategy());				// ideal for many differences (all databases)
//		synchronizationStrategyMap.put("diff",         new DifferentialSynchronizationStrategy());		// ideal for few differences (all databases, iff all tables have identity columns)
		synchronizationStrategyMap.put("passive",      new PassiveSynchronizationStrategy());			// ideal for external synchronization algorithm

		final ArrayList<DataSourceDatabase> configDataSources = new ArrayList<DataSourceDatabase>(dataDatabaseJdbcUrls.length);
		int j = 1;
		for (final String jdbcUrl : dataDatabaseJdbcUrls) {
			final DataSourceDatabase configDataSource = new DataSourceDatabase();
			configDataSource.setId(SCHEMA_NAME + j++);
			configDataSource.setLocal(true);
			configDataSource.setWeight(1);
			configDataSource.setLocation("org.apache.commons.dbcp.BasicDataSource");	// NOT org.apache.tomcat.dbcp.dbcp.BasicDataSource! 
			configDataSource.setProperty("url",                           jdbcUrl);
			configDataSource.setProperty("driverClassName",               dataDatabaseJdbcDriver);
			configDataSource.setProperty("username",                      dataDbUsername);	// mandatory for DBCP, even though it might be optional in underlying DB (ex: H2)
			configDataSource.setProperty("password",                      dataDbPassword);	// mandatory for DBCP, even though it might be optional in underlying DB (ex: H2)
			configDataSource.setProperty("maxActive",                     "100");
			configDataSource.setProperty("maxIdle",                       "25");
			configDataSource.setProperty("minIdle",                       "5");
			configDataSource.setProperty("removeAbandoned",               "true");
			configDataSource.setProperty("removeAbandonedTimeout",        "300");
			configDataSource.setProperty("logAbandoned",                  "false");
			configDataSource.setProperty("validationQueryTimeout",        "3");
			configDataSource.setProperty("validationQuery",               validationQuery);
			configDataSource.setProperty("testOnBorrow",                  "true");
			configDataSource.setProperty("testOnReturn",                  "false");
			configDataSource.setProperty("maxWait",                       "10000");
			configDataSource.setProperty("testWhileIdle",                 "false");
			configDataSource.setProperty("timeBetweenEvictionRunsMillis", "300000");
			configDataSource.setProperty("numTestsPerEvictionRun",        "3");
			configDataSource.setProperty("minEvictableIdleTimeMillis",    "1800000");
			if (null != connectionProperties) {
				configDataSource.setProperty("connectionProperties", connectionProperties);
			}
			configDataSources.add(configDataSource);
		}

		final DataSourceDatabaseClusterConfiguration config = new DataSourceDatabaseClusterConfiguration();
		config.setBalancerFactory(new SimpleBalancerFactory());
		config.setSynchronizationStrategyMap(synchronizationStrategyMap);
		config.setDefaultSynchronizationStrategy("full");
		config.setDialectFactory(new H2DialectFactory());
		config.setDatabaseMetaDataCacheFactory(new LazyDatabaseMetaDataCacheFactory());
		config.setTransactionMode(TransactionModeEnum.SERIAL);
		config.setFailureDetectionExpression(cronExpression);
		config.setAutoActivationExpression(cronExpression);
		config.setIdentityColumnDetectionEnabled(false);
		config.setSequenceDetectionEnabled(false);
		config.setCurrentDateEvaluationEnabled(true);
		config.setCurrentTimeEvaluationEnabled(true);
		config.setCurrentTimestampEvaluationEnabled(true);
		config.setRandEvaluationEnabled(true);
		config.setEmptyClusterAllowed(false);
		config.setDurabilityFactory(new NoDurabilityFactory());
		if (null != commandDispatcherFactory) {
			config.setDispatcherFactory(commandDispatcherFactory);
		}
		config.setStateManagerFactory(stateManagerFactory);
		config.setLockManagerFactory(new SemaphoreLockManagerFactory());
		config.setDatabases(configDataSources);

		final net.sf.hajdbc.sql.DataSource configDataSource = new net.sf.hajdbc.sql.DataSource();	// CONFIG DATA SOURCE (leaks connections)
		configDataSource.setCluster(CLUSTER_NAME);
		final SimpleDatabaseClusterConfigurationFactory<DataSource, DataSourceDatabase> configFactory = new SimpleDatabaseClusterConfigurationFactory<DataSource, DataSourceDatabase>(config);
		configDataSource.setConfigurationFactory(configFactory);
		configDataSource.setTimeout(3, TimeUnit.SECONDS);

		final DataSource proxyDataSource = configDataSource.getProxy();							// PROXY DATA SOURCE (does not leak connections)

		final InvocationHandler<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException, ProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException>> handler = (InvocationHandler<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException, ProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException>>) Proxy.getInvocationHandler(proxyDataSource);
		final ProxyFactory<javax.sql.DataSource, DataSourceDatabase, javax.sql.DataSource, SQLException> proxyFactory = handler.getProxyFactory();
		final DatabaseClusterImpl<DataSource, DataSourceDatabase> clusterBean = (DatabaseClusterImpl<DataSource, DataSourceDatabase>) proxyFactory.getDatabaseCluster();
		final StateManager stateManager = clusterBean.getStateManager();
		clusterBean.addListener(new CustomDatabaseClusterListener(clusterBean));
		clusterBean.addSynchronizationListener(new CustomSynchronizationListener(clusterBean));


		System.out.println("ACTIVE: " + new TreeSet<String>(clusterBean.getActiveDatabases()));

		DataSource chosenDataSource = (USE_PROXY_DATA_SOURCE ? proxyDataSource : configDataSource);

		connectLoop(chosenDataSource, numConnections, validationQuery);
		executeUpdate(chosenDataSource, "DROP TABLE IF EXISTS testtbl");
		executeUpdate(chosenDataSource, "CREATE TABLE testtbl (testcol INT NOT NULL PRIMARY KEY) ENGINE=INNODB");
		executeUpdate(chosenDataSource, "TRUNCATE TABLE testtbl");
		executeUpdate(chosenDataSource, "INSERT INTO testtbl (testcol) VALUES (100)");


		boolean isActiveFromDataSourceDatabase1  = configDataSources.get(0).isActive();
		boolean isActiveFromDataSourceDatabase2  = clusterBean.getDatabase(SCHEMA_NAME+"1").isActive();
		boolean isActiveFromFromDatabaseCluster = clusterBean.getActiveDatabases().contains(SCHEMA_NAME+"1");
		System.out.println("isActiveFromDataSourceDatabase1 = " + isActiveFromDataSourceDatabase1);
		System.out.println("isActiveFromDataSourceDatabase2 = " + isActiveFromDataSourceDatabase2);
		System.out.println("isActiveFromFromDatabaseCluster = " + isActiveFromFromDatabaseCluster);
		if (isActiveFromFromDatabaseCluster) {
			clusterBean.deactivate(SCHEMA_NAME+"1");
		}

		connectLoop(chosenDataSource, numConnections, validationQuery);

		executeUpdate(chosenDataSource, "INSERT INTO testtbl (testcol) VALUES (200)");

		for (int i=0; i<1; i++) {
			clusterBean.isAlive(SCHEMA_NAME + "1");
			connectLoop(chosenDataSource, 1, validationQuery);
			clusterBean.isAlive(SCHEMA_NAME + "2");
			connectLoop(chosenDataSource, 1, validationQuery);
		}

		// this activate(String) call throws exception
		try {
			clusterBean.activate(SCHEMA_NAME + "1");
		} catch(Exception npe) {
			npe.printStackTrace();	// SQLException causes by NullPointerException
		}

		// this activate(DataSourceDatabase, StateManager) call activates, but fails to resynchronize db1 using db2's contents
		DataSourceDatabase dataSourceDatabase = clusterBean.getDatabase(SCHEMA_NAME + "1");
		clusterBean.activate(dataSourceDatabase, stateManager);

		// update triggers db1 deactivation because contents do not match db2 (i.e. db2 has 2 rows, db1 has one row)
		executeUpdate(chosenDataSource, "UPDATE testtbl SET testcol=testcol+1");
//		SEVERE: A result [1] of an operation on database db2 in cluster net.sf.hajdbc.sql.DatabaseClusterImpl@bc8928 did not match the expected result [2]

//		configDataSource.stop();
	}

	private static void executeUpdate(final DataSource proxyDataSource, final String sql) throws SQLException {
		try (Connection c = proxyDataSource.getConnection()) {
			try (Statement s = c.createStatement()) {
				s.executeUpdate(sql);
			}
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}

	private static void connectLoop(final DataSource ds, final int num, final String validationQuery) throws SQLException {
		for (int i=1; i<=num; i++) {	// open N connections, run validation query, and autoclose
			try (final Connection c = ds.getConnection()) {
				try (final Statement s = c.createStatement()) {
					s.execute(validationQuery);
					try (final ResultSet rs = s.getResultSet()) {
						while (rs.next()) {
							System.out.println("Connection["+i+"]=" + rs.getString(1));
						}
					}
				}
			}
		}
	}

	public static boolean delete(final String path) throws Exception {
        final File fileOrDir = new File(path);
        if (fileOrDir.isDirectory()) {
        	final File[] filesAndSubdirs = fileOrDir.listFiles();
            if (null == filesAndSubdirs) {
            	return false;
            } else if (0 != filesAndSubdirs.length) {
                for (final File fileOrSubdir : filesAndSubdirs) {
                    if (!delete(fileOrSubdir.getCanonicalPath())) {
                    	return false;
                    }
                }
            }
        }
        return fileOrDir.delete();
    }

	public static final void main(String[] args) throws Exception {
		System.out.println(delete("garbage"));
		System.out.println(delete(EMBEDDED_STATE_DB_DIRECTORY));
		System.out.println(delete(EMBEDDED_DATA_DB_DIRECTORY));
	}

	private static class CustomDatabaseClusterListener implements DatabaseClusterListener {
		private DatabaseClusterImpl<DataSource, DataSourceDatabase> clusterBean;
		protected CustomDatabaseClusterListener(final DatabaseClusterImpl<DataSource, DataSourceDatabase> clusterBean) {
			this.clusterBean = clusterBean;
		}
		private void showClusterState() {
			try {
				final TreeSet<String> activeDatabases = new TreeSet<String>(this.clusterBean.getActiveDatabases());
				System.out.println("ACTIVE: " + activeDatabases);
	        } catch(Exception ex) {
	        	System.out.println("Exception: " + ex.getLocalizedMessage());
	        }
		}
		@Override
		public void activated(final DatabaseEvent e) {
			System.out.println("Detected activated event for '" + e.getSource() + "'.");
	        showClusterState();
		}
		@Override
		public void deactivated(final DatabaseEvent e) {
			System.out.println("Detected deactivated event for '" + e.getSource() + "'.");
	        showClusterState();
		}
	}

	private static class CustomSynchronizationListener implements SynchronizationListener {
		private DatabaseClusterImpl<DataSource, DataSourceDatabase> clusterBean;
		protected CustomSynchronizationListener(final DatabaseClusterImpl<DataSource, DataSourceDatabase> clusterBean) {
			this.clusterBean = clusterBean;
		}
		@Override
		public void beforeSynchronization(final DatabaseEvent e) {
			System.out.println("Before synchronization event for '" + e.getSource() + "'.");
		}
		@Override
		public void afterSynchronization(final DatabaseEvent e) {
			System.out.println("After synchronization event for '" + e.getSource() + "'.");
		}
	}
}