package avem.jdbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import avem.common.util.dao.DaoManagerException;
import avem.common.util.properties.GlobalFileProperties;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

/**
 * Clase madre para todos los managers JDBC. Esta clase contiene las datasource
 * utilizadas por los managers.
 * 
 * CREATE SCHEMA `cgps` DEFAULT CHARACTER SET utf8 ;
 * 
 * @see #setDataSource(DataSource)
 * @author avmendo
 * 
 */
public abstract class JdbcManager {
	static Logger log = Logger.getLogger(JdbcManager.class);

	protected static DataSource dataSource = null;

	/**
	 * Instala la datasource que sera utilizada por todos los managers JDBC
	 * 
	 * @param newDataSource
	 */
	public static void setDataSource(DataSource newDataSource) {
		if (newDataSource == null) {
			log.error("Inicializanfdo la DataSource con un valor NULO.");
		} else {
			log
					.info("Inicializanfdo la DataSource con '" + newDataSource
							+ "'");
			dataSource = newDataSource;
		}
	}

	/**
	 * Obtiene una datasource JNI.
	 * 
	 * @see #setDataSource(DataSource)
	 * 
	 * @param jniDatasourceName
	 * @return
	 * @throws NamingException
	 */
	public static DataSource getJniDataSource(String jniDatasourceName)throws NamingException {

		// Context initContext = new InitialContext();
		// Context envContext = (Context) initContext.lookup("java:/comp/env");
		// DataSource dataSource = (DataSource)
		// envContext.lookup(jniDatasourceName);

		// javax.naming.InitialContext ic = new javax.naming.InitialContext();
		// javax.sql.DataSource dataSource = (javax.sql.DataSource) ic
		// .lookup(jniDatasourceName);

		log.info("Accedeiendo a la Datasource jni:'" + jniDatasourceName + "'");

		Context initContext = new InitialContext();
		Context envContext = (Context) initContext.lookup("java:/comp/env");
		DataSource ds = (DataSource) envContext.lookup(jniDatasourceName);
		if (ds == null) {
			log
					.warn("la busqueda en java:/comp/env dio un resultado nulo buscando directamente el nombre");
			ds = (DataSource) initContext.lookup(jniDatasourceName);
			if (ds == null) {
				log.warn("No se encontro una datasource jni de nombre:'"
						+ jniDatasourceName + "'");
			}
		} else {
			log.info("Se obtubo la datasource:'" + ds + "' de nombre:'"
					+ jniDatasourceName + "'");
		}

		return ds;
	}

//	/**
//	 * @param driverClassName
//	 * @param url
//	 *            jdbc:oracle:thin:@172.24.0.22:1521:ESTRADA1
//	 * @param user
//	 *            USER
//	 * @param password
//	 *            SECRET
//	 * @return
//	 * @throws SQLException
//	 */
//	public static DataSource createOracleDataSource(String url,
//			String username, String password) throws SQLException {
//
//		// http://www.cs.uvm.edu/oracle9doc/java.901/a90211/connpoca.htm
//
////		try {
////			DriverAdapterCPDS cpds = new DriverAdapterCPDS();
////	        cpds.setDriver("oracle.jdbc.OracleDriver");
////	        cpds.setUrl(url);
////	        cpds.setUser(username);
////	        cpds.setPassword(password);
////	
////	        SharedPoolDataSource datasource = new SharedPoolDataSource();
////	        datasource.setConnectionPoolDataSource(cpds);
////	        datasource.setMaxActive(maxActive);
////	        datasource.setMaxWait(maxWait);
////	        datasource.setMaxIdle(maxIdle);
////			return datasource;
////		}catch(ClassNotFoundException e){
////			throw new SQLException("oracle.jdbc.OracleDriver",e);
////		}
////		
//		
//		
//		
////		BasicDataSource datasource=new BasicDataSource();
////		datasource.setUrl(url);
////		datasource.setUsername(username);
////		datasource.setPassword(password);
////		datasource.setDriverClassName("oracle.jdbc.OracleDriver");
//
//		
////		
////		GenericObjectPool connectionPool = new GenericObjectPool(null);
////		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, username, password);
////		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,connectionPool,null,null,false,true);
////		PoolingDataSource datasource = new PoolingDataSource(connectionPool);
//		
//		
//		
//		/*
//		 * De Oracle
//		 */
//		oracle.jdbc.pool.OracleDataSource oracleDataSource = new oracle.jdbc.pool.OracleDataSource();
//
//		oracleDataSource.setURL(url);
//		oracleDataSource.setUser(username);
//		oracleDataSource.setPassword(password);
//		oracleDataSource.setConnectionCachingEnabled(true);
//		Properties prop=oracleDataSource.getConnectionCacheProperties();
//		
//		if (prop!=null){
//			prop.list(System.err);
//		}
//		// Esto lanza una ONC Exception
//		// datasource.setFastConnectionFailoverEnabled(true);
//		return oracleDataSource;
//	
//		/**
//		 * Antigua
//		 */
//		
////		 OracleConnectionPoolDataSource datasource = new
////		 OracleConnectionPoolDataSource();
////		
////		 datasource.setURL(url);
////		 datasource.setUser(username);
////		 datasource.setPassword(password);
//
//
//	}
	

	/**
	 * @param driverClassName
	 * @param url
	 *            jdbc:oracle:thin:@172.24.0.22:1521:ESTRADA1
	 * @param user
	 *            USER
	 * @param password
	 *            SECRET
	 * @return
	 * @throws SQLException
	 */
	public static DataSource createMySQLDataSource(String url,
												   String username, 
												   String password) throws SQLException {
		
		/*
		 * De Oracle
		 */
		MysqlConnectionPoolDataSource datasource=new MysqlConnectionPoolDataSource();
		datasource.setUser(username); 
		datasource.setPassword(password); 
		datasource.setURL(url); 

		return datasource;
	}
	

//	/**
//	 * @param filePath
//	 * @return
//	 * @throws NamingException
//	 * @throws FileNotFoundException
//	 * @throws IOException
//	 */
//	public static DataSource createOracleDataSourceFromPropertiesFile(
//			String filePath) throws SQLException, FileNotFoundException,
//			IOException {
//		
//		filePath=GlobalFileProperties.getPropertiesFilePath(filePath);
//		
//		Properties prop = new Properties();
//		prop.load(new FileInputStream(filePath));
//
//		String url = prop.getProperty("jdbc.url");
//		String username = prop.getProperty("jdbc.username");
//		String password = prop.getProperty("jdbc.password");
////        int maxActive=getIntProperty(prop,"jdbc.datasource.maxActive",30);
////        int maxWait=getIntProperty(prop,"jdbc.datasource.maxWait",-1);
////        int maxIdle=getIntProperty(prop,"jdbc.datasource.maxIdle",3);
//
//        
//		DataSource ret=createOracleDataSource(url, username, password);
//		return ret;
//	}

	/**
	 * @param filePath
	 * @return
	 * @throws NamingException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static DataSource createMySQLDataSourceFromPropertiesFile(String filePath) throws SQLException, 
		FileNotFoundException,IOException {
		
		filePath=GlobalFileProperties.getPropertiesFilePath(filePath);
		
		Properties prop = new Properties();
		prop.load(new FileInputStream(filePath));

		String url = prop.getProperty("jdbc.url");
		String username = prop.getProperty("jdbc.username");
		String password = prop.getProperty("jdbc.password");
        
		DataSource ret=createMySQLDataSource(url, username, password);
		return ret;
	}

	public static boolean testConnection() throws Exception{
		if (dataSource==null){
			throw new DaoManagerException("datasource is null");
		} else {
			try {
				Connection connection = dataSource.getConnection();
				String testQuery="SELECT 1 from DUAL";
				try {
					PreparedStatement pstm = connection.prepareStatement(testQuery);
					
					pstm.executeQuery(testQuery);
					return true;
					
				} finally {
					connection.close();
				}	
			} catch (Exception e) {
				throw new DaoManagerException("Connection test error", e);
			}
		}
	}

//	private static int getIntProperty(Properties prop,String name,int defaultValue){
//		String value=prop.getProperty(name);
//
//		try {
//			return Integer.parseInt(value);
//		} catch(Exception e){
//			log.debug("Paring int value:'"+value+"' for propertie name :'"+name+"'");
//			return defaultValue;
//		}
//	
//	}
}
