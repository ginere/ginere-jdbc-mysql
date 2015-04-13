package eu.ginere.jdbc.mysql;

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

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.properties.GlobalFileProperties;

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
public abstract class MySQLDatabaseUtils {
	static Logger log = Logger.getLogger(MySQLDatabaseUtils.class);

//	protected static DataSource dataSource = null;

//	/**
//	 * Instala la datasource que sera utilizada por todos los managers JDBC
//	 * 
//	 * @param newDataSource
//	 */
//	public static void setDataSource(DataSource newDataSource) {
//		if (newDataSource == null) {
//			log.error("Inicializanfdo la DataSource con un valor NULO.");
//		} else {
//			log
//					.info("Inicializanfdo la DataSource con '" + newDataSource
//							+ "'");
//			dataSource = newDataSource;
//		}
//	}

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

//	public static boolean testConnection() throws Exception{
//		if (dataSource==null){
//			throw new DaoManagerException("datasource is null");
//		} else {
//			try {
//				Connection connection = dataSource.getConnection();
//				String testQuery="SELECT 1 from DUAL";
//				try {
//					PreparedStatement pstm = connection.prepareStatement(testQuery);
//					
//					pstm.executeQuery(testQuery);
//					return true;
//					
//				} finally {
//					connection.close();
//				}	
//			} catch (Exception e) {
//				throw new DaoManagerException("Connection test error", e);
//			}
//		}
//	}

}
