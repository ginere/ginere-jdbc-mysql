package eu.ginere.jdbc.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.test.TestInterface;
import eu.ginere.base.util.test.TestResult;

/**
 * 
 */
public class MySQLDataBase implements TestInterface{

	protected static final Logger log = Logger.getLogger(MySQLDataBase.class);

	public static MySQLDataBase DEFAULT_DATABASE=null;

	private DataSource dataSource;

	private final String name;

	public MySQLDataBase(String jndiPath,String jndiName) throws NamingException{
		this.name=jndiName;
		log.info("Getting the datasource from the jndi ressource:"+jndiName+"'");
		InitialContext initContext = new InitialContext();
		this.dataSource = (DataSource) initContext.lookup(jndiPath+jndiName);		
		log.info("Database sucesfully initialized from jndi ressource jndi:"+jndiName+"'");		
	}

	public MySQLDataBase(String jndiName) throws NamingException{
		this.name=jndiName;
		log.info("Getting the datasource from the jndi ressource:"+jndiName+"'");
		InitialContext initContext = new InitialContext();
		this.dataSource = (DataSource) initContext.lookup("java:comp/env/"+jndiName);		
		log.info("Database sucesfully initialized from jndi ressource jndi:"+jndiName+"'");		
	}
	
	public MySQLDataBase(String name,DataSource datasource) throws NamingException{
		this.name=name;
		this.dataSource = datasource;
		log.info("Sacesfully actualized datasource:"+name+"'");		
	}
	
	public String getName() {
		return name;
	}

	public DataSource getDataSource(){
		return dataSource;
	}
	
	/**
	 * @param jndiName
	 * @throws NamingException
	 */
	public static void initDatasource(String jndiName) throws NamingException {
		DEFAULT_DATABASE=new MySQLDataBase(jndiName);
//		log.info("Obteniendo la datasource del recurso jndi:"+jndiName+"'");
//		InitialContext initContext = new InitialContext();
//		this.dataSource = (DataSource) initContext.lookup("java:comp/env/"+jndiName);		
//		log.info("Se ha actualizado satisfactoriamente la DataSource del recurso jndi:"+jndiName+"'");
	}

	/**
	 * @param jndiName
	 * @throws NamingException
	 */
	public static void initDatasource(String jndiPath,String jndiName) throws NamingException {
		DEFAULT_DATABASE=new MySQLDataBase(jndiPath,jndiName);
	}
	public static void initDatasource(String name,DataSource datasource) throws NamingException {
		DEFAULT_DATABASE=new MySQLDataBase(name,datasource);
	}

	public static void initDatasource(MySQLDataBase database) throws NamingException {
		DEFAULT_DATABASE=database;
	}
	
	public boolean testConnection() {
		return testConnection(dataSource);
	}

	/**
	 * Verifica que la conexion es correcta.
	 * 
	 * @param dataSource
	 * @return
	 */
	public boolean testConnection(DataSource dataSource) {
		try {
			Connection connection = getConnection();
			String testQuery="SELECT 1 from DUAL";
			try {
				PreparedStatement pstm = getPrepareStatement(connection,
															 testQuery);
				try {
//					executeQuery(pstm, testQuery);
					hasResult(pstm, testQuery);
					return true;				
				}finally{
					close(pstm);
				}
			} finally {
				closeConnection(connection);
			}	
		} catch (Exception e) {
			log.error("Connection test error", e);
			return false;
		}
	}

	
	/**
	 * Obtien un prepared statemend.
	 * 
	 * @param connection
	 * @param query
	 * @return
	 * @throws DaoManagerException
	 */
	public PreparedStatement getPrepareStatement(Connection connection,String query) throws DaoManagerException {
		return ThreadLocalConection.getPrepareStatement(this,connection, query);
//		try {
//			return connection.prepareStatement(query);
//		} catch (SQLException e) {
//			throw new DaoManagerException("Query:'" + query	+ "'", e);
//		}
	}

	public List<String> getStringList (String query) throws DaoManagerException{
		Connection connection = getConnection();
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				return getStringList(pstm, query);
			}finally{
				close(pstm);
			}
		} finally {
			closeConnection(connection);
		}
	}
	
	
	public static List<String> getStringList(PreparedStatement pstm,
											 String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				List<String> ret = new ArrayList<String>(rset.getFetchSize());
				
				while (rset.next()) {
					ret.add(rset.getString(1));
				}
				return ret;
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally{
				close(rset);
			}
		} finally {			
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	
	
	
	public static HashSet<String> getHashSet(PreparedStatement pstm,
											 String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				HashSet<String> ret = new HashSet<String>(rset.getFetchSize());
				
				while (rset.next()) {
					ret.add(rset.getString(1));
				}
				return ret;
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally{
				close(rset);
			}
		} finally {			
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	
	

	public static String getString(PreparedStatement pstm,
								   String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if(rset.next()) {
					return rset.getString(1);
				} else {
					throw new DaoManagerException("Not result for query:'"+query+"'");
				}
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally{
				close(rset);
			}
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	public static String getString(PreparedStatement pstm,								  
								   String defaultValue,
								   String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if(rset.next()) {
					return rset.getString(1);
				} else {
					return defaultValue;
				}
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally {
				close(rset);
			}
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	public String getString(String query,String defaultValue) throws DaoManagerException {
		Connection connection = getConnection();
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				return getString(pstm, defaultValue,query);
			}finally{
				close(pstm);
			}
		} finally {
			closeConnection(connection);
		}
	}
	


	public static Date getDate(PreparedStatement pstm,
								   String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if(rset.next()) {
					return rset.getTimestamp(1);
				} else {
					throw new DaoManagerException("Not result for query:'"+query+"'");
				}
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally{
				close(rset);
			}
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	
	

	public static Date getDate(PreparedStatement pstm,
								   String query,
								   Date defaultValue) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if(rset.next()) {
					return rset.getTimestamp(1);
				} else {
					return defaultValue;
				}
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally {
				close(rset);
			}
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	
	


	public static boolean hasResult(PreparedStatement pstm,
								   String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				return rset.next();
			}finally{
				close(rset);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("While executing query:'" + query+ "'", e);
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	
	public List<Object[]> getStringListArray (String query,int tamano) throws DaoManagerException{
		Connection connection = getConnection();
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				return getStringListArray(pstm, query,tamano);
			}finally{
				close(pstm);
			}			
		} finally {
			closeConnection(connection);
		}
	}
	
	public static List<Object[]> getStringListArray(PreparedStatement pstm,
													String query,
													int tamano) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}

		try {
			ResultSet rset = executeQuery(pstm, query);
			try {
				List<Object[]> ret = new ArrayList<Object[]>(rset.getFetchSize());
				
				while(rset.next()){
					Object[] objetoResultado =  new Object[tamano];				
					for (int i = 0; i < objetoResultado.length; i++) {
						objetoResultado[i] = rset.getString(i+1);
					}				
					ret.add(objetoResultado);			
				}
				
				return ret;
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query
											  + "'", e);
			}finally{
				close(rset);
			}
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	
	public long getLong(String query) throws DaoManagerException{
		Connection connection = getConnection();
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				return getLong(pstm, query);
			}finally{
				close(pstm);
			}
		} finally {
			closeConnection(connection);
		}
	}
	
	public long getLong(String query,long defaultValue) throws DaoManagerException{
		try {
			Connection connection = getConnection();
			try{
				PreparedStatement pstm = getPrepareStatement(connection,query);
				try {
					return getLong(pstm, query);
				}finally{
					close(pstm);
				}				
			} finally {
				closeConnection(connection);
			}
		}catch (DaoManagerException e) {
			log.debug("query:"+query+"' fails, return default value.",e);
			return defaultValue;
		}
	}
	
	public static long getLong(PreparedStatement pstm,
							   String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if(rset.next()) {
					long ret=rset.getLong(1);
					if (!rset.wasNull()){
						return ret;
					}
				}
				
				throw new DaoManagerException("No result for query:'"+query+"'");
			} catch (SQLException e) {
				throw new DaoManagerException("While executing query:'" + query+ "'", e);
			}finally{
				close(rset);
			}
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}



	/**
	 * Ejecuta una query.
	 * 
	 * @param pstm
	 * @param query
	 * @return
	 * @throws DaoManagerException
	 */
	public long executeUpdate(String query) throws DaoManagerException {
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				return executeUpdate(pstm, query);
			}finally{
				close(pstm);
			}
		} finally {
			closeConnection(connection);
		}	
	}

	/**
	 * Ejecuta una query.
	 * 
	 * @param pstm
	 * @param query
	 * @return
	 * @throws DaoManagerException
	 */
	public static long executeUpdate(PreparedStatement pstm, String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}

		try {
			return pstm.executeUpdate();
		} catch (SQLException e) {
			throw new DaoManagerException("While executing query:'" + query+ "'", e);
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}

	/**
	 * Ejecuta una query.
	 * 
	 * @param pstm
	 * @param query
	 * @return
	 * @throws DaoManagerException
	 */
	public static ResultSet executeQuery(PreparedStatement pstm, String query) throws DaoManagerException {
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}

		try {
			return pstm.executeQuery();
		} catch (SQLException e) {
			throw new DaoManagerException("While executing query:'" + query
					+ "'", e);
		} finally {
			if (log.isDebugEnabled()) {
				log.debug("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}

	/**
	 * Cierra una conexion.
	 * @param connection
	 */
	public static void closeConnection(Connection connection) {
		ThreadLocalConection.close(connection);
//		try {
//			connection.close();
//		} catch (SQLException e) {
//			log.warn("Clossing conection", e);
//		}
	}

	static public void close(PreparedStatement pstm) {
		ThreadLocalConection.close(pstm);
	}
	
	static public void close(ResultSet rset) {
		try {
			rset.close();
		} catch (SQLException e) {
			log.warn("Clossing conection", e);
		}
	}

	public Connection getConnection() throws DaoManagerException {
//		return getConnection(dataSource);
		return ThreadLocalConection.getConnection(this); 
	}


//	/**
//	 * Obtiene una conexion.
//	 * 
//	 * @param dataSource
//	 * @return
//	 * @throws DaoManagerException
//	 */
//	public static Connection getConnection(DataSource dataSource) throws DaoManagerException {
//		if (dataSource == null) {
//			throw new DaoManagerException(
//					"La datasource es null, lo mas probable es que no se haya inicializado correctamente");
//		} else {
//			try {
//				long time = 0;
//
//				if (log.isDebugEnabled()) {
//					time = System.currentTimeMillis();
//				}
//				Connection ret = dataSource.getConnection();
//				ret.setAutoCommit(true);
//				if (log.isDebugEnabled()) {
//					log.debug("Connection obtained in:"
//							+ (System.currentTimeMillis() - time) + " mill");
//				}
//
//				return ret;
//			} catch (SQLException e) {
//				throw new DaoManagerException("Obteniendo la conexion", e);
//			}
//		}
//	}
	
	public long getSequenceNextVal(String sequence) throws DaoManagerException {
		String query="SELECT "+sequence+".nextval from dual";
		
		return getLong(query);
	}

	
	public static void setString(PreparedStatement pstm,int poss,String value,String query) throws DaoManagerException {
		try {
			pstm.setString(poss,value);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}

	public static void setInt(PreparedStatement pstm,int poss,int value,String query) throws DaoManagerException {
		try {
			pstm.setInt(poss,value);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	public static void setInt(PreparedStatement pstm,int poss,Integer value,String query) throws DaoManagerException {
		try {
			if (value==null){
				pstm.setNull(poss, Types.INTEGER);
			} else {
				pstm.setInt(poss,value);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	public static void setFloat(PreparedStatement pstm,int poss,Float value,String query) throws DaoManagerException {
		try {
			if (value!=null){
				pstm.setFloat(poss,value);
			} else {
				pstm.setNull(poss, Types.FLOAT);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	public static void setDate(PreparedStatement pstm,int poss,Date value,String query) throws DaoManagerException {
		try {
			if (value != null) {
				Timestamp sqlDate = new Timestamp(value.getTime());
				pstm.setTimestamp(poss, sqlDate);
			} else {
				pstm.setNull(poss, Types.TIMESTAMP);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	public static void setTimestamp(PreparedStatement pstm,int poss,Timestamp value,String query) throws DaoManagerException {
		try {
			if (value != null) {
				pstm.setTimestamp(poss, value);
			} else {
				pstm.setNull(poss, Types.TIMESTAMP);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}

	public static void setLong(PreparedStatement pstm, int poss, Long value,String query) throws DaoManagerException {
		try {
			if (value!=null){
				pstm.setLong(poss,value);
			} else {
				pstm.setNull(poss, Types.BIGINT);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	public static void setDouble(PreparedStatement pstm, int poss, Double value,String query) throws DaoManagerException {
		try {
			if (value!=null){
				pstm.setDouble(poss,value);
			} else {
				pstm.setNull(poss, Types.DOUBLE);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	public static void setBoolean(PreparedStatement pstm, int poss, Boolean value,String query) throws DaoManagerException {
		try {
			if (value!=null){
				pstm.setBoolean(poss,value);
			} else {
				pstm.setNull(poss, Types.BOOLEAN);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}

	public TestResult test() {
		TestResult ret=new TestResult(MySQLDataBase.class);
		if (dataSource == null){
			ret.addError("The datasource is null");
			return ret;
		} else if (!testConnection()){
			ret.addError("There is not conection to the database:"+getName());
			return ret;
		} else {
			return ret;
		}
	}	
	
	protected long getNextValueFromSecuence(String sequenceName)throws DaoManagerException {
		Connection connection = getConnection();

		StringBuilder buffer = new StringBuilder();

		buffer.append("SELECT ");
		buffer.append(sequenceName);
		buffer.append(".nextval from dual");

		String query = buffer.toString();

		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				long value = getLongFromQuery(pstm, query, -1);
				if (value < 0) {
					throw new DaoManagerException(
												  "No se pudo obtener un valor de la secuencia:'"
												  + sequenceName + "'");
				} else {
					return value;
				}
			}finally{
				close(pstm);
			}
		} finally {
			closeConnection(connection);
		}
	}

	public static long getLongFromQuery(PreparedStatement pstm, 
										   String query,
										   long defaultValue) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if (!rset.next()) {
					return defaultValue;
				} else {
					long ret=rset.getLong(1);
					
					if (rset.wasNull()){
						return defaultValue;
					} else {
						return ret;
					}
				}
				
		
			}finally{
				close(rset);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("While executing query:'" + query
										  + "'", e);
		} finally {
			if (log.isInfoEnabled()) {
				log.info("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}
	

	public static int getIntFromQuery(PreparedStatement pstm, 
										   String query,
										   int defaultValue) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}
		try {
			ResultSet rset = executeQuery(pstm,query);
			try {
				if (!rset.next()) {
					return defaultValue;
				} else {
					int ret=rset.getInt(1);
					
					if (rset.wasNull()){
						return defaultValue;
					} else {
						return ret;
					}
				}				
			}finally{
				close(rset);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("While executing query:'" + query
										  + "'", e);
		} finally {
			if (log.isInfoEnabled()) {
				log.info("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}

	public boolean hasNext(String query) throws DaoManagerException {		
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);

            try {
                return hasNext(pstm, query);
            }finally{
                close(pstm);
            }
		} catch (DaoManagerException e) {
			String error = "query:'"+query+"'";

			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}
	
	/**
	 * Pruebas si la query produce algun resultado, por ejemplo util para saber
	 * si un elemento existe en la tabla
	 * 
	 * @param pstm
	 * @param query
	 * @return true si la query produce algun resultado, false si no.
	 * @throws DaoManagerException
	 */
	protected static boolean hasNext(PreparedStatement pstm, String query) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}
		try {
			ResultSet rset = pstm.executeQuery();
            try {
                return rset.next();
            }finally{
                close(rset);
            }
		} catch (SQLException e) {
			throw new DaoManagerException("While executing query:'" + query
					+ "'", e);
		} finally {
			if (log.isInfoEnabled()) {
				log.info("query:'" + query + "' executed in:"
						+ (System.currentTimeMillis() - time) + " mill");
			}
		}
	}

	protected static void set(PreparedStatement pstm,int poss,Object value,String query) throws DaoManagerException {

		if (value instanceof String || value == null){
			setString(pstm,poss,(String)value,query);
		} else if (value instanceof Date){
			setDate(pstm,poss,(Date)value,query);
		} else if (value instanceof Boolean){
			setBoolean(pstm,poss,(Boolean)value,query);
		} else if (value instanceof Integer){
			setInt(pstm,poss,(Integer)value,query);
		} else if (value instanceof Long){
			setLong(pstm,poss,(Long)value,query);
		} else if (value instanceof Double){
			setDouble(pstm,poss,(Double)value,query);
		} else if (value instanceof Float){
			setFloat(pstm,poss,(Float)value,query);
		} else if (value instanceof Timestamp){
			setTimestamp(pstm,poss,(Timestamp)value,query);
		} else {
			throw new IllegalAccessError("Type :"+value.getClass().getName()+" is not suported");
		}
	}

	public void startThreadLocal(){
		ThreadLocalConection.startThreadLocal();
	}

	static public void endThreadLocal(boolean forzeClean) {
		ThreadLocalConection.endThreadLocal(forzeClean);
	}
	

	private static final String GET_DATA_BASES_QUERY = "show databases";
	
	public List<String> getDatabases() throws DaoManagerException {
		return getStringList(GET_DATA_BASES_QUERY);        
	}

	private static final String GET_SELECTED_DATABASE = "SELECT DATABASE();";
	public String getSelectedDatabase() throws DaoManagerException {
		return getString(GET_SELECTED_DATABASE,null);        
	}

	public boolean exitsDatabase(String databaseName) throws DaoManagerException {
		return hasNext("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '"+databaseName+"'");
    }

	public void createDatabase(String databaseName) throws DaoManagerException {
		createDatabase(databaseName, null, null);
	}
	public void createDatabase(String databaseName,String charset,String collationName) throws DaoManagerException {
        if (databaseName == null){
            throw new DaoManagerException("The database name can not be null");
        } else {
            if (charset == null){
                charset="utf8";
            }
            if (collationName == null){
                collationName="utf8_general_ci";
            }
            try {
            	executeUpdate("create database "+databaseName+" CHAR SET = "+charset+" COLLATE = "+collationName);        
            }catch(DaoManagerException e){
                throw new DaoManagerException("Database:"+databaseName+" charset:"+charset+" collation:"+collationName,e);
            }
        }
	}

	public void dropDatabase(String databaseName) throws DaoManagerException {
        if (databaseName == null){
            throw new DaoManagerException("The database name can not be null");
        } else {
            try {
                executeUpdate("drop database "+databaseName);        
            }catch(DaoManagerException e){
                throw new DaoManagerException("Database:"+databaseName,e);
            }
        }
	}

	public void useDatabase(String databaseName) throws DaoManagerException {
		executeUpdate("USE  "+databaseName+"");
	}

	public void createAndUseDatabase(String databaseName) throws DaoManagerException {
		if (!exitsDatabase(databaseName)){
			createDatabase(databaseName);
		}
		useDatabase(databaseName);		
	}


}
