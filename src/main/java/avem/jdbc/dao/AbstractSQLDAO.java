package avem.jdbc.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import avem.common.util.dao.DaoManagerException;
import avem.common.util.dao.KeyDTO;
import avem.common.util.enumeration.SQLEnum;
import avem.common.util.file.FileId;
import avem.common.util.i18n.I18NLabel;
import avem.jdbc.JdbcManager;
import avem.jdbc.backend.BackEndInterface;
import avem.jdbc.backend.BackendInfo;
import avem.jdbc.backend.BackendManager;


/**
 * Clase Madre para todos los datos de insercion en base de datos
 *
 */
public abstract class AbstractSQLDAO extends JdbcManager implements BackEndInterface {
	static final Logger log = Logger.getLogger(AbstractSQLDAO.class);

	// public final BackendInfo backendInfo;
	private final String createQueryArray[][];
	private final String deleteQueryArray[];
	private final String tableName;
	
	private final String COUNT ;

	protected final String DELETE_ALL_QUERY;

	//	public static boolean GLOBAL_USE_THREADLOCAL_CONECTION=false;
	//	private static ThreadLocal<ThreadLocalConection> connection=new ThreadLocal<ThreadLocalConection>();
	
//	private static ThreadLocal<Hashtable<String, PreparedStatement> > statementCachedThreadLocal=new ThreadLocal<Hashtable<String, PreparedStatement> >();
//	private final static Hashtable<String, PreparedStatement> statementCached=new Hashtable<String, PreparedStatement>();

	protected AbstractSQLDAO(String tableName,String createQueryArray[][],String deleteQueryArray[]) {
		this.createQueryArray=createQueryArray;
		this.tableName=tableName;
		
		this.COUNT = "select COUNT(*) from " + tableName+ " limit 1";
		this.deleteQueryArray=deleteQueryArray;
		
		this.DELETE_ALL_QUERY="DELETE from " + tableName ;
		
		BackendManager.subscrive(this);
	}
	
	protected AbstractSQLDAO(String tableName,String createQueryArray[][]) {
		this.createQueryArray=createQueryArray;
		this.tableName=tableName;
		
		this.COUNT = "select COUNT(*) from " + tableName + " limit 1";
		this.DELETE_ALL_QUERY="DELETE from " + tableName ;
		this.deleteQueryArray=null;
		
		BackendManager.subscrive(this);
	}
	
	protected void createIndexes(String indexes[][]){
		if (indexes == null){
			log.error("No indexes");
		} else {
			try {
				Connection connection = getConnection();
				try {
					for (int i =0;i<indexes.length;i++){
						String array[]=indexes[i];
						if (array!=null && array.length >=2){
							String indexName=array[0];
							String indexQuery=array[1];
							
							try {
								log.info("Creating index:"+indexName+" ....");
								PreparedStatement pstm = getPrepareStatement(connection,
															indexQuery);
			
								executeUpdate(pstm, indexQuery);
								log.info("Index:"+indexName+" CREATED.");
							}catch (DaoManagerException e) {
								log.error("While creating Index:"+indexName,e);
							}
						}
					}
				}finally{
					closeConnection(connection);
				}
			}catch (DaoManagerException e) {
				log.error("While geting connection to create indexes",e);
			}
		}
	}
	
	protected void dropIndexes(String indexes[][]){
		if (indexes == null){
			log.error("No indexes");
		} else {
			try {
				Connection connection = getConnection();
				try {
					for (int i =0;i<indexes.length;i++){
						String array[]=indexes[i];
						if (array!=null && array.length >=2){
							String indexName=array[0];
							String indexQuery="ALTER TABLE "+tableName+" DROP INDEX "+indexName;
							
							try {
								log.info("Dropping index:"+indexName+" ....");
								PreparedStatement pstm = getPrepareStatement(connection,
															indexQuery);
			
								executeUpdate(pstm, indexQuery);
								log.info("Index:"+indexName+" DROPED.");
							}catch (DaoManagerException e) {
								log.error("While dropping Index:"+indexName,e);
							}
						}
					}
				}finally{
					closeConnection(connection);
				}
			}catch (DaoManagerException e) {
				log.error("While geting connection to drop indexes",e);
			}
		}
	}
	
	protected static String getQueryForDeleteFromOneColumn(String tableName,
														String columnName) {
		return "delete  from " + tableName
		+ " where "+columnName+"=? ";
	}
	
	public void deleteFromOneColmunQuery(String query,String value)throws DaoManagerException{
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = getPrepareStatement(connection,
														 query);
			setString(pstm, 1, value, query);
			executeUpdate(pstm, query);

		} catch (DaoManagerException e) {
			String error = "query:"+query;
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}
	
	public void deleteAll()throws DaoManagerException{
		Connection connection = getConnection();
		String query=DELETE_ALL_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection,
														 query);
			
			executeUpdate(pstm, query);

		} catch (DaoManagerException e) {
			String error = "query:"+query;
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}
	
	@Override
	public int getCodeVersion() {
		if (createQueryArray==null){
			return 0;
		} else {
			return createQueryArray.length;
		}
	}

	@Override
	public int getInstalledVersion() {
		BackendInfo backendInfo=BackendManager.getInfo(getClass());
		return backendInfo.getVersion();
	}

	/**
	 * Test if the backend is working fine
	 *
	 * @return Return one humah readeable string 
	 */
	@Override
	public boolean isBackendOk() {
		return (getInstalledVersion()==getCodeVersion());
	}

	@Override
	public void createorUpdateBackEnd() throws DaoManagerException{
		if (getCodeVersion()>getInstalledVersion()){
			for (int i=getInstalledVersion()+1;i<=getCodeVersion();i++){
				try {
					upgradeBackendToVersion(i);
					BackendManager.setCurrentVersion(getClass(),i);
				}catch(DaoManagerException e){
					log.error("While upgrading :'"+this.getClass().getName()+"' to version:"+i,e);
					throw new DaoManagerException("While upgrading :'"+this.getClass().getName()+"' to version:"+i,e);
				}
			}
		} else if (getCodeVersion() == 0){
			// Por ejemplo un backend que ya no tiene scripts de creacion, borramos la version en base de datos
			BackendManager.delete(getClass());
		} else {
			log.warn("For backend:'"+getClass()+"' the code version:"+getCodeVersion()+" is lower that the DB installed version:"+getInstalledVersion());
			
		}
	}

	/**
	 * Upgade or crete the backen. If version <0 this creates the backend
	 */
	private void upgradeBackendToVersion(int version) throws DaoManagerException{				
		if (createQueryArray==null){
			return ;
		}
		Connection connection = getConnection();
		try {
			String querys[]=createQueryArray[version-1];
			for (String query:querys){
				try {
					PreparedStatement pstm = getPrepareStatement(connection,
																 query);
		
					executeUpdate(pstm, query);
				} catch (DaoManagerException e) {
					throw new DaoManagerException(query, e);
				}
			}

		} finally {
			closeConnection(connection);
		}
	}
	
	/**
	 * Upgade or crete the backen. If version <0 this creates the backend
	 */
	protected synchronized void truncate() throws DaoManagerException{
		Connection connection = getConnection();
		String query="TRUNCATE TABLE "+tableName;
		
		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				if (log.isInfoEnabled()){
					log.info("Starting truncate Table:"+tableName+".");
				}
				
				executeUpdate(pstm, query);
				log.error("Table:"+tableName+" truncated");
			}finally{
				close(pstm);
			}
		} catch (DaoManagerException e) {
			String error = "Truncate table:'"+tableName+"'";
			
			log.error(error, e);
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
		
	}
//	protected static long getNextValueFromSecuence(String sequenceName)
//			throws DaoManagerException {
//		long time = 0;
//		if (log.isInfoEnabled()) {
//			time = System.currentTimeMillis();
//		}
//
//		StringBuilder buffer = new StringBuilder();
//
//		buffer.append("SELECT ");
//		buffer.append(sequenceName);
//		buffer.append(".nextval from dual");
//
//		String query = buffer.toString();
//
//		Connection connection=getConnection();
//			
//		try {
//			PreparedStatement pstm = connection.prepareStatement(query);
//
//			long value = getLongFromQuery(pstm, query, -1);
//			if (value < 0) {
//				throw new DaoManagerException(
//						"No se pudo obtener un valor de la secuencia:'"
//								+ sequenceName + "'");
//			} else {
//				return value;
//			}
//		} catch (SQLException e) {
//			throw new DaoManagerException("While executing query:'" + query
//					+ "' for secuence:'" + sequenceName + "'", e);
//		} finally {
//			closeConnection(connection);
//			if (log.isInfoEnabled()) {
//				log.info("query:'" + query + "' executed in:"
//						+ (System.currentTimeMillis() - time) + " mill");
//			}
//		}
//	}
//
//	protected static long getNextValueFromSecuence(String sequenceName)throws DaoManagerException {
//		Connection connection=getConnection();
//			
//		try {
//			return getNextValueFromSecuence(connection,sequenceName);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//
//
//	protected static long getNextValueFromSecuence(Connection connection,String sequenceName)throws DaoManagerException {
//		long time = 0;
//		if (log.isInfoEnabled()) {
//			time = System.currentTimeMillis();
//		}
//
//		StringBuilder buffer = new StringBuilder();
//
//		buffer.append("SELECT ");
//		buffer.append(sequenceName);
//		buffer.append(".nextval from dual");
//
//		String query = buffer.toString();
//
//		try {
//			PreparedStatement pstm = connection.prepareStatement(query);
//			try {
//				long value = getLongFromQuery(pstm, query, -1);
//				if (value < 0) {
//					throw new DaoManagerException(
//												  "No se pudo obtener un valor de la secuencia:'"
//												  + sequenceName + "'");
//				} else {
//					return value;
//				}
//			}finally{
//				pstm.close();
//			}
//		} catch (SQLException e) {
//			throw new DaoManagerException("While executing query:'" + query
//					+ "' for secuence:'" + sequenceName + "'", e);
//		} finally {
////          Eliminamos el comentario porque el log ya lo hace la funcion que ejecuta la query 
////			if (log.isInfoEnabled()) {
////				log.info("query:'" + query + "' executed in:"
////						+ (System.currentTimeMillis() - time) + " mill");
////			}
//		}
//	}

	protected static int getIntFromQuery(PreparedStatement pstm, 
										 String query,
										 int defaultValue) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}
		try {
			ResultSet rset = pstm.executeQuery();
			try {
				if (!rset.next()) {
					return defaultValue;
				} else {
					int ret= rset.getInt(1);
					if (rset.wasNull()){
						return defaultValue;
					} else {
						return ret;
					}
				}
			}finally{
				rset.close();
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
	

	protected static long getLongFromQuery(PreparedStatement pstm, 
										 String query,
										 long defaultValue) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}
		try {
			ResultSet rset = pstm.executeQuery();
			try {
				if (!rset.next()) {
					return defaultValue;
				} else {
					long ret =rset.getLong(1);
					if (rset.wasNull()){
						return defaultValue;
					} else {
						return ret;
					}
				}
			}finally{
				rset.close();
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
	
	protected static String getStringFromQuery(PreparedStatement pstm, String query,
			String defaultValue) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}
		try {
			ResultSet rset = pstm.executeQuery();
			if (!rset.next()) {
				return defaultValue;
			} else {
				String ret= rset.getString(1);
				if (rset.wasNull()){
					return defaultValue;
				} else {
					return ret;
				}
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
	
	
	public boolean hasNextForOneColmunQuery(String query,String value) throws DaoManagerException {		
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);

			setString(pstm, 1, value, query);

			return hasNext(pstm, query);
		} catch (DaoManagerException e) {
			String error = "query:'"+query+"' id:'" + value + "'";

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

			return rset.next();
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

	protected static List<String> getStringList(PreparedStatement pstm,
			String query) throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}

		try {
			ResultSet rset = pstm.executeQuery();
			List<String> ret = new ArrayList<String>(rset.getFetchSize());

			while (rset.next()) {
				ret.add(rset.getString(1));
			}
			return ret;
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

	protected static ResultSet executeQuery(PreparedStatement pstm, String query)
			throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}

		try {

			return pstm.executeQuery();

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

	protected static int executeUpdate(PreparedStatement pstm, String query)
			throws DaoManagerException {
		long time = 0;
		if (log.isInfoEnabled()) {
			time = System.currentTimeMillis();
		}

		try {

			int number = pstm.executeUpdate();

			return number;
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

	static protected Connection getConnection() throws DaoManagerException {
		return ThreadLocalConection.getConnection(dataSource);
		
//		if (ThreadLocalConection.useThreadLocal){
//			return ThreadLocalConection.getConnection()  getThreadLocalConnection();
//		} else {
//			if (dataSource == null) {
//				throw new DaoManagerException("La datasource es null, lo mas probable es que no se haya inicializado correctamente");
//			} else {
//				try {
//					long time = 0;
//					
//					if (log.isInfoEnabled()) {
//						time = System.currentTimeMillis();
//					}
//					Connection ret = dataSource.getConnection();
//					ret.setAutoCommit(true);
//					if (log.isInfoEnabled()) {
//						log.info("Connection obtained in:"
//								 + (System.currentTimeMillis() - time) + " mill");
//					}
//					
//					return ret;
//				} catch (SQLException e) {
//					throw new DaoManagerException("Obteniendo la conexion", e);
//				}
//			}
//		}
	}


//	static private Connection getThreadLocalConnection() throws DaoManagerException {
//		if (!GLOBAL_USE_THREADLOCAL_CONECTION){
//			return null;
//		} else {
//			Connection ret=connection.get();
//			
//			if (ret==null){
//				if (dataSource == null) {
//					throw new DaoManagerException("La datasource es null, lo mas probable es que no se haya inicializado correctamente");
//				} else {
//					try {
//						log.info("Genrating new threadLocal connection for thread Thread:"+Thread.currentThread().getId());
//						ret = dataSource.getConnection();
//						ret.setAutoCommit(true);
//						connection.set(ret);
//					}catch (SQLException e) {
//						throw new DaoManagerException("For thread:"+Thread.currentThread().getId(),e);
//					}
//				} 						
//			}
//			return ret;
//		}
//	}

	static protected void closeConnection(Connection connection) {
		ThreadLocalConection.close(connection);
////		if (GLOBAL_USE_THREADLOCAL_CONECTION){
////			return ;
////		} else {
//			try {
//				long time = 0;
//				
//				if (log.isInfoEnabled()) {
//					time = System.currentTimeMillis();
//				}
//				connection.close();
//				
//				if (log.isInfoEnabled()) {
//					log.info("Connection Closed in:"
//							 + (System.currentTimeMillis() - time) + " mill");
//				}
//			} catch (SQLException e) {
//				log.warn("Clossing conection", e);
//			}
////		}
	}
	
	static protected void close(PreparedStatement pstm) {
		ThreadLocalConection.close(pstm);
////		if (GLOBAL_USE_THREADLOCAL_CONECTION){
////			return ;
////		} else {
//			try {
//				pstm.close();
//			} catch (SQLException e) {
//				log.warn("Clossing conection", e);
//			}
////		}
	}
	
	static protected void close(ResultSet rset) {
		ThreadLocalConection.close(rset);
////		if (GLOBAL_USE_THREADLOCAL_CONECTION){
////			return ;
////		} else {
//			try {
//				pstm.close();
//			} catch (SQLException e) {
//				log.warn("Clossing conection", e);
//			}
////		}
	}
	
//	private static Hashtable<String, PreparedStatement> getStatementCachedThreadLocal(){
//		Hashtable<String, PreparedStatement> ret=statementCachedThreadLocal.get();
//		
//		if (ret==null){
//			ret = new Hashtable<String, PreparedStatement>();
//			statementCachedThreadLocal.set(ret);
//		}
//		
//		return ret;
//	}
	
	protected static PreparedStatement getPrepareStatement(Connection connection,String query) throws DaoManagerException {
//		try {
////			if (GLOBAL_USE_THREADLOCAL_CONECTION){
////				Hashtable<String, PreparedStatement> statementCached=getStatementCachedThreadLocal();
////				
////				PreparedStatement ret=statementCached.get(query);
////			
////				if (ret==null){
////					try {
////						ret=connection.prepareStatement(query);
////						statementCached.put(query,ret);
////					} catch (SQLException e) {
////						throw new DaoManagerException("For thread:"+Thread.currentThread().getId()+" Query:'" + query	+ "'", e);
////					}
////				}
////				
////				return ret;				
////			} else {			
//				return connection.prepareStatement(query);
////			}
//		} catch (SQLException e) {
//			throw new DaoManagerException("Query:'" + query	+ "'", e);
//		}
		return ThreadLocalConection.getPrepareStatement(connection, query);
	}

	protected static void set(PreparedStatement pstm,int poss,Object value,String query) throws DaoManagerException {

		if (value instanceof String || value == null){
			setString(pstm,poss,(String)value,query);
		} else if (value instanceof Date){
			setDate(pstm,poss,(Date)value,query);
		} else if (value instanceof SQLEnum){
			setSQLEnum(pstm,poss,(SQLEnum)value,query);
		} else if (value instanceof Enum){
			setEnum(pstm,poss,(Enum)value,query);
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
		} else if (value instanceof FileId){
			setFileId(pstm,poss,(FileId)value,query);
		} else {
			throw new IllegalAccessError("Type :"+value.getClass().getName()+" is not suported");
		}
	}	
	protected static void setString(PreparedStatement pstm,int poss,String value,String query) throws DaoManagerException {
		try {
			pstm.setString(poss,value);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	protected static void setKeyDTO(PreparedStatement pstm,int poss,KeyDTO dto,String query) throws DaoManagerException {
		try {
			if (dto!=null){
				pstm.setString(poss,dto.getId());
			} else {
				pstm.setNull(poss, Types.VARCHAR);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + dto	+ "'", e);
		}
	}
	
	protected static void setI18NLabel(PreparedStatement pstm,int poss,I18NLabel label,String query) throws DaoManagerException {
		try {
			if (label!=null){
				pstm.setString(poss,label.getId());
			} else {
				pstm.setNull(poss, Types.VARCHAR);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + label	+ "'", e);
		}
	}
	
	protected static void setFileId(PreparedStatement pstm,int poss,FileId value,String query) throws DaoManagerException {
		try {
			
			if (value != null) {
				pstm.setString(poss,value.getId());
			} else {
				pstm.setNull(poss, Types.VARCHAR);
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	protected static void setEnum(PreparedStatement pstm,int poss,Enum value,String query) throws DaoManagerException {
		try {
			pstm.setString(poss,value.toString());
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}

	protected static void setSQLEnum(PreparedStatement pstm,int poss,SQLEnum value,String query) throws DaoManagerException {
		try {
			if (value==null){
				pstm.setNull(poss, Types.INTEGER);
			} else {
				pstm.setString(poss,value.getId());
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	protected static void setInt(PreparedStatement pstm,int poss,int value,String query) throws DaoManagerException {
		try {
			pstm.setInt(poss,value);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	protected static void setShort(PreparedStatement pstm,int poss,short value,String query) throws DaoManagerException {
		try {
			pstm.setShort(poss,value);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	
	protected static void setInt(PreparedStatement pstm,int poss,Integer value,String query) throws DaoManagerException {
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
	
	protected static void setFloat(PreparedStatement pstm,int poss,Float value,String query) throws DaoManagerException {
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
	
	protected static void setDate(PreparedStatement pstm,int poss,Date value,String query) throws DaoManagerException {
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
	
	protected static void setTimestamp(PreparedStatement pstm,int poss,Timestamp value,String query) throws DaoManagerException {
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

	protected static void setLong(PreparedStatement pstm, int poss, Long value,String query) throws DaoManagerException {
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
	
	protected static void setDouble(PreparedStatement pstm, int poss, Double value,String query) throws DaoManagerException {
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
	
	protected static void setByte(PreparedStatement pstm, int poss, byte value,String query) throws DaoManagerException {
		try {
			pstm.setByte(poss,value);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'" + value	+ "'", e);
		}
	}
	
	protected static void setBoolean(PreparedStatement pstm, int poss, Boolean value,String query) throws DaoManagerException {
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

	protected static void setNull(PreparedStatement pstm, int poss, String query) throws DaoManagerException {
		try {
			pstm.setNull(poss, Types.VARCHAR);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' Value:'NULL'", e);
		}
	}
	public static boolean testConnection() {
		try {
			Connection connection = getConnection();
			String testQuery="SELECT 1 from DUAL";
			try {
				PreparedStatement pstm = getPrepareStatement(connection,
						testQuery);
	
				executeQuery(pstm, testQuery);
				return true;
	
			} finally {
				closeConnection(connection);
			}	
		} catch (Exception e) {
			log.error("Connection test error", e);
			return false;
		}
	}
	
	protected static String appendTablenameToColumnName(String tableName,String columnsArray[]){
		StringBuilder builder=new StringBuilder();
		
		for (int i=0;i<columnsArray.length;i++){
//			Si hacemos una selet de varias tablas poner el nombre de la tabla no funciona mut bien en jdbc
			builder.append(tableName);
			builder.append('.');
			builder.append(columnsArray[i]);
			if (i<columnsArray.length-1){
				builder.append(",");
			}
		}

		return builder.toString();
	}

	//
	// Ejecutando los metodos quet
	//

	public static String getString(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getString(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}

	/**
	 * If the result is null this retuns EMPTY_STRING_ARRAY
	 */
	public static int getInt(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getInt(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}
	
	public static short getShort(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getShort(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}
	

	public static byte getByte(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getByte(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}
	
	
	public static Timestamp getTimestamp(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getTimestamp(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}

	public static Date getDate(ResultSet rset, String columnName,String query) throws DaoManagerException {
		return (Date)getTimestamp(rset,columnName,query);
	}
	
	public static boolean getBoolean(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getBoolean(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}

	public static long getLong(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getLong(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}
	
	public static Long getLongObject(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			Long ret=rset.getLong(columnName);
			if (rset.wasNull()){
				return null;
			} else {
				return ret;
			}
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}


	public static double getDouble(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getDouble(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}

	public static float getFloat(ResultSet rset, String columnName,String query) throws DaoManagerException {
		try {
			return rset.getFloat(columnName);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'"+query+"' columnName:'" + columnName	+ "'", e);
		}
	}
	
//	public static SQLEnum getSQLEnum(ResultSet rset,Class clazz,String columnName,String query) throws DaoManagerException{
//		return SQLEnum.value(clazz, getString(rset,columnName,query));
//	}

	/**
	 * DEvuelve el nombre de la tabla
	 * @return
	 */
	public String getTableName(){
		return tableName;
	}

	public long getBackendElementNumber() {
		try {
			Connection connection = getConnection();
			String query=COUNT;
	
			try {
				PreparedStatement pstm = getPrepareStatement(connection,query);
	
				return getLongFromQuery(pstm, query, 0);
			} catch (DaoManagerException e) {
	
				throw new DaoManagerException("Count", e);
			} finally {
				closeConnection(connection);
			}
		}catch (DaoManagerException e) {
			log.info("",e);
			return 0;
		}
	}

	public void deleteBackEnd() throws DaoManagerException{
//		if (createQueryArray!=null){
//			dropTable(tableName);
//		} else {
//			log.info("The backend:"+getClass().getName()+" not deleted becaus it don not have creation queries" );
//		}
	
	
	
		if (deleteQueryArray==null){
			dropTable(tableName);
			log.info("Not delete query , the deleting table:'"+tableName+"' for backend:"+getClass().getName()+"." );
		} else {
			Connection connection = getConnection();
			try {
				for (String query:deleteQueryArray){
					try {
						PreparedStatement pstm = getPrepareStatement(connection,
																	 query);
			
						executeUpdate(pstm, query);
					} catch (DaoManagerException e) {
						throw new DaoManagerException(query, e);
					}
				}
		
			} finally {
				closeConnection(connection);
			}
		}

		try {
			BackendManager.setCurrentVersion(getClass(),0);
		}catch(DaoManagerException e){
			log.error("While upgrading :'"+this.getClass().getName()+"' to version:"+0,e);
			throw new DaoManagerException("While upgrading :'"+this.getClass().getName()+"' to version:"+0,e);
		}
	
	}

	protected static void dropTable(String tableName) throws DaoManagerException{
		Connection connection = getConnection();
		String query="DROP TABLE "+tableName;
		try {
			try {
				PreparedStatement pstm = getPrepareStatement(connection,
															 query);
				
				executeUpdate(pstm, query);
			} catch (DaoManagerException e) {
				throw new DaoManagerException(query, e);
			}
		} finally {
			closeConnection(connection);
		}
	}
	
	static public void startThreadLocal() {
		ThreadLocalConection.startThreadLocal();
	}

	static public void endThreadLocal(boolean forzeClean) {
		ThreadLocalConection.endThreadLocal(forzeClean);
	}

}
