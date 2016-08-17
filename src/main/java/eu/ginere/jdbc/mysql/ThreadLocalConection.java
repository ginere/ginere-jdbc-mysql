package eu.ginere.jdbc.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import eu.ginere.base.util.dao.DaoManagerException;

public class ThreadLocalConection {
	static final Logger log = Logger.getLogger(ThreadLocalConection.class);
	
	public static boolean GLOBAL_USE_THREADLOCAL_CONECTION=false;

	/**
	 * The ThreadLocal Object 
	 */
	private static ThreadLocal<ThreadLocalConection> threadLocalConection=new ThreadLocal<ThreadLocalConection>();


	/**
	 * Se almacenan los datos para cada database a las que se conectan. 
	 * 
	 * @author ventura
	 */
	private class DataBaseStuff {
		MySQLDataBase database=null;
		Connection connection=null;
		Hashtable<String, PreparedStatement> statementCached=null;

		public DataBaseStuff(MySQLDataBase database){
			this.database=database;
		}

		Connection getConnection() throws DaoManagerException{
			if (connection==null){
				connection=getConnectionInner(database.getDataSource());
				cleared=false;
			}

			return connection;
		}	
		
		public void clean() {
			log.warn("Cleaning threadlocal conextion thread:"+Thread.currentThread().getName());
			closeInner(connection);
			connection=null;
			statementCached=null;
		}
		
		private Hashtable<String, PreparedStatement> getStatementCachedThreadLocal() {
			if (statementCached == null){
				statementCached=new Hashtable<String, PreparedStatement>();
			}

			return statementCached;
		}
	}

	boolean useThreadLocal=false;
	boolean cleared=true;
	private final Hashtable<String, DataBaseStuff> dataBaseCache=new Hashtable<String, DataBaseStuff>();
	
	public ThreadLocalConection(){			
	}

	private static ThreadLocalConection getThreadlocal(){
		ThreadLocalConection ret= threadLocalConection.get();

		if (ret==null){
			log.warn("Starting thread local at thread:"+Thread.currentThread().getName());
			ret = new ThreadLocalConection();
			threadLocalConection.set(ret);
		}

		return ret;
	}
	
	/**
	 * Returns the stuff related to this database or create a new one
	 * if it does not exists
	 * 
	 * @param dataBase
	 * @return
	 */
	private DataBaseStuff getDatabase(MySQLDataBase dataBase) {
		DataBaseStuff ret=dataBaseCache.get(dataBase.getName());
		
		if (ret==null){
			ret=new DataBaseStuff(dataBase);
			dataBaseCache.put(dataBase.getName(), ret);
		}
		
		return ret;
	}

	public void startThreadLocalPrivate() {
		useThreadLocal=true;
	}

	public void endThreadLocalPrivate(boolean forzeClear) {
		useThreadLocal=false;
		if (forzeClear){
			clean();
		}
	}

	static private boolean useThreadLocal(){
		if (GLOBAL_USE_THREADLOCAL_CONECTION){
			return true;
		} else {
			return getThreadlocal().useThreadLocal;
		}
	}

	public void clean() {
		if (!cleared){
			long time = 0;

			if (log.isInfoEnabled()) {
				time = System.currentTimeMillis();
			}
			
			for (Map.Entry<String,DataBaseStuff>entry:dataBaseCache.entrySet()){
				DataBaseStuff value=entry.getValue();
				value.clean();
			}
			dataBaseCache.clear();
			
			if (log.isInfoEnabled()) {
				log.info("Thread local cleared in:"
							 + (System.currentTimeMillis() - time) + " mill");
			}
			cleared=true;
		}
        /*
		if (!cleared){
			long time = 0;

			if (log.isInfoEnabled()) {
				time = System.currentTimeMillis();
			}

			closeInner(connection);
			statementCached=null;
			
			if (log.isInfoEnabled()) {
				log.info("Thread local cleared in:"
							 + (System.currentTimeMillis() - time) + " mill");
			}
			cleared=true;
		}
        */
	}

	private Connection getConnectionPrivate(MySQLDataBase dataBase) throws DaoManagerException{        
        /*
		if (connection==null){
			connection=getConnectionInner(datasource);
			cleared=false;
		}

		return connection;
        */
		DataBaseStuff database=getDatabase(dataBase);
		
		return database.getConnection();

	}

	public void closePrivate(Connection connection) {
		return;
	}

	public void closePrivate(PreparedStatement pstm) {
		return;
//		// Si hay muchas queries no es puede mantener abiertos los PSTM
//		closeInner(pstm);
	}
	
    /*
	public void closePrivate(ResultSet rset) {
		return;
	}
    */

	private Hashtable<String, PreparedStatement> getStatementCachedThreadLocal(MySQLDataBase dataBase) {
        /*
		if (statementCached == null){
			statementCached=new Hashtable<String, PreparedStatement>();
		}

		return statementCached;
        */
		DataBaseStuff database=getDatabase(dataBase);
		return database.getStatementCachedThreadLocal();
	}

	private PreparedStatement getPrepareStatementPrivate(MySQLDataBase dataBase,
														 Connection connection, 
														 String query) throws DaoManagerException {
        /*
		Hashtable<String, PreparedStatement> statementCached=getStatementCachedThreadLocal();

		PreparedStatement ret=statementCached.get(query);
		
		if (ret==null){
			ret=getPrepareStatementInner(connection,query);
			statementCached.put(query,ret);
		}
				
		return ret;				
        */
		Hashtable<String, PreparedStatement> statementCached=getStatementCachedThreadLocal(dataBase);

		PreparedStatement ret=statementCached.get(query);
		
		if (ret==null){
			ret=getPrepareStatementInner(connection,query);
			statementCached.put(query,ret);
		}
				
		return ret;	
	}


	/**
	 * 
	 * Inner stuff: Here we do the jdb actions
	 *  
	 */
	static private Connection getConnectionInner(DataSource dataSource) throws DaoManagerException {
		if (dataSource == null) {
			throw new DaoManagerException("La datasource es null, lo mas probable es que no se haya inicializado correctamente");
		} else {
			try {
				long time = 0;
					
				if (log.isInfoEnabled()) {
					time = System.currentTimeMillis();
				}

				Connection ret = dataSource.getConnection();

				// Some probles in XA drivers
				ret.setAutoCommit(true);
				// ret.setAutoCommit(true);
                
				if (log.isInfoEnabled()) {
					log.info("Connection obtained in:"
							 + (System.currentTimeMillis() - time) + " mill");
				}
				
				return ret;
			} catch (SQLException e) {
				throw new DaoManagerException("Obteniendo la conexion", e);
			}
		}
	}
	
	static protected void closeInner(Connection connection) {
		try {
			long time = 0;
			
			if (log.isInfoEnabled()) {
                time = System.currentTimeMillis();
			}
            
			connection.close();
			
			if (log.isInfoEnabled()) {
				log.info("Connection Closed in:"
						 + (System.currentTimeMillis() - time) + " mill");
			}
		} catch (SQLException e) {
			log.warn("Clossing conection", e);
		}
	}

	static protected void closeInner(PreparedStatement pstm) {
		try {
			pstm.close();
		} catch (SQLException e) {
			log.warn("Clossing PreparedStatement", e);
		}
	}

    /*
	static protected void closeInner(ResultSet rset) {
		try {
			rset.close();
		} catch (SQLException e) {
			log.warn("Clossing ResultSet", e);
		}
    }
    */
	
	private static PreparedStatement getPrepareStatementInner(Connection connection,String query) throws DaoManagerException {
		try {
			return connection.prepareStatement(query);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'" + query	+ "'", e);
		}
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


	/**
	 *
	 * Public stuff
	 * 
	 */
	static public Connection getConnection(MySQLDataBase database) throws DaoManagerException{
		if (useThreadLocal()){
			return getThreadlocal().getConnectionPrivate(database);
		} else {
			getThreadlocal().clean();
            //			return getConnectionInner(dataSource);
			return getConnectionInner(database.getDataSource());
		}
	}

	protected static PreparedStatement getPrepareStatement(MySQLDataBase dataBase,Connection connection,String query) throws DaoManagerException {
		if (useThreadLocal()){
			return getThreadlocal().getPrepareStatementPrivate(dataBase,connection,query);
		} else {
			return getPrepareStatementInner(connection,query);
		}
	}


	static public void close(Connection connection){
		if (useThreadLocal()){
			getThreadlocal().closePrivate(connection);
		} else {
			closeInner(connection);
		}
	}

	static public void close(PreparedStatement pstm){
		if (useThreadLocal()){
			getThreadlocal().closePrivate(pstm);
		} else {
			closeInner(pstm);
		}
	}

    /*
	static public void close(ResultSet rset){
		if (useThreadLocal()){
			getThreadlocal().closePrivate(rset);
		} else {
			closeInner(rset);
		}
	}
    */

	public static void startThreadLocal() {
		getThreadlocal().startThreadLocalPrivate();
	}


	public static void endThreadLocal(boolean forzeClear) {
		getThreadlocal().endThreadLocalPrivate(forzeClear);
	}



}

