package avem.jdbc.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import avem.common.util.dao.DaoManagerException;

public class ThreadLocalConection {
	static final Logger log = Logger.getLogger(ThreadLocalConection.class);
	
	public static boolean GLOBAL_USE_THREADLOCAL_CONECTION=false;
	private static ThreadLocal<ThreadLocalConection> threadLocalConection=new ThreadLocal<ThreadLocalConection>();


	Connection connection=null;
	Hashtable<String, PreparedStatement> statementCached=null;
	boolean useThreadLocal=false;
	boolean cleared=true;
	
	public ThreadLocalConection(){
			
	}
	

	private static ThreadLocalConection getThreadlocal(){
		ThreadLocalConection ret= threadLocalConection.get();

		if (ret==null){
			ret = new ThreadLocalConection();
			threadLocalConection.set(ret);
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

	public void clean() {
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
	}

	private Connection getConnectionPrivate(DataSource datasource) throws DaoManagerException{
		if (connection==null){
			connection=getConnectionInner(datasource);
			cleared=false;
		}

		return connection;
	}

	public void closePrivate(Connection connection) {
		return;
	}

	public void closePrivate(PreparedStatement pstm) {
		return;
	}
	
	public void closePrivate(ResultSet rset) {
		return;
	}

	private Hashtable<String, PreparedStatement> getStatementCachedThreadLocal() {
		if (statementCached == null){
			statementCached=new Hashtable<String, PreparedStatement>();
		}

		return statementCached;
	}

	private PreparedStatement getPrepareStatementPrivate(Connection connection, 
														 String query) throws DaoManagerException {
		Hashtable<String, PreparedStatement> statementCached=getStatementCachedThreadLocal();

		PreparedStatement ret=statementCached.get(query);
		
		if (ret==null){
			ret=getPrepareStatementInner(connection,query);
			statementCached.put(query,ret);
		}
				
		return ret;				
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


	static public boolean useThreadLocal(){
		if (GLOBAL_USE_THREADLOCAL_CONECTION){
			return true;
		} else {
			return getThreadlocal().useThreadLocal;
		}
	}

	static public Connection getConnection(DataSource dataSource) throws DaoManagerException{
		if (useThreadLocal()){
			return getThreadlocal().getConnectionPrivate(dataSource);
		} else {
			getThreadlocal().clean();
			return getConnectionInner(dataSource);
		}
	}

	static public void close(Connection connection){
		if (useThreadLocal()){
			getThreadlocal().closePrivate(connection);
		} else {
			closeInner(connection);
		}
	}

	protected static PreparedStatement getPrepareStatement(Connection connection,String query) throws DaoManagerException {
		if (useThreadLocal()){
			return getThreadlocal().getPrepareStatementPrivate(connection,query);
		} else {
			return getPrepareStatementInner(connection,query);
		}
	}

	static public void close(PreparedStatement pstm){
		if (useThreadLocal()){
			getThreadlocal().closePrivate(pstm);
		} else {
			closeInner(pstm);
		}
	}
	
	static public void close(ResultSet rset){
		if (useThreadLocal()){
			getThreadlocal().closePrivate(rset);
		} else {
			closeInner(rset);
		}
	}


	public static void startThreadLocal() {
		getThreadlocal().startThreadLocalPrivate();
	}


	public static void endThreadLocal(boolean forzeClear) {
		getThreadlocal().endThreadLocalPrivate(forzeClear);
	}


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
				ret.setAutoCommit(true);
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

	static protected void closeInner(ResultSet rset) {
		try {
			rset.close();
		} catch (SQLException e) {
			log.warn("Clossing ResultSet", e);
		}
	}
	
	private static PreparedStatement getPrepareStatementInner(Connection connection,String query) throws DaoManagerException {
		try {
			return connection.prepareStatement(query);
		} catch (SQLException e) {
			throw new DaoManagerException("Query:'" + query	+ "'", e);
		}
	}
}

