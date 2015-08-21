package eu.ginere.jdbc.mysql.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.test.TestResult;
import eu.ginere.jdbc.mysql.KeyDTO;

/**
 * @author ventura
 *
 * DAO para ser usuados por objetos que gestionan su pripio identificador, como los usuarios o BackEnd
 *
 * @param <I>
 * @param <T>
 */
public abstract class AbstractKeyDao<T extends KeyDTO> extends AbstractDAO implements KeyDAOInterface<T>{
	static final Logger log = Logger.getLogger(AbstractKeyDao.class);

	protected final String keyColumnName;
	protected final String columnsArrayMinusKeyColumnName[];
	protected final String columnsName[];

	protected final String GET_BY_ID_QUERY;
	protected final String GET_ALL_QUERY_LIMIT;

	protected final String GET_ALL_QUERY;
	protected final String GET_ALL_IDS;
//	protected final String COUNT_QUERY;
	protected final String DELETE_QUERY;

	protected final String INSERT_QUERY_VALID_KEY;
	protected final String INSERT_QUERY_AUTO_INCREMENT;
	protected final String UPDATE_QUERY;


	protected final String  COLUMNS_MINUS_COLUMN_NAME;
	protected final String  COLUMNS_INCLUDING_COLUMN_NAME;
	
	
	protected AbstractKeyDao(String tableName,
							 String keyColumnName,
							 String columnsArrayMinusKeyColumnName[],
							 String createQueryArray[][],
							 String deleteQueryArray[]){		
		super(tableName,createQueryArray,deleteQueryArray);
		
		this.keyColumnName=keyColumnName;
		this.columnsArrayMinusKeyColumnName=columnsArrayMinusKeyColumnName;
		this.columnsName=new String[columnsArrayMinusKeyColumnName.length+1];

		columnsName[0]=keyColumnName;
		
		for (int i=0;i<columnsArrayMinusKeyColumnName.length;i++){
			columnsName[i+1]=columnsArrayMinusKeyColumnName[i];	
		}
		
		
		this.COLUMNS_MINUS_COLUMN_NAME=StringUtils.join(columnsArrayMinusKeyColumnName,',');
		this.COLUMNS_INCLUDING_COLUMN_NAME=keyColumnName+','+COLUMNS_MINUS_COLUMN_NAME;


		this.GET_BY_ID_QUERY="SELECT "+COLUMNS_MINUS_COLUMN_NAME+
			" from "+tableName + " WHERE "+keyColumnName+"=? LIMIT 1";
		this.GET_ALL_QUERY="select " + COLUMNS_INCLUDING_COLUMN_NAME+ " from " + tableName+ " ";
		this.GET_ALL_QUERY_LIMIT="select " + COLUMNS_INCLUDING_COLUMN_NAME+ " from " + tableName+ " limit ?";

		this.GET_ALL_IDS="SELECT "+keyColumnName+" from "+tableName;
//		this.COUNT_QUERY="select count(*) from " + tableName;
		this.DELETE_QUERY="DELETE from " + tableName + " where "+keyColumnName+"=?";


		StringBuilder insertBuilder=new StringBuilder();
		insertBuilder.append("INSERT INTO ");
		insertBuilder.append(tableName);
		insertBuilder.append("(");
		insertBuilder.append(COLUMNS_INCLUDING_COLUMN_NAME);
		insertBuilder.append(") VALUES (");

		// First the key column
		insertBuilder.append("?");
		
		// then the rest of the column
		for (int i=0;i<columnsArrayMinusKeyColumnName.length;i++){
			insertBuilder.append(",?");
			//			insertBuilder.append(i+2);
			//			insertBuilder.append("");
		}
		insertBuilder.append(")");
		
		this.INSERT_QUERY_VALID_KEY=insertBuilder.toString();


		// Auton Incremente
		insertBuilder=new StringBuilder();
		insertBuilder.append("INSERT INTO ");
		insertBuilder.append(tableName);
		insertBuilder.append("(");
		insertBuilder.append(COLUMNS_MINUS_COLUMN_NAME);
		insertBuilder.append(") VALUES (");

		// then the rest of the column
		for (int i=0;i<columnsArrayMinusKeyColumnName.length;i++){
			if (i<columnsArrayMinusKeyColumnName.length-1){
				insertBuilder.append("?,");
			}else {
				insertBuilder.append("?");
			}
		}
		insertBuilder.append(")");
		
		this.INSERT_QUERY_AUTO_INCREMENT=insertBuilder.toString();

		// Update
		StringBuilder updateBuilder=new StringBuilder();
		updateBuilder.append("UPDATE ");
		updateBuilder.append(tableName);
		updateBuilder.append(" set ");

		for (int i=0;i<columnsArrayMinusKeyColumnName.length;i++){
			if (i<columnsArrayMinusKeyColumnName.length-1){
				updateBuilder.append(columnsArrayMinusKeyColumnName[i]);
				updateBuilder.append("=?");
				//				updateBuilder.append(i+2);
				updateBuilder.append(",");
			} else {
				updateBuilder.append(columnsArrayMinusKeyColumnName[i]);
				updateBuilder.append("=?");
				//				updateBuilder.append(i+2);
			}
		}
		updateBuilder.append(" WHERE ");
		updateBuilder.append(keyColumnName);
		updateBuilder.append("=?");
		//		updateBuilder.append(columnsArray.length+1);
				
		this.UPDATE_QUERY=updateBuilder.toString();
				
	}
	
	public T get(String id) throws DaoManagerException {
		
		Connection connection = getConnection();
		try {
			return get(connection,id);
		} finally {
			closeConnection(connection);
		}
	}

	public T get(Connection connection,String id) throws DaoManagerException {
		String query=GET_BY_ID_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);
			try {
				setString(pstm, 1, id, query);
				
				ResultSet rset = executeQuery(pstm, query);
				try {
					if (rset.next()) {
						return  createFromResultSet(id,rset,query);
					} else {
						throw new DaoManagerException("Object id:'"+id+"' do not exists");
					}
				}finally{
					close(rset);
				}
			}finally{
				close(pstm);
			}
		} catch (SQLException e) {
			String error = "id:'" + id + "'";

			throw new DaoManagerException(error, e);
		}
	}
	
	public T get(String id,T defaultValue) throws DaoManagerException {
		
		Connection connection = getConnection();
		try {
			return get(connection,id,defaultValue);
		} finally {
			closeConnection(connection);
		}
	}

	
	public T get(Connection connection,String id,T defaultValue) throws DaoManagerException {
		String query=GET_BY_ID_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);
			try {
				setString(pstm, 1, id, query);
				
				ResultSet rset = executeQuery(pstm, query);
				try {
					if (rset.next()) {
						return  createFromResultSet(id,rset,query);
					} else {
						return defaultValue;
					}
				}finally{
					close(rset);
				}
			}finally{
				close(pstm);
			}
		} catch (SQLException e) {
			String error = "id:'" + id + "'";

			throw new DaoManagerException(error, e);
		}
	}

	public boolean exists(String id) throws DaoManagerException {		
		Connection connection = getConnection();
		String query=GET_BY_ID_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);
            try {
                setString(pstm, 1, id, query);
                
                ResultSet rset = executeQuery(pstm, query);
                
                try {
                    if (rset.next()) {
                        return true;
                    } else {
                        return false;
                    }
                }finally{
                    close(rset);
                }
            }finally{
                close(pstm);
            }
		} catch (SQLException e) {
			String error = "id:'" + id + "'";

			throw new DaoManagerException(error, e);
		} catch (DaoManagerException e) {
			String error = "id:'" + id + "'";

			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public List<T> getAll () throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_QUERY;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
            try {
                ResultSet rset = executeQuery(pstm,query);
                
                try {
                    List<T> list= new ArrayList<T>(rset.getFetchSize());
                    
                    while (rset.next()){
                        T t=createFromResultSet(rset,query);
                        list.add(t);
                    }
                    return list;
				}catch (SQLException e){
					String error="Query:'" + query+"'";
					throw new DaoManagerException(error,e);
                }finally{
                    close(rset);
                }
            }finally{
                close(pstm);
            }
		}catch (DaoManagerException e) {
			String error="Query:'" + query+"'";
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public List<T> getAll(int limit) throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_QUERY_LIMIT;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);		
			try {
				setInt(pstm, 1, limit, query);
				
				ResultSet rset = executeQuery(pstm,query);
				try {
					List<T> list= new ArrayList<T>();
					
					while (rset.next()){
						T t=createFromResultSet(rset,query);
						list.add(t);
					}
					return list;
				}catch (SQLException e){
					String error="Query:'" + query+"'";
					throw new DaoManagerException(error,e);
				}finally{
					close(rset);
				}

			} finally {
				closeConnection(connection);
			}
		}catch (DaoManagerException e) {
			String error="Query" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public List<String> getAllIds () throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_IDS;
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
                return getStringList(pstm, query);
            }finally{
                close(pstm);
            }
		}catch (DaoManagerException e) {
			String error="Query:'" + query+"'";

			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

//	public long count() throws DaoManagerException{
//		Connection connection=getConnection();
//		String query=COUNT_QUERY;
//		try{
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//                return getLongFromQuery(pstm, query, 0);
//            }finally{
//                close(pstm);
//            }
//		}catch (DaoManagerException e) {
//			String error="Query:'" + query+"'";
//
//			throw new DaoManagerException(error, e);
//		}finally{
//			closeConnection(connection);
//		}
//	}
//	
	public void delete(String id)throws DaoManagerException{
		Connection connection = getConnection();
		String query=DELETE_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection,
                                                         query);

            try {
                setString(pstm, 1, id, query);
                
                executeUpdate(pstm, query);
            }finally{
                close(pstm);
            }
		} catch (DaoManagerException e) {
			String error = "id:'" + id + "' query:"+query;

			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}
	
	/**
	 * The id of the object must not be mull
	 *
	 * @param interf
	 * @return
	 * @throws DaoManagerException
	 */
	protected String insertWithValidKey(T interf) throws DaoManagerException {
		
		Connection connection = getConnection();
		
		try {				
			return insertWithValidKey(connection,interf);
		} finally {
			closeConnection(connection);
		}		
	}

	/**
	 * The id of the object must not be mull
	 * @param interf
	 * @return
	 * @throws DaoManagerException
	 */
	protected String insertWithValidKey(Connection connection,T interf) throws DaoManagerException {		
		String query=INSERT_QUERY_VALID_KEY;
		
		try {				
			PreparedStatement pstmInsert = getPrepareStatement(connection,
															   query);
            try {
                setInsertStatement(pstmInsert,interf,query);
                
                executeUpdate(pstmInsert, query);
                
                return interf.getKey();
            }finally{
                close(pstmInsert);
            }
		} catch (DaoManagerException e) {
			String error = "Insert object:'" + interf + 
				"' query:'" + query+
				"'";
			
			throw new DaoManagerException(error, e);
		} 	
	}
	
	/**
	 * The id of the object must not be mull
	 * @param interf
	 * @return
	 * @throws DaoManagerException
	 */
	protected String insertWithValidKey(String id,T interf) throws DaoManagerException {
		
		Connection connection = getConnection();
		String query=INSERT_QUERY_VALID_KEY;
		
		try {				
			PreparedStatement pstmInsert = getPrepareStatement(connection,
															   query);
            try {
                setInsertStatement(pstmInsert,id,interf,query);
                
                executeUpdate(pstmInsert, query);
                
				// return interf.getId();
                return id;
            }finally{
                close(pstmInsert);
            }
		} catch (DaoManagerException e) {
			String error = "Insert object:'" + interf + 
				"' query:'" + query+
				"'";
			
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}		
	}
	public String insert(T interf) throws DaoManagerException {
		return insertWithValidKey(interf);
	}
	
	public String insert(Connection connection,T interf) throws DaoManagerException {
		return insertWithValidKey(connection,interf);
	}
	
	public String insert(String id,T interf) throws DaoManagerException {
		log.warn("Why this function is called? for id:"+id+" and object"+interf);
		
		return insertWithValidKey(id,interf);		
	}

	
	public void update(T interf) throws DaoManagerException {
		Connection connection = getConnection();

		try {						
			update(connection,interf);
		} finally {
			closeConnection(connection);
		}		

	}

	public void update(Connection connection,T interf) throws DaoManagerException {
		String query=UPDATE_QUERY;

		try {						
			PreparedStatement pstmInsert = getPrepareStatement(connection,
															   query);
			try {
                setUpdateStatement(pstmInsert,interf,query);
                
                executeUpdate(pstmInsert, query);
            }finally{
                close(pstmInsert);
            }
		} catch (DaoManagerException e) {
			String error = "Update object:'" + interf + 
				"' query:'" + query+
				"'";
			throw new DaoManagerException(error, e);
		} 
	}
	

	/**
	 * This must include the where clause if necessary
	 */
	public List<T> getByConditions (String conditions) throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_QUERY+conditions;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
            try {
                ResultSet rset = executeQuery(pstm,query);
                try {
                    List<T> list= new ArrayList<T>(rset.getFetchSize());
                    
                    while (rset.next()){
                        T t=createFromResultSet(rset,query);
                        list.add(t);
                    }
                    return list;
				}catch (SQLException e){
					String error="Query:'" + query+"'";
					throw new DaoManagerException(error,e);
                }finally{
                    close(rset);
                }                
            }finally{
                close(pstm);
            }
		}catch (DaoManagerException e) {
			String error="Query:'" + query+"'";
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	/**
	 * Tiene que leer todas las columnas menos la columna de la primary key
	 */
	public abstract T createFromResultSet(String id,ResultSet rset,String query) throws SQLException, DaoManagerException;
	
	protected T createFromResultSet(ResultSet rset,String query)throws SQLException, DaoManagerException {
		String id=getString(rset,keyColumnName,query);
		
		return createFromResultSet(id,rset,query);
	}

	/**
	 * To iterate over all the elements of the table
	 */
	public interface Iterator<T>{
		/**
		 * @param id
		 * @param name
		 * @param folderFullName
		 * @return while this return true, continues working
		 */
		boolean access(T i);		
	};


	public void iterate(Iterator<T> i) throws DaoManagerException {
		
		Connection connection=getConnection();
		String query=GET_ALL_QUERY;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
			try {
				iterate(i, pstm, query);
				return ;
			}finally{
				close(pstm);
			}							
		}catch (DaoManagerException e) {
			String error="Query" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public long iterate(Iterator<T> i,PreparedStatement pstm,String query ) throws DaoManagerException {
		ResultSet rset = executeQuery(pstm,query);
		int ret=0;
		try {
			while (rset.next()){
				T t=createFromResultSet(rset,query);
				if (!i.access(t)){
					return ret++;
				}
				ret++;
			}
			return ret;
		}catch (SQLException e){
			String error="Query" + query;
			throw new DaoManagerException(error,e);
		}finally{
			close(rset);
		}
	}



	/**
	 * Vuelca todos los 
	 * @param pstmInsert
	 * @param obj
	 * @param query
	 * @throws DaoManagerException
	 */
//	protected abstract void setInsertStatementFromSequence(PreparedStatement pstm,String id,T obj,String query) throws DaoManagerException;
//	protected abstract void setInsertFromAutoIncrementKeyColumnStatement(PreparedStatement pstm,String id,T obj,String query) throws DaoManagerException;
//	protected abstract void setInsertStatement(PreparedStatement pstm,T obj,String query) throws DaoManagerException;
//	protected abstract void setUpdateStatement(PreparedStatement pstm,T obj,String query) throws DaoManagerException;

	protected void setInsertStatement(PreparedStatement pstm,
									  T obj, 
									  String query)throws DaoManagerException {
		set(pstm,1, obj.getKey(), query);
		fillUpdateStatement(pstm, obj, 2,query);
	}

	protected void setInsertStatement(PreparedStatement pstm,
									  String id,
									  T obj, 
									  String query)throws DaoManagerException {
		set(pstm,1, id, query);
		fillUpdateStatement(pstm, obj, 2,query);
	}
	
	protected void setUpdateStatement(PreparedStatement pstm, 
									  T obj,
									  String query) throws DaoManagerException {

		int i=fillUpdateStatement(pstm, obj, 1, query);

		set(pstm,i, obj.getKey(), query);
	}
	
	/**
	 * @param pstm
	 * @param obj
	 * @param firstIndex
	 * @param query
	 * @return The last index when a new column could be inserted
	 * @throws DaoManagerException
	 */
	protected abstract int fillUpdateStatement(PreparedStatement pstm,
											   T obj, 
											   int firstIndex,
											   String query)throws DaoManagerException ;

	@Override
	public TestResult test() {
		TestResult ret=new TestResult(getClass());
		TestResult parent=super.test();
		
		ret.add(parent);
		
		if (parent.isOK()){
			try {
				getAll(1);
			} catch (DaoManagerException e) {
				ret.addError("While getting one line from database", e);
			}
		} 
		
		return ret;
	}



	/***
	 *
	 */
	protected static String getQueryForOneResultOneColumn(String tableName,
														  String columnName) {
		return "select * from " + tableName
		+ " where "+columnName+"=? LIMIT 1";
	}
	
	public T getOneValueForOneColmunQuery(String query,String value) throws DaoManagerException {
		T ret=getOneValueForOneColmunQuery(query, value,null);
		
		if (ret==null){
			throw new DaoManagerException("Nout found, column value:"+value);
		} else {
			return ret;
		}
	}
	
	public T getOneValueForOneColmunQuery(String query,String value, T defaultValue) throws DaoManagerException {
		
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);
			try {
				
				setString(pstm, 1, value, query);
				
				ResultSet rset = executeQuery(pstm, query);
				try {
					if (rset.next()) {
						return  createFromResultSet(rset,query);
					} else {
						return defaultValue;
					}
				} catch (SQLException e) {
					String error = "query:'"+query+"' value:'" + value + "'";

					throw new DaoManagerException(error, e);
				}finally{
					close(rset);
				}
			}finally{
				close(pstm);
			}

		} finally {
			closeConnection(connection);
		}
	}

	/**
	 * Returuns all the columns Name
	 * @return
	 */
	public String[] getColumnsName(){
		return columnsName;
	}

	/**
	 * The key Column Name
	 * @return
	 */
	public String getKeyColumnName(){
		return keyColumnName;
	}

	/**
	 * all the columns minus the key column
	 * @return
	 */
	public String[] getColumnsMinusKeyColumnName(){
		return columnsArrayMinusKeyColumnName;
	}

	@Override
	public long getBackendElementNumber() {
		try {
			return count();
		}catch (DaoManagerException e) {
			log.info("",e);
			return 0;
		}
	}
	

}
