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
import eu.ginere.base.util.dao.KeyDTO;
import eu.ginere.jdbc.mysql.dao.ParentQueryDAO.ChildDAOInterface;

/**
 * @author ventura
 *
 * Este dao sirve para obtener objetos de una clase-tabla hina teniendo encuenta los
 * datos de la clase tabla padre.
 * Ejemplo, devuelve denuncias de radar fijo teniendo en cuaenta los datos de la tabla denuncia.
 *
 *
 *  Esta clase solo permite encadenar una herarquia de 2 clases pero puede ser utilizada como padre
 *  para una jerarquia de N clases ver QueryExtendsDAOBis
 *
 * @param <I>
 * @param <T>
 */
public abstract class ExtendsDAO<I extends KeyDTO,T extends I> extends AbstractSQLDAO implements ChildDAOInterface<I>{
	static final Logger log = Logger.getLogger(ExtendsDAO.class);


	private final AbstractKeyObjectSQLDAO<I> parentDao;
	private final String keyColumnName;

	protected final String GET_BY_ID_QUERY;
	protected final String GET_ALL_QUERY;
	protected final String GET_ALL_IDS;
	protected final String COUNT_QUERY;
	protected final String DELETE_PARENT_QUERY;
	protected final String DELETE_CHILD_QUERY;

	protected final String INSERT_CHILD_QUERY;
	protected final String UPDATE_CHILD_QUERY;

	private final String totalColumns;
	private final String totalTableNames;
	private final String totalConditions;
	private final String tableName;


	protected ExtendsDAO(AbstractKeyObjectSQLDAO<I> parentDao,
							  String childTableName,
							  String childColumnsArrayMinusKeyColumnName[],
							  String createChildTableQueryArray[][]){
		super(childTableName,createChildTableQueryArray);
		
		this.parentDao=parentDao;
		this.keyColumnName=parentDao.getKeyColumnName();
		String PARENT_COLUMNS=appendTablenameToColumnName(parentDao.getTableName(),parentDao.getColumnsMinusKeyColumnName());
		String CHILD_COLUMNS=appendTablenameToColumnName(childTableName,childColumnsArrayMinusKeyColumnName);

		this.totalColumns=PARENT_COLUMNS+","+CHILD_COLUMNS;
		this.totalTableNames=parentDao.getTableName()+","+childTableName;
		this.totalConditions=childTableName+"."+keyColumnName+"="+parentDao.getTableName()+"."+keyColumnName;
		this.tableName=childTableName;

		
		StringBuilder builder=new StringBuilder();

		builder.append(" SELECT ");
		builder.append(this.totalColumns);
		builder.append(" FROM ");
		builder.append(this.totalTableNames);
		builder.append(" WHERE ");
		builder.append(childTableName);
		builder.append(".");
		builder.append(keyColumnName);
		builder.append("=? AND ");
		builder.append(this.totalConditions);
		builder.append(" LIMIT 1");

		this.GET_BY_ID_QUERY=builder.toString();;

		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(parentDao.getTableName());
		builder.append('.');
		builder.append(keyColumnName);
		builder.append(",");
		builder.append(this.totalColumns);
		builder.append(" FROM ");
		builder.append(this.totalTableNames);
		builder.append(" WHERE ");
		builder.append(this.totalConditions);
		this.GET_ALL_QUERY=builder.toString();

		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(keyColumnName);
		builder.append(" FROM ");
		builder.append(childTableName);
		this.GET_ALL_IDS=builder.toString();

		builder.setLength(0);
		builder.append(" SELECT count(*) FROM ");
		builder.append(childTableName);
		this.COUNT_QUERY=builder.toString();

		this.DELETE_PARENT_QUERY="DELETE from " + parentDao.getTableName() + " where "+keyColumnName+"=?";
		this.DELETE_CHILD_QUERY="DELETE from " + childTableName + " where "+keyColumnName+"=?";


		// Insert Chils
		String CHILD_COLUMNS_MINUS_COLUMN_NAME=StringUtils.join(childColumnsArrayMinusKeyColumnName,',');
		String CHILD_COLUMNS_INCLUDING_COLUMN_NAME=keyColumnName+','+CHILD_COLUMNS_MINUS_COLUMN_NAME;

		StringBuilder insertBuilder=new StringBuilder();
		insertBuilder.append("INSERT INTO ");
		insertBuilder.append(childTableName);
		insertBuilder.append("(");
		insertBuilder.append(CHILD_COLUMNS_INCLUDING_COLUMN_NAME);
		insertBuilder.append(") VALUES (");

		// First the key column
		insertBuilder.append("?");
		
		// then the rest of the column
		for (int i=0;i<childColumnsArrayMinusKeyColumnName.length;i++){
			insertBuilder.append(",?");
		}
		insertBuilder.append(")");
		
		this.INSERT_CHILD_QUERY=insertBuilder.toString();
		

		// Udate Child
		StringBuilder updateBuilder=new StringBuilder();
		updateBuilder.append("UPDATE ");
		updateBuilder.append(tableName);
		updateBuilder.append(" set ");

		for (int i=0;i<childColumnsArrayMinusKeyColumnName.length;i++){
			if (i<childColumnsArrayMinusKeyColumnName.length-1){
				updateBuilder.append(childColumnsArrayMinusKeyColumnName[i]);
				updateBuilder.append("=?");
				updateBuilder.append(",");
			} else {
				updateBuilder.append(childColumnsArrayMinusKeyColumnName[i]);
				updateBuilder.append("=?");
			}
		}
		updateBuilder.append(" WHERE ");
		updateBuilder.append(keyColumnName);
		updateBuilder.append("=?");
				
		this.UPDATE_CHILD_QUERY=updateBuilder.toString();

	}

	public List<String> getAllIds () throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_IDS;
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			
			return getStringList(pstm, query);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			log.error(error, e);
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}


	public List<String> getList (GetListQueryInterface listQuery) throws DaoManagerException{
		Connection connection=getConnection();
		String query=listQuery.getQuery();
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			listQuery.setAttributes(pstm,query);

			return getStringList(pstm, query);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			log.error(error, e);
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
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
						throw new DaoManagerException("id:'"+id+"'");
					}
				}finally{
					rset.close();
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

			setString(pstm, 1, id, query);

			ResultSet rset = executeQuery(pstm, query);
			
			if (rset.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			String error = "id:'" + id + "'";

			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public List<I> getAll () throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_QUERY;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
			ResultSet rset = executeQuery(pstm,query);
			
			List<I> list= new ArrayList<I>(rset.getFetchSize());
			
			while (rset.next()){
				T t=createFromResultSet(rset,query);
				list.add(t);
			}
			return list;
		}catch (SQLException e){
			String error="Query:" + query;
			throw new DaoManagerException(error,e);
		}catch (DaoManagerException e) {
			String error="Query:" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	/**
	 * This must not include the where clause but it must include de AND clause at the begginig
	 */

	public List<I> getByConditions (String conditions) throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_QUERY+conditions;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
			ResultSet rset = executeQuery(pstm,query);
			
			List<I> list= new ArrayList<I>(rset.getFetchSize());
			
			while (rset.next()){
				T t=createFromResultSet(rset,query);
				list.add(t);
			}
			return list;
		}catch (SQLException e){
			String error="Query:" + query;
			throw new DaoManagerException(error,e);
		}catch (DaoManagerException e) {
			String error="Query:" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public void delete(String id)throws DaoManagerException{

		Connection connection = getConnection();
		try {
			try {
				connection.setAutoCommit(false);

				delete(connection,id);

				connection.commit();
			}finally {
				connection.setAutoCommit(true);
			}
		} catch (SQLException e) {
			String error = "id:'" + id ;
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public void delete(Connection connection,String id)throws DaoManagerException{		
		// DELETE PARENT
		PreparedStatement parentPstm = getPrepareStatement(connection,DELETE_CHILD_QUERY);
		setString(parentPstm, 1, id, DELETE_CHILD_QUERY);
		executeUpdate(parentPstm, DELETE_CHILD_QUERY);
		
		// DELETE CHILD
		PreparedStatement childPstm= getPrepareStatement(connection,DELETE_PARENT_QUERY);
		setString(childPstm, 1, id, DELETE_PARENT_QUERY);
		executeUpdate(childPstm, DELETE_PARENT_QUERY);
	}


	public String insert(T interf) throws DaoManagerException {
		Connection connection = getConnection();
		
		try {				
			connection.setAutoCommit(false);
			try {
				// Get the id and insert parent
				String id = parentDao.insert(connection,interf);

				// Insert Child
				String query=INSERT_CHILD_QUERY;

				PreparedStatement pstmInsert = getPrepareStatement(connection,
												 query);
				setInsertStatement(pstmInsert,id,interf,query);
				
				executeUpdate(pstmInsert, query);


				// commit;
				connection.commit();

				return id;
			}finally {
				connection.setAutoCommit(true);
			}
		} catch (Exception e) {
			String error = "Insert object:'" + interf + 
				"'";
			
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}		
	}

	public void update(T interf) throws DaoManagerException {
		Connection connection = getConnection();

		try {				
			connection.setAutoCommit(false);

			try {						
				// first update the parent
				parentDao.update(connection,interf);

				// the update the child
				String query=UPDATE_CHILD_QUERY;

				PreparedStatement pstmInsert = getPrepareStatement(connection,
																   query);
			
				setUpdateStatement(pstmInsert,interf,query);
			
				executeUpdate(pstmInsert, query);

				// commit
				connection.commit();
			}finally {
				connection.setAutoCommit(true);
			}
		} catch (SQLException e) {
			String error = "Update object:'" + interf + 
				"'";
			throw new DaoManagerException(error, e);
		} catch (DaoManagerException e) {
			String error = "Update object:'" + interf + 
				"'";
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}		
	}


	public String getColumns(){
		return totalColumns;
	}
	public String getTableNames(){
		return totalTableNames;
	}

	public String getConditions(){
		return totalConditions;
	}

	/**
	 * Devuelve la ultima tabla de este dao
	 * @return
	 */
	public String getTableName(){
		return tableName;
	}

	public String getKeyColumnName(){
		return keyColumnName;
	}

	public long count() throws DaoManagerException{
		Connection connection=getConnection();
		String query=COUNT_QUERY;
		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			
			return getLongFromQuery(pstm, query, 0);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			log.error(error, e);
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}


	protected T createFromResultSet(ResultSet rset,String query)throws SQLException, DaoManagerException {
		return createFromResultSet(getString(rset,keyColumnName,query),rset,query);
	}

	protected void setUpdateStatement(PreparedStatement pstm, 
									  T obj,
									  String query) throws DaoManagerException {
		
		int i=setInsertColumnsInner(pstm, obj, 1, query);
		
		set(pstm,i, obj.getId(), query);
	}

	protected void setInsertStatement(PreparedStatement pstm,
										String id,
									  T obj, 
									  String query)throws DaoManagerException {
		set(pstm,1, id, query);
		setInsertColumnsInner(pstm, obj, 2,query);
	}

	protected int setInsertColumnsInner(PreparedStatement pstm,
										T obj, 
										int firstIndex,
										String query)throws DaoManagerException {
//		int index=parentDao.setInsertColumns(pstm,
//											 obj,
//											 firstIndex,
//											 query);
		return setInsertColumns(pstm,
								obj,
								firstIndex,
								query);
	}

	/**
	 * Tiene que leer todas las columnas menos la columna de la primary key
	 */
	protected abstract T createFromResultSet(String id,ResultSet rset,String query) throws SQLException, DaoManagerException;

	/**
	 * @param pstm
	 * @param obj
	 * @param firstIndex
	 * @param query
	 * @return the last index when o new column will be inserted
	 * @throws DaoManagerException
	 */
	protected abstract int setInsertColumns(PreparedStatement pstm,
											T obj, 
											int firstIndex,
											String query)throws DaoManagerException ;

}