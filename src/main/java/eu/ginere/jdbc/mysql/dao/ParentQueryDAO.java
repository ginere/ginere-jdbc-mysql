package eu.ginere.jdbc.mysql.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.dao.KeyDTO;

/**
 * @author ventura
 *
 * Este dao sirve para obtener datos de una jerareuia de objetos almacenada en tablas.
 * Si tenemos una tabla madre con tablas hijas, en funcion del tipo del objeto de la tabla madre 
 * realiza las queries pertinentes en las tablas hijas, al final las implementacion de los objetos 
 * devuletos es polimorfica, esto es en una lista de objetos los objetos devuletos pueden 
 * tener distintas implementacions.
 * 
 * Ejemplo, hace queries en las tablas denuncias, denunciasFijos, denunciasMoviles y los 
 * objetos devueltos pueden ser fijos, moviles, tramo, etc ...
 *
 *
 * @param <I>
 * @param <T>
 */
public abstract class ParentQueryDAO<P extends KeyDTO> extends AbstractSQLDAO {
	static final Logger log = Logger.getLogger(ParentQueryDAO.class);

	protected final String typeColumnName;
	protected final String keyColumnName;

	protected final String GET_BY_ID_QUERY;

	protected final String GET_ALL_QUERY;
	protected final String GET_ALL_BY_CONDITIONS_QUERY;
	protected final String GET_ALL_BY_CONDITIONS_QUERY_ROW_NUM;

	protected final String GET_ALL_IDS;
	protected final String GET_IDS_BY_CONDITIONS;
	protected final String GET_IDS_BY_CONDITIONS_ROW_NUM;

	protected final String COUNT_QUERY;
	protected final String COUNT_BY_CONDITIONS_QUERY;

	protected final String DELETE_PARENT_QUERY;

	private final Map<String,ChildDAOInterface<? extends P> > cache=new Hashtable<String, ChildDAOInterface<? extends P>>();
	private final Map<String,String > childDeleteQuery=new Hashtable<String, String>();

	public static interface ChildDAOInterface<P> {
		public String getTableName();
		public String getKeyColumnName();
		public P get(Connection connection,String id) throws DaoManagerException;
//		public String insert(P element) throws DaoManagerException ;
//		public void update(P element) throws DaoManagerException ;
	}
	

	public void addChildQueryExtendsDAO(String type,ChildDAOInterface<? extends P> childDao){
		cache.put(type, childDao);
		childDeleteQuery.put(type,"DELETE from " + childDao.getTableName() + " where "+childDao.getKeyColumnName() +"=?");
	}
	

	protected ParentQueryDAO(String tableName,
							 String keyColumnName,
							 String typeColumnName,
							 String createQueryArray[][]){		

		super(tableName,createQueryArray);
		
		this.keyColumnName=keyColumnName;
		this.typeColumnName=typeColumnName;

		StringBuilder builder=new StringBuilder();

		builder.append(" SELECT ");
		builder.append(typeColumnName);
		builder.append(" FROM ");
		builder.append(tableName);
		builder.append(" WHERE ");
		builder.append(keyColumnName);
		builder.append("=? LIMIT 1");

		this.GET_BY_ID_QUERY=builder.toString();;


		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(keyColumnName);
		builder.append(",");
		builder.append(typeColumnName);
		builder.append(" FROM ");
		builder.append(tableName);
		this.GET_ALL_QUERY=builder.toString();

		//		builder.append(" WHERE ");
		this.GET_ALL_BY_CONDITIONS_QUERY=builder.toString();


		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(keyColumnName);
		builder.append(",");
		builder.append(typeColumnName);
		builder.append(" FROM ( SELECT ");
		builder.append(keyColumnName);
		builder.append(",");
		builder.append(typeColumnName);
		builder.append(" FROM ");
		builder.append(tableName);

		//		builder.append(" WHERE ");
		this.GET_ALL_BY_CONDITIONS_QUERY_ROW_NUM=builder.toString();



		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(keyColumnName);
		builder.append(" FROM ");
		builder.append(tableName);
		this.GET_ALL_IDS=builder.toString();


		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(keyColumnName);
		builder.append(" from ( SELECT ");
		builder.append(keyColumnName);
		builder.append(" FROM ");
		builder.append(tableName);
		this.GET_IDS_BY_CONDITIONS_ROW_NUM=builder.toString();


		builder.setLength(0);
		builder.append(" SELECT ");
		builder.append(keyColumnName);
		builder.append(" FROM ");
		builder.append(tableName);
		this.GET_IDS_BY_CONDITIONS=builder.toString();



		builder.setLength(0);
		builder.append(" SELECT count(*) FROM ");
		builder.append(tableName);
		this.COUNT_QUERY=builder.toString();

		// builder.append(" WHERE ");
		this.COUNT_BY_CONDITIONS_QUERY=builder.toString();
		
		this.DELETE_PARENT_QUERY="DELETE from " + tableName + " where "+keyColumnName+"=?";
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
	
	public List<String> getIdsByContitions (String conditions, Integer rowNum) throws DaoManagerException{
		Connection connection=getConnection();

		String query;

		if (rowNum!=null){
			query=GET_IDS_BY_CONDITIONS_ROW_NUM+conditions+" ) LIMIT ? " ;
		} else {
			query=GET_IDS_BY_CONDITIONS+conditions;
		}

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);
			
			if (rowNum!=null){
				setInt(pstm,1,rowNum,query);			
			}

			return getStringList(pstm, query);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			log.error(error, e);
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}



	public P get(String id) throws DaoManagerException  {
		
		Connection connection = getConnection();
		try {
			return get(connection,id);
		} finally {
			closeConnection(connection);
		}
	}

	public P get(String id,P defaultValue) throws DaoManagerException  {
		
		Connection connection = getConnection();
		try {
			return get(connection,id,defaultValue);
		} finally {
			closeConnection(connection);
		}
	}
	
	public P get(Connection connection,String id) throws DaoManagerException  {
		
		String query=GET_BY_ID_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);

			try {
				setString(pstm, 1, id, query);
				
				ResultSet rset = executeQuery(pstm, query);
				
				try {
					if (rset.next()) {
						String type=rset.getString(typeColumnName);
						return getById(connection,id,type);
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
	
	public P get(Connection connection,String id,P defaultValue) throws DaoManagerException  {
		
		String query=GET_BY_ID_QUERY;
		try {
			PreparedStatement pstm = getPrepareStatement(connection, query);

			try {
				setString(pstm, 1, id, query);
				
				ResultSet rset = executeQuery(pstm, query);
				
				try {
					if (rset.next()) {
						String type=rset.getString(typeColumnName);
						return getById(connection,id,type);
					} else {
						return defaultValue;
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

	public boolean exists(String id) throws DaoManagerException  {
		
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


	public List<P> getAll () throws DaoManagerException{
		Connection connection=getConnection();
		String query=GET_ALL_QUERY;

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
			ResultSet rset = executeQuery(pstm,query);
			
			List<P> list= new ArrayList<P>(rset.getFetchSize());
			
			while (rset.next()){
				String id=rset.getString(keyColumnName);
				String type=rset.getString(typeColumnName);
				P p=getById(connection,id,type);
				list.add(p);
			}
			return list;
		}catch (SQLException e){
			String error="Query" + query;
			throw new DaoManagerException(error,e);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public List<P> getList (GetListQueryInterface listQuery) throws DaoManagerException{
		Connection connection=getConnection();
		String query=listQuery.getQuery();

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			
			listQuery.setAttributes(pstm,query);

			ResultSet rset = executeQuery(pstm,query);
			
			List<P> list= new ArrayList<P>(rset.getFetchSize());
			
			while (rset.next()){
				String id=rset.getString(keyColumnName);
				String type=rset.getString(typeColumnName);
				P p=getById(connection,id,type);
				list.add(p);
			}
			return list;
		}catch (SQLException e){
			String error="Query" + query;
			throw new DaoManagerException(error,e);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}



	public List<P> getByConditions (String conditions,Integer rowNum) throws DaoManagerException{
		Connection connection=getConnection();

		String query;
		if (rowNum==null){
			query=GET_ALL_BY_CONDITIONS_QUERY+conditions;
		} else {
			query=GET_ALL_BY_CONDITIONS_QUERY_ROW_NUM+conditions+" ) LIMIT ?";
		}

		try{
			PreparedStatement pstm = getPrepareStatement(connection,query);			

			if (rowNum!=null){
				setInt(pstm,1,rowNum,query);			
			}

			ResultSet rset = executeQuery(pstm,query);
			
			List<P> list= new ArrayList<P>(rset.getFetchSize());
			
			while (rset.next()){
				String id=rset.getString(keyColumnName);
				String type=rset.getString(typeColumnName);
				P p=getById(connection,id,type);
				list.add(p);
			}
			return list;
		}catch (SQLException e){
			String error="Query" + query;
			throw new DaoManagerException(error,e);
		}catch (DaoManagerException e) {
			String error="Query" + query;
			throw new DaoManagerException(error, e);
		}finally{
			closeConnection(connection);
		}
	}

	public void delete(String id)throws DaoManagerException{

		Connection connection = getConnection();
		try {
			delete(connection,id);
		} finally {
			closeConnection(connection);
		}
	}

	public void delete(Connection connection,String id)throws DaoManagerException{
		try {
			try {
				connection.setAutoCommit(false);

				PreparedStatement pstm = getPrepareStatement(connection, GET_BY_ID_QUERY);

				setString(pstm, 1, id, GET_BY_ID_QUERY);

				ResultSet rset = executeQuery(pstm, GET_BY_ID_QUERY);
			
				if (!rset.next()) {
					return ;
				}
				String type=rset.getString(typeColumnName);

				// DELETE PARENT
				pstm = getPrepareStatement(connection,DELETE_PARENT_QUERY);
				setString(pstm, 1, id, DELETE_PARENT_QUERY);
				executeUpdate(pstm, DELETE_PARENT_QUERY);

				// DELETE CHILD
				delete(connection,id,type);

				// COMIT
				connection.commit();
			}finally {
				connection.setAutoCommit(true);
			}
		} catch (Exception e) {
			String error = "id:'" + id ;
			throw new DaoManagerException(error, e);
		} 
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

	public long countByConditions(String conditions) throws DaoManagerException{
		Connection connection=getConnection();
		String query=COUNT_BY_CONDITIONS_QUERY+conditions;
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

	private P getById(Connection connection,String id, String type) throws DaoManagerException {
		if (cache.containsKey(type)){
			ChildDAOInterface<? extends P>dao=cache.get(type);
			return dao.get(connection,id);
		} else {
			throw new DaoManagerException("There no dao for type:"+type);
		}
	}
	
	private void delete(Connection connection, String id, String type) throws DaoManagerException {
		if (cache.containsKey(type)){
			String deleteQuery=childDeleteQuery.get(type);
			PreparedStatement pstm = getPrepareStatement(connection, deleteQuery);
			try {
				setString(pstm, 1, id, deleteQuery);
				executeUpdate(pstm, deleteQuery);							
			}finally{
				close(pstm);
			}
		} else {
			throw new DaoManagerException("There no dao for type:"+type);
		}
		
	}
}